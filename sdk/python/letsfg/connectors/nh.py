"""
All Nippon Airways (NH) nodriver + Playwright hybrid connector.

ANA's booking SPA (aswbe.ana.co.jp) is protected by Akamai Bot Manager.
Standard CDP Chrome (even headed) gets _abck score -1 and is blocked.
nodriver bypasses Akamai's sensor, so we use it to launch Chrome and navigate,
then connect Playwright via CDP for response interception.

Strategy (nodriver + Playwright hybrid):
1. Launch Chrome via nodriver (auto-bypasses Akamai bot sensor).
2. Navigate to ana.co.jp/en/us/ homepage, set hidden form fields.
3. Submit form → SPA loads at aswbe.ana.co.jp, initialises JWT.
4. Connect Playwright via CDP to the same Chrome for response capture.
5. Intercept 200 response from roundtrip-owd API.
6. Parse roundtripBounds[0].travelSolutions + airOffers → FlightOffer.

API details (discovered Mar 2026):
  Initialization: POST space.ana.co.jp/aswbe-initialization/api/v1/initialization
  Search:         POST space.ana.co.jp/aswbe-search/api/v1/roundtrip-owd
  Response: {data: {roundtripBounds: [{travelSolutions: [{flights, ...}], ...}],
             airOffers: {offerId: {prices, bounds, ...}}, airOffersSummary, ...}}
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
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

logger = logging.getLogger(__name__)

# ── nodriver + Playwright hybrid browser ──────────────────────────────────────
_nd_browser = None      # nodriver browser (owns Chrome process)
_pw_instance = None     # Playwright async API instance
_pw_browser = None      # Playwright CDP browser (connected to nodriver's Chrome)
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _ensure_nd_browser():
    """Launch nodriver Chrome if needed. Returns the nodriver browser instance."""
    global _nd_browser
    lock = _get_lock()
    async with lock:
        if _nd_browser:
            try:
                # Quick liveness check - get tabs
                _ = _nd_browser.tabs
                return _nd_browser
            except Exception:
                _nd_browser = None

        import nodriver as uc

        _nd_browser = await uc.start(
            headless=False,
            browser_args=[
                "--window-size=1400,900",
                "--window-position=-2400,-2400",
                "--disable-http2",
            ],
        )
        logger.info("NH: nodriver Chrome launched (port %s)", _nd_browser.config.port)
        return _nd_browser


async def _connect_playwright():
    """Connect Playwright to the already-running nodriver Chrome. Returns context."""
    global _pw_instance, _pw_browser
    # Disconnect old connection if any
    await _disconnect_playwright()

    from playwright.async_api import async_playwright

    _pw_instance = await async_playwright().start()
    port = _nd_browser.config.port
    host = _nd_browser.config.host or "127.0.0.1"
    _pw_browser = await _pw_instance.chromium.connect_over_cdp(
        f"http://{host}:{port}"
    )
    logger.info("NH: Playwright connected via CDP to %s:%s", host, port)
    return _pw_browser.contexts[0]


async def _disconnect_playwright():
    """Disconnect Playwright (keep nodriver Chrome alive)."""
    global _pw_instance, _pw_browser
    try:
        if _pw_browser:
            await _pw_browser.close()
    except Exception:
        pass
    try:
        if _pw_instance:
            await _pw_instance.stop()
    except Exception:
        pass
    _pw_browser = None
    _pw_instance = None


async def _reset_browser():
    """Tear down everything when Akamai flags the session."""
    global _nd_browser
    await _disconnect_playwright()
    if _nd_browser:
        try:
            _nd_browser.stop()
        except Exception:
            pass
    _nd_browser = None
    logger.info("NH: browser reset")


def _to_datetime(val) -> datetime:
    if isinstance(val, datetime):
        return val
    if isinstance(val, date_type):
        return datetime(val.year, val.month, val.day)
    return datetime.strptime(str(val), "%Y-%m-%d")


def _parse_nh_datetime(s: str) -> datetime:
    """Parse ANA datetime like '2026-03-14T16:45:00'."""
    return datetime.fromisoformat(s)


# ── Airline name mapping ─────────────────────────────────────────────────────
_AIRLINE_NAMES = {
    "NH": "All Nippon Airways",
    "UA": "United Airlines",
    "AC": "Air Canada",
    "LH": "Lufthansa",
    "SQ": "Singapore Airlines",
    "TG": "Thai Airways",
    "OZ": "Asiana Airlines",
    "BR": "EVA Air",
    "NZ": "Air New Zealand",
    "SA": "South African Airways",
    "CA": "Air China",
    "AI": "Air India",
    "ET": "Ethiopian Airlines",
    "LO": "LOT Polish Airlines",
    "OS": "Austrian Airlines",
    "SN": "Brussels Airlines",
    "SK": "SAS Scandinavian",
    "TP": "TAP Air Portugal",
}


class ANAConnectorClient:
    """ANA (NH) nodriver+Playwright hybrid — form fill + search API interception."""

    def __init__(self, timeout: float = 60.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        browser = await _ensure_nd_browser()
        if not browser:
            logger.error("NH: failed to launch nodriver Chrome")
            return self._empty(req)

        # Prepare parameters
        dt = _to_datetime(req.date_from)
        date_str = dt.strftime("%Y-%m-%d")
        ret_dt = dt + timedelta(days=5)
        adults = req.adults or 1

        # Phase 1: Navigate via nodriver (bypasses Akamai sensor)
        nd_tab = await browser.get("https://www.ana.co.jp/en/us/")
        logger.info("NH: loading homepage for %s->%s on %s", req.origin, req.destination, date_str)
        await asyncio.sleep(10.0)

        # Dismiss overlays + set hidden inputs via nodriver
        await nd_tab.evaluate("""
            (function() {
                document.querySelectorAll(
                    '#onetrust-consent-sdk, .onetrust-pc-dark-filter, ' +
                    '#onetrust-banner-sdk, [role="dialog"], ' +
                    '[class*="cookie"], [class*="consent"]'
                ).forEach(function(el) { el.remove(); });
                document.querySelectorAll('*').forEach(function(el) {
                    var s = window.getComputedStyle(el);
                    if (s.position === 'fixed' && parseInt(s.zIndex) > 999 && el.offsetHeight > 100) el.remove();
                });
            })()
        """)

        await nd_tab.evaluate("""
            (function() {
                var set = function(n,v) { var i=document.querySelector('input[name="'+n+'"]'); if(i) i.value=v; };
                set('origin', '""" + req.origin + """');
                set('destination', '""" + req.destination + """');
                set('departureDate', '""" + date_str + """');
                set('wayToMonth', '""" + f"{dt.month:02d}" + """');
                set('wayToDay', '""" + f"{dt.day:02d}" + """');
                set('returnDate', '""" + ret_dt.strftime("%Y-%m-%d") + """');
                set('wayBackMonth', '""" + f"{ret_dt.month:02d}" + """');
                set('wayBackDay', '""" + f"{ret_dt.day:02d}" + """');
                set('ADT', '""" + str(adults) + """');
            })()
        """)
        logger.info("NH: hidden inputs set")

        # Phase 2: Connect Playwright AFTER nodriver has loaded the page
        context = await _connect_playwright()
        if not context:
            logger.error("NH: failed to connect Playwright")
            return self._empty(req)

        ana_page = None
        for p in context.pages:
            if "ana.co.jp" in p.url:
                ana_page = p
                break
        if not ana_page:
            ana_page = context.pages[0] if context.pages else None
        if not ana_page:
            logger.error("NH: no ANA page found in Playwright")
            await _disconnect_playwright()
            return self._empty(req)
        logger.info("NH: Playwright found page at %s", ana_page.url[:80])

        search_data: dict = {}
        akamai_blocked = False

        async def _on_response(response):
            nonlocal akamai_blocked
            url = response.url
            if "roundtrip-owd" not in url and "oneway-owd" not in url:
                return
            status = response.status
            if status == 403:
                akamai_blocked = True
                logger.warning("NH: Akamai 403 on search API")
                return
            if status == 200:
                try:
                    data = await response.json()
                    if isinstance(data, dict) and "data" in data:
                        inner = data["data"]
                        if "roundtripBounds" in inner or "onewayBounds" in inner:
                            search_data.update(inner)
                            bk = "roundtripBounds" if "roundtripBounds" in inner else "onewayBounds"
                            bounds = inner.get(bk, [])
                            n = len(bounds[0].get("travelSolutions", [])) if bounds else 0
                            logger.info("NH: captured search — %d travel solutions", n)
                except Exception as e:
                    logger.warning("NH: failed to parse search response: %s", e)

        ana_page.on("response", _on_response)

        try:
            # Phase 3: Submit form via Playwright (nodriver's evaluate may conflict
            # with Playwright's CDP session on the same target)
            btn_found = await ana_page.evaluate("""() => {
                var btn = document.querySelector('button.be-wws-reserve-ticket-submit__button');
                if (btn) { btn.click(); return 'clicked'; }
                // Fallback: submit the form directly
                var forms = document.querySelectorAll('form');
                for (var i = 0; i < forms.length; i++) {
                    if (forms[i].action && forms[i].action.indexOf('flight-search') !== -1) {
                        forms[i].submit();
                        return 'form_submitted';
                    }
                }
                return 'not_found';
            }""")
            logger.info("NH: form submitted %s->%s (method: %s)", req.origin, req.destination, btn_found)

            # Wait for JWT + search response
            spa_ready = False
            remaining = max(self.timeout - (time.monotonic() - t0), 15)
            deadline = time.monotonic() + min(remaining, 45)
            while time.monotonic() < deadline:
                await asyncio.sleep(1.0)
                if search_data:
                    break
                if akamai_blocked:
                    break
                if not spa_ready:
                    try:
                        token = await ana_page.evaluate(
                            "(() => { try { return sessionStorage.getItem('accessToken'); } catch { return null; } })()"
                        )
                    except Exception:
                        continue
                    if token:
                        spa_ready = True
                        logger.info("NH: SPA ready, JWT obtained")
                elif not search_data and time.monotonic() - t0 > 40:
                    # Check for soft block (page shows "heavy traffic" without 403)
                    try:
                        content = await ana_page.evaluate(
                            "() => (document.body?.innerText||'').substring(0,200)"
                        )
                        if "cannot be accepted" in content or "heavy" in content.lower():
                            akamai_blocked = True
                            logger.warning("NH: Akamai soft-blocked (page content)")
                            break
                    except Exception:
                        pass

            if akamai_blocked:
                logger.warning("NH: Akamai blocked, resetting browser")
                await _reset_browser()
                return self._empty(req)

            if not search_data:
                logger.warning("NH: no search data captured")
                return self._empty(req)

            offers = self._parse_search(search_data, req)
            offers.sort(key=lambda o: o.price)

            currency = "USD"
            if offers:
                currency = offers[0].currency
            elif search_data.get("airOffersSummary", {}).get("minPrice", {}).get("currencyCode"):
                currency = search_data["airOffersSummary"]["minPrice"]["currencyCode"]

            elapsed = time.monotonic() - t0
            logger.info("NH %s->%s returned %d offers in %.1fs",
                        req.origin, req.destination, len(offers), elapsed)

            search_hash = hashlib.md5(
                f"nh{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
            ).hexdigest()[:12]

            return FlightSearchResponse(
                search_id=f"fs_{search_hash}",
                origin=req.origin,
                destination=req.destination,
                currency=currency,
                offers=offers,
                total_results=len(offers),
            )

        except Exception as e:
            logger.error("NH error: %s", e)
            return self._empty(req)
        finally:
            await _disconnect_playwright()



    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    def _parse_search(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse roundtrip-owd response into one-way FlightOffers."""
        offers: list[FlightOffer] = []

        # Get outbound travel solutions (bound[0])
        bounds_key = "roundtripBounds" if "roundtripBounds" in data else "onewayBounds"
        bounds = data.get(bounds_key, [])
        if not bounds:
            return offers

        outbound = bounds[0]
        travel_solutions = outbound.get("travelSolutions", [])
        air_offers = data.get("airOffers", {})
        availability = data.get("airOffersSummary", {}).get("travelSolutionsAvailability", {})

        # Build a map: travelSolutionId → cheapest price
        ts_prices: dict[str, tuple[float, str]] = {}
        for offer_id, offer in air_offers.items():
            if offer.get("isUnselectable"):
                continue
            prices = offer.get("prices", {})
            total_price_obj = prices.get("totalPrice", {})
            total = total_price_obj.get("total", 0)
            currency = total_price_obj.get("currencyCode", "USD")
            if total <= 0:
                continue

            # Each offer has bounds linking to travelSolutionIds
            for bound in offer.get("bounds", []):
                ts_id = bound.get("travelSolutionId", "")
                if not ts_id.startswith("o"):
                    continue  # Only outbound
                bound_price = bound.get("totalPrice", {}).get("total", 0)
                if bound_price <= 0:
                    bound_price = total
                if ts_id not in ts_prices or bound_price < ts_prices[ts_id][0]:
                    ts_prices[ts_id] = (bound_price, currency)

        for ts in travel_solutions:
            ts_id = ts.get("travelSolutionId", "")

            # Skip unavailable solutions
            avail = availability.get(ts_id, {})
            if avail.get("isUnavailable", False):
                continue

            # Get price for this solution
            if ts_id not in ts_prices:
                continue
            price, currency = ts_prices[ts_id]

            flights = ts.get("flights", [])
            if not flights:
                continue

            segments = []
            for flight in flights:
                dep = flight.get("departure", {})
                arr = flight.get("arrival", {})
                airline_code = flight.get("marketingAirlineCode", "NH")
                flight_num = flight.get("marketingFlightNumber", "")
                op_airline = flight.get("operatingAirlineCode", airline_code)
                airline_name = _AIRLINE_NAMES.get(airline_code, flight.get("operatingAirlineName", airline_code))

                segments.append(
                    FlightSegment(
                        airline=airline_code,
                        airline_name=airline_name,
                        flight_no=f"{airline_code}{flight_num}",
                        origin=dep.get("locationCode", ""),
                        destination=arr.get("locationCode", ""),
                        departure=_parse_nh_datetime(dep.get("dateTime", "")),
                        arrival=_parse_nh_datetime(arr.get("dateTime", "")),
                        duration_seconds=flight.get("duration", 0),
                        cabin_class="economy",
                        aircraft=flight.get("aircraftName", ""),
                    )
                )

            total_dur = ts.get("duration", 0)
            stopovers = ts.get("numberOfConnections", max(len(segments) - 1, 0))

            route = FlightRoute(
                segments=segments,
                total_duration_seconds=total_dur,
                stopovers=stopovers,
            )

            offer_id = hashlib.md5(
                f"nh_{req.origin}_{req.destination}_{ts_id}_{price}".encode()
            ).hexdigest()[:12]

            all_airlines = list({s.airline for s in segments})

            offers.append(
                FlightOffer(
                    id=f"nh_{offer_id}",
                    price=price,
                    currency=currency,
                    price_formatted=f"{price:,.2f} {currency}",
                    outbound=route,
                    inbound=None,
                    airlines=[_AIRLINE_NAMES.get(a, a) for a in all_airlines],
                    owner_airline="NH",
                    booking_url=self._booking_url(req),
                    is_locked=False,
                    source="nh_direct",
                    source_tier="free",
                )
            )

        return offers

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _booking_url(req: FlightSearchRequest) -> str:
        dt = _to_datetime(req.date_from)
        return (
            f"https://www.ana.co.jp/en/us/book-plan/search/flight-search/"
            f"?origin={req.origin}"
            f"&destination={req.destination}"
            f"&departureDate={dt.strftime('%Y-%m-%d')}"
            f"&adults={req.adults or 1}"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"nh{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency="USD",
            offers=[],
            total_results=0,
        )


# ── Module-level interface (required by connector loader) ────────────────────

_client = ANAConnectorClient()


async def search(request: FlightSearchRequest) -> FlightSearchResponse:
    return await _client.search_flights(request)
