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

from .browser import patchright_bandwidth_args, apply_cdp_url_blocking

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
                *patchright_bandwidth_args(),
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
        await asyncio.sleep(5.0)

        # Dismiss overlays (OneTrust + Ensighten consent dialogs)
        await nd_tab.evaluate("""
            (function() {
                document.querySelectorAll(
                    '#onetrust-consent-sdk, .onetrust-pc-dark-filter, ' +
                    '#onetrust-banner-sdk, #ensModalWrapper, #ensNotifyBanner, ' +
                    '.ens-notice, dialog[open], [class*="consent"]'
                ).forEach(function(el) { el.remove(); });
            })()
        """)

        # Phase 2: Connect Playwright for UI interaction + response capture
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

        # Remove any overlays that reappeared via Playwright
        await ana_page.evaluate("""() => {
            document.querySelectorAll(
                '#ensModalWrapper, dialog[open], .ens-notice, ' +
                '#onetrust-consent-sdk, .onetrust-pc-dark-filter'
            ).forEach(el => el.remove());
        }""")
        await asyncio.sleep(0.3)

        # Phase 3: Select airports via UI dialog interaction
        try:
            # Select DEPARTURE airport
            dep_btn = ana_page.locator("button.be-wws-reserve-ticket-departure-airport__button")
            await dep_btn.click(timeout=5000)
            await asyncio.sleep(1.0)

            dep_dialog = ana_page.locator("div.be-dialog.be-dialog--show")
            dep_search = dep_dialog.locator("input.be-list-with-search__searchbox-input")
            await dep_search.fill(req.origin)
            await asyncio.sleep(0.5)

            dep_click = await dep_dialog.evaluate("""(el, iata) => {
                let item = el.querySelector('li.be-list__item[data-value="' + iata + '"]');
                if (item) { item.click(); return 'ok'; }
                for (const li of el.querySelectorAll('li.be-list__item')) {
                    if (li.innerText.includes(iata)) { li.click(); return 'ok_text'; }
                }
                return 'not_found';
            }""", req.origin)
            if "not_found" in str(dep_click):
                logger.warning("NH: departure airport %s not found in dialog", req.origin)
                await _disconnect_playwright()
                return self._empty(req)
            logger.info("NH: selected departure %s", req.origin)
            await asyncio.sleep(0.5)

            # Close dep dialog if still open
            if await ana_page.locator("div.be-dialog.be-dialog--show").count() > 0:
                close = ana_page.locator("div.be-dialog.be-dialog--show button.be-dialog__close-button")
                if await close.count() > 0:
                    await close.first.click()
                    await asyncio.sleep(0.3)

            # Select ARRIVAL airport
            arr_btn = ana_page.locator("button.be-wws-reserve-ticket-arrival-airport__button")
            await arr_btn.click(timeout=5000)
            await asyncio.sleep(1.0)

            arr_dialog = ana_page.locator("div.be-dialog.be-dialog--show")
            arr_search = arr_dialog.locator("input.be-list-with-search__searchbox-input")
            await arr_search.fill(req.destination)
            await asyncio.sleep(0.5)

            arr_click = await arr_dialog.evaluate("""(el, iata) => {
                let item = el.querySelector('li.be-list__item[data-value="' + iata + '"]');
                if (item) { item.click(); return 'ok'; }
                for (const li of el.querySelectorAll('li.be-list__item')) {
                    if (li.innerText.includes(iata)) { li.click(); return 'ok_text'; }
                }
                return 'not_found';
            }""", req.destination)
            if "not_found" in str(arr_click):
                logger.warning("NH: arrival airport %s not found in dialog", req.destination)
                await _disconnect_playwright()
                return self._empty(req)
            logger.info("NH: selected arrival %s", req.destination)
            await asyncio.sleep(0.5)

            # Close arr dialog if still open
            if await ana_page.locator("div.be-dialog.be-dialog--show").count() > 0:
                close2 = ana_page.locator("div.be-dialog.be-dialog--show button.be-dialog__close-button")
                if await close2.count() > 0:
                    await close2.first.click()
                    await asyncio.sleep(0.3)

            # Set date via hidden inputs (date dialogs are complex, hidden inputs work)
            await ana_page.evaluate("""(params) => {
                const set = (n,v) => { const i=document.querySelector('input[name="'+n+'"]'); if(i) i.value=v; };
                set('departureDate', params.date);
                set('wayToMonth', params.month);
                set('wayToDay', params.day);
                set('returnDate', params.retDate);
                set('wayBackMonth', params.retMonth);
                set('wayBackDay', params.retDay);
                set('ADT', params.adults);
            }""", {
                "date": date_str,
                "month": f"{dt.month:02d}",
                "day": f"{dt.day:02d}",
                "retDate": ret_dt.strftime("%Y-%m-%d"),
                "retMonth": f"{ret_dt.month:02d}",
                "retDay": f"{ret_dt.day:02d}",
                "adults": str(adults),
            })
            logger.info("NH: date set to %s, adults=%d", date_str, adults)

        except Exception as e:
            logger.error("NH: form fill failed: %s", e)
            await _disconnect_playwright()
            return self._empty(req)

        # Phase 4: Setup response interception + submit search
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
            # Click search button (force=True to bypass any remaining overlays)
            submit = ana_page.locator("button.be-wws-reserve-ticket-submit__button")
            await submit.click(force=True, timeout=5000)
            logger.info("NH: search submitted %s->%s", req.origin, req.destination)

            # Wait for search API response
            remaining = max(self.timeout - (time.monotonic() - t0), 15)
            deadline = time.monotonic() + min(remaining, 45)
            while time.monotonic() < deadline:
                await asyncio.sleep(1.0)
                if search_data:
                    break
                if akamai_blocked:
                    break
                # Check for soft block after 30s
                if time.monotonic() - t0 > 30 and not search_data:
                    try:
                        content = await ana_page.evaluate(
                            "() => (document.body?.innerText||'').substring(0,200)"
                        )
                        if "cannot be accepted" in content or "heavy" in content.lower() or "could not be processed" in content.lower():
                            akamai_blocked = True
                            logger.warning("NH: soft-blocked (page content)")
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
        """Parse roundtrip-owd response into FlightOffers (with inbound for RT)."""
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

        # ── Build IB route if RT and bounds[1] exists ──
        ib_route = None
        ib_price = 0.0
        has_ib = req.return_from and bounds_key == "roundtripBounds" and len(bounds) > 1
        if has_ib:
            ib_bound = bounds[1]
            ib_ts_list = ib_bound.get("travelSolutions", [])
            # Build IB price map (travelSolutionId starting with "i")
            ib_prices: dict[str, tuple[float, str]] = {}
            for _oid, offer in air_offers.items():
                if offer.get("isUnselectable"):
                    continue
                for bnd in offer.get("bounds", []):
                    ts_id = bnd.get("travelSolutionId", "")
                    if not ts_id.startswith("i"):
                        continue
                    bp = bnd.get("totalPrice", {}).get("total", 0)
                    cur = bnd.get("totalPrice", {}).get("currencyCode",
                          offer.get("prices", {}).get("totalPrice", {}).get("currencyCode", "USD"))
                    if bp <= 0:
                        continue
                    if ts_id not in ib_prices or bp < ib_prices[ts_id][0]:
                        ib_prices[ts_id] = (bp, cur)

            # Find cheapest IB travel solution and build route
            best_ib_price = float("inf")
            best_ib_ts = None
            for ts in ib_ts_list:
                ts_id = ts.get("travelSolutionId", "")
                avail = availability.get(ts_id, {})
                if avail.get("isUnavailable", False):
                    continue
                if ts_id in ib_prices and ib_prices[ts_id][0] < best_ib_price:
                    best_ib_price = ib_prices[ts_id][0]
                    best_ib_ts = ts

            if best_ib_ts and best_ib_price < float("inf"):
                ib_price = best_ib_price
                _nh_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
                ib_segs = []
                for fl in best_ib_ts.get("flights", []):
                    dep = fl.get("departure", {})
                    arr = fl.get("arrival", {})
                    ac = fl.get("marketingAirlineCode", "NH")
                    fn = fl.get("marketingFlightNumber", "")
                    aname = _AIRLINE_NAMES.get(ac, fl.get("operatingAirlineName", ac))
                    ib_segs.append(FlightSegment(
                        airline=ac, airline_name=aname,
                        flight_no=f"{ac}{fn}",
                        origin=dep.get("locationCode", ""),
                        destination=arr.get("locationCode", ""),
                        departure=_parse_nh_datetime(dep.get("dateTime", "")),
                        arrival=_parse_nh_datetime(arr.get("dateTime", "")),
                        duration_seconds=fl.get("duration", 0),
                        cabin_class=_nh_cabin,
                        aircraft=fl.get("aircraftName", ""),
                    ))
                if ib_segs:
                    ib_route = FlightRoute(
                        segments=ib_segs,
                        total_duration_seconds=best_ib_ts.get("duration", 0),
                        stopovers=best_ib_ts.get("numberOfConnections", max(len(ib_segs) - 1, 0)),
                    )

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

            _nh_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
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
                        cabin_class=_nh_cabin,
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

            combined = round(price + ib_price, 2) if ib_route else price

            offers.append(
                FlightOffer(
                    id=f"nh_rt_{offer_id}" if ib_route else f"nh_{offer_id}",
                    price=combined,
                    currency=currency,
                    price_formatted=f"{combined:,.2f} {currency}",
                    outbound=route,
                    inbound=ib_route,
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
        url = (
            f"https://www.ana.co.jp/en/us/book-plan/search/flight-search/"
            f"?origin={req.origin}"
            f"&destination={req.destination}"
            f"&departureDate={dt.strftime('%Y-%m-%d')}"
            f"&adults={req.adults or 1}"
        )
        if req.return_from:
            rd = _to_datetime(req.return_from)
            url += f"&returnDate={rd.strftime('%Y-%m-%d')}"
        return url

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
                    id=f"rt_nh_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

_client = ANAConnectorClient()


async def search(request: FlightSearchRequest) -> FlightSearchResponse:
    return await _client.search_flights(request)
