"""
GOL Linhas Aereas hybrid scraper -- headed Chrome + Angular navigation.

GOL (IATA: G3) is Brazil's largest low-cost carrier.
Website: b2c.voegol.com.br -- Angular SPA booking flow.

Strategy (persistent headed Chrome + Angular navigation):
1. Launch persistent headed Chrome (Akamai blocks headless)
2. Navigate to b2c.voegol.com.br/compra once -- Angular boots
3. For each search: inject sessionStorage search params -> navigate to results page
   -> Angular resolver fires BFF request -> intercept response
4. Navigate back to /compra for next search
5. Parse offers -> FlightOffer objects

Persistent page kept alive -- first search ~8s (Angular boot), subsequent ~3-5s.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
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
from .browser import auto_block_if_proxied, get_default_proxy

logger = logging.getLogger(__name__)

_GOL_BASE = "https://b2c.voegol.com.br"
_USER_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", ".gol_chrome_data")

_pw_instance = None
_pw_context = None
_persistent_page = None
_page_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _page_lock
    if _page_lock is None:
        _page_lock = asyncio.Lock()
    return _page_lock


async def _get_context():
    """Persistent headed Chrome context -- cookies survive across searches."""
    global _pw_instance, _pw_context
    if _pw_context:
        try:
            _pw_context.pages
            return _pw_context
        except Exception:
            _pw_context = None

    from playwright.async_api import async_playwright

    os.makedirs(os.path.abspath(_USER_DATA_DIR), exist_ok=True)
    _pw_instance = await async_playwright().start()

    _pw_context = await _pw_instance.chromium.launch_persistent_context(
        os.path.abspath(_USER_DATA_DIR),
        channel="chrome",
        headless=False,
        proxy=get_default_proxy(),
        args=[
            "--disable-blink-features=AutomationControlled",
            "--window-position=-2400,-2400",
            "--window-size=1366,768",
        ],
        viewport={"width": 1366, "height": 768},
        locale="pt-BR",
        timezone_id="America/Sao_Paulo",
        service_workers="block",
    )
    logger.info("GOL: persistent Chrome context ready")
    return _pw_context


async def _ensure_persistent_page():
    """Get or create a persistent page with Angular SPA loaded."""
    global _persistent_page

    if _persistent_page and not _persistent_page.is_closed():
        try:
            url = _persistent_page.url
            if "voegol.com.br" in url:
                return _persistent_page
        except Exception:
            pass

    ctx = await _get_context()
    page = await ctx.new_page()
    await auto_block_if_proxied(page)

    logger.info("GOL: loading Angular SPA...")
    await page.goto(f"{_GOL_BASE}/compra", wait_until="domcontentloaded", timeout=30000)
    # Wait for network to settle so Angular boots fully and populates sessionStorage
    try:
        await page.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass
    await asyncio.sleep(2)

    # Dismiss LGPD/cookie overlays
    await _dismiss_cookies(page)

    # Wait for Angular to boot and populate session UUID
    try:
        uuid = await page.wait_for_function("""() => {
            // GOL stores UUID in currentTabUUID key directly
            const directUUID = sessionStorage.getItem('currentTabUUID');
            if (directUUID && /^[0-9a-f-]{36}$/.test(directUUID)) return directUUID;
            // Fallback: find UUID from namespaced key prefixes
            for (let i = 0; i < sessionStorage.length; i++) {
                const key = sessionStorage.key(i);
                const m = key.match(/^([0-9a-f-]{36})_@SiteGolB2C/)
                    || key.match(/^([0-9a-f-]{36})[@_](?:SiteGol|GolSite|gol)/i);
                if (m) return m[1];
            }
            return null;
        }""", timeout=25000)
        uuid_val = await uuid.json_value()
        if uuid_val:
            logger.info("GOL: Angular SPA ready (UUID=%s...)", uuid_val[:8])
    except Exception:
        logger.warning("GOL: Angular UUID not found after 15s")

    _persistent_page = page
    return page


async def _dismiss_cookies(page) -> None:
    for sel in [
        "button:has-text('Continuar e fechar')",
        "button:has-text('Continue and close')",
    ]:
        try:
            el = page.locator(sel).first
            if await el.count() > 0 and await el.is_visible():
                await el.click(timeout=2000)
                await asyncio.sleep(0.5)
                return
        except Exception:
            continue
    try:
        await page.evaluate("""() => {
            document.querySelectorAll(
                '[class*="cookie"], [id*="cookie"], [class*="consent"], [id*="consent"], '
                + '[class*="onetrust"], [id*="onetrust"], [class*="lgpd"]'
            ).forEach(el => { if (el.offsetHeight > 0) el.remove(); });
            document.body.style.overflow = 'auto';
        }""")
    except Exception:
        pass




class GolConnectorClient:
    """GOL scraper — CDP Chrome + Angular navigation."""

    def __init__(self, timeout: float = 45.0):
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
        lock = _get_lock()
        async with lock:
            try:
                return await self._attempt_search(req, t0)
            except Exception as e:
                logger.error("GOL search error: %s", e)
                return self._empty(req)

    async def _attempt_search(
        self, req: FlightSearchRequest, t0: float
    ) -> FlightSearchResponse:
        global _persistent_page

        page = await _ensure_persistent_page()

        # Extract session UUID
        uuid = await page.evaluate("""() => {
            const directUUID = sessionStorage.getItem('currentTabUUID');
            if (directUUID && /^[0-9a-f-]{36}$/.test(directUUID)) return directUUID;
            for (let i = 0; i < sessionStorage.length; i++) {
                const key = sessionStorage.key(i);
                const m = key.match(/^([0-9a-f-]{36})_@SiteGolB2C/)
                    || key.match(/^([0-9a-f-]{36})[@_](?:SiteGol|GolSite|gol)/i);
                if (m) return m[1];
            }
            return null;
        }""")
        if not uuid:
            logger.warning("GOL: no session UUID, resetting page")
            _persistent_page = None
            page = await _ensure_persistent_page()
            uuid = await page.evaluate("""() => {
                const directUUID = sessionStorage.getItem('currentTabUUID');
                if (directUUID && /^[0-9a-f-]{36}$/.test(directUUID)) return directUUID;
                for (let i = 0; i < sessionStorage.length; i++) {
                    const key = sessionStorage.key(i);
                    const m = key.match(/^([0-9a-f-]{36})_@SiteGolB2C/)
                        || key.match(/^([0-9a-f-]{36})[@_](?:SiteGol|GolSite|gol)/i);
                    if (m) return m[1];
                }
                return null;
            }""")
            if not uuid:
                return self._empty(req)

        # Build sessionStorage search payload
        dep_date = req.date_from.isoformat()
        itinerary_parts = [{
            "from": {"code": req.origin, "useNearbyLocations": False},
            "to": {"code": req.destination, "useNearbyLocations": False},
            "when": {"date": f"{dep_date}T00:00:00"},
        }]
        is_roundtrip = req.return_from is not None
        if is_roundtrip and req.return_from:
            ret_date = req.return_from.isoformat()
            itinerary_parts.append({
                "from": {"code": req.destination, "useNearbyLocations": False},
                "to": {"code": req.origin, "useNearbyLocations": False},
                "when": {"date": f"{ret_date}T00:00:00"},
            })

        # Map cabin code to GOL cabin class (ECONOMY, EXECUTIVA)
        _g3_cabin = {"M": "ECONOMY", "W": "ECONOMY", "C": "EXECUTIVA", "F": "EXECUTIVA"}.get(req.cabin_class or "M", "ECONOMY")
        search_payload = {
            "promocodebanner": False,
            "destinationCountryToUSA": False,
            "lastSearchCourtesyTicket": False,
            "passengerCourtesyType": None,
            "airSearch": {
                "cabinClass": _g3_cabin,
                "currency": None,
                "pointOfSale": "BR",
                "awardBooking": False,
                "searchType": "BRANDED",
                "promoCodes": [""],
                "originalItineraryParts": itinerary_parts,
                "itineraryParts": itinerary_parts,
                "passengers": {
                    "ADT": req.adults,
                    "TEEN": 0,
                    "CHD": req.children,
                    "INF": req.infants,
                    "UNN": 0,
                },
            },
        }
        journey_type = "round-trip" if is_roundtrip else "one-way"
        passengers = {
            "ADT": req.adults, "TEEN": 0,
            "CHD": req.children, "INF": req.infants, "UNN": 0,
        }

        # Inject search params into sessionStorage
        await page.evaluate("""({uuid, search, journey, passengers}) => {
            sessionStorage.setItem(uuid + '_@SiteGolB2C:search', JSON.stringify(search));
            sessionStorage.setItem(uuid + '_@SiteGolB2C:search-properties', JSON.stringify({journey: journey}));
            sessionStorage.setItem(uuid + '_@SiteGolB2C:passengers', JSON.stringify(passengers));
            sessionStorage.setItem('flightSelectionScreen', JSON.stringify('v2'));
        }""", {
            "uuid": uuid,
            "search": search_payload,
            "journey": journey_type,
            "passengers": passengers,
        })

        logger.info("GOL: searching %s→%s via Angular navigation", req.origin, req.destination)

        # Set up BFF response interception
        captured_data: dict = {}
        api_event = asyncio.Event()

        async def on_response(response):
            try:
                if "bff-flight" in response.url and "search" in response.url and response.status == 200:
                    ct = response.headers.get("content-type", "")
                    if "json" in ct:
                        data = await response.json()
                        if data and isinstance(data, dict) and "offers" in data:
                            captured_data["json"] = data
                            api_event.set()
            except Exception:
                pass

        page.on("response", on_response)
        try:
            # Navigate to results page — Angular resolver fires BFF search
            await page.goto(f"{_GOL_BASE}/compra/selecao-de-voo2/ida",
                            wait_until="domcontentloaded", timeout=int(self.timeout * 1000))

            remaining = max(self.timeout - (time.monotonic() - t0), 10)
            try:
                await asyncio.wait_for(api_event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                logger.warning("GOL: timed out waiting for BFF response")
                return self._empty(req)
        finally:
            page.remove_listener("response", on_response)

        data = captured_data.get("json", {})
        if not data:
            return self._empty(req)

        # Navigate back to /compra for next search (keeps SPA alive)
        try:
            await page.goto(f"{_GOL_BASE}/compra",
                            wait_until="domcontentloaded", timeout=15000)
            await asyncio.sleep(1)
        except Exception:
            pass

        elapsed = time.monotonic() - t0
        offers = self._parse_response(data, req)

        # RT: do a second search for return direction
        if req.return_from and offers:
            return_offers = await self._search_return(page, uuid, req, t0)
            if return_offers:
                combos = self._build_rt_combos(offers, return_offers, req)
                if combos:
                    offers = combos + offers

        return self._build_response(offers, req, elapsed)

    # ── Return direction search ────────────────────────────────────────────

    async def _search_return(
        self, page, uuid: str, req: FlightSearchRequest, t0: float,
    ) -> list[FlightOffer]:
        """Search for return direction by injecting reversed search params."""
        ret_date = req.return_from.isoformat()
        itinerary_parts = [{
            "from": {"code": req.destination, "useNearbyLocations": False},
            "to": {"code": req.origin, "useNearbyLocations": False},
            "when": {"date": f"{ret_date}T00:00:00"},
        }]
        # Map cabin code to GOL cabin class (ECONOMY, EXECUTIVA)
        _g3_cabin_ib = {"M": "ECONOMY", "W": "ECONOMY", "C": "EXECUTIVA", "F": "EXECUTIVA"}.get(req.cabin_class or "M", "ECONOMY")
        search_payload = {
            "promocodebanner": False,
            "destinationCountryToUSA": False,
            "lastSearchCourtesyTicket": False,
            "passengerCourtesyType": None,
            "airSearch": {
                "cabinClass": _g3_cabin_ib,
                "currency": None,
                "pointOfSale": "BR",
                "awardBooking": False,
                "searchType": "BRANDED",
                "promoCodes": [""],
                "originalItineraryParts": itinerary_parts,
                "itineraryParts": itinerary_parts,
                "passengers": {
                    "ADT": req.adults, "TEEN": 0,
                    "CHD": req.children, "INF": req.infants, "UNN": 0,
                },
            },
        }
        await page.evaluate("""({uuid, search, passengers}) => {
            sessionStorage.setItem(uuid + '_@SiteGolB2C:search', JSON.stringify(search));
            sessionStorage.setItem(uuid + '_@SiteGolB2C:search-properties', JSON.stringify({journey: 'one-way'}));
            sessionStorage.setItem(uuid + '_@SiteGolB2C:passengers', JSON.stringify(passengers));
        }""", {
            "uuid": uuid,
            "search": search_payload,
            "passengers": {
                "ADT": req.adults, "TEEN": 0,
                "CHD": req.children, "INF": req.infants, "UNN": 0,
            },
        })

        captured_data: dict = {}
        api_event = asyncio.Event()

        async def on_response(response):
            try:
                if "bff-flight" in response.url and "search" in response.url and response.status == 200:
                    ct = response.headers.get("content-type", "")
                    if "json" in ct:
                        data = await response.json()
                        if data and isinstance(data, dict) and "offers" in data:
                            captured_data["json"] = data
                            api_event.set()
            except Exception:
                pass

        page.on("response", on_response)
        try:
            await page.goto(f"{_GOL_BASE}/compra/selecao-de-voo2/ida",
                            wait_until="domcontentloaded", timeout=int(self.timeout * 1000))
            remaining = max(self.timeout - (time.monotonic() - t0), 5)
            try:
                await asyncio.wait_for(api_event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                logger.warning("GOL: timed out waiting for return BFF response")
                return []
        finally:
            page.remove_listener("response", on_response)

        data = captured_data.get("json", {})
        if not data:
            return []

        # Navigate back for next search
        try:
            await page.goto(f"{_GOL_BASE}/compra",
                            wait_until="domcontentloaded", timeout=15000)
            await asyncio.sleep(1)
        except Exception:
            pass

        return self._parse_response(data, req, is_return=True)

    # ── Response parsing (GOL BFF format) ───────────────────────────────────

    def _parse_response(self, data: dict, req: FlightSearchRequest, is_return: bool = False) -> list[FlightOffer]:
        raw_offers = data.get("offers") or []
        if not raw_offers:
            return []

        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        for offer_data in raw_offers:
            parsed = self._parse_offer(offer_data, req, booking_url, is_return=is_return)
            if parsed:
                offers.append(parsed)

        return offers

    def _parse_offer(
        self, offer_data: dict, req: FlightSearchRequest, booking_url: str,
        is_return: bool = False,
    ) -> Optional[FlightOffer]:
        itinerary = offer_data.get("itinerary", {})
        fare_family = offer_data.get("fareFamily", [])
        segments_raw = offer_data.get("segments", [])

        # Find cheapest fare (LI = Light is typically cheapest)
        best_price = float("inf")
        best_currency = "BRL"
        cheapest_fare: dict = {}
        for fare in fare_family:
            price_info = fare.get("price", {})
            total = price_info.get("total")
            if total is not None and 0 < total < best_price:
                best_price = total
                best_currency = price_info.get("currency", "BRL")
                cheapest_fare = fare

        if best_price == float("inf") or best_price <= 0:
            return None

        conditions, bags_price = self._extract_gol_bag_info(cheapest_fare, best_currency)

        # Map cabin code to cabin name for segment
        _g3_cabin_name = {"M": "economy", "W": "economy", "C": "business", "F": "business"}.get(req.cabin_class or "M", "economy")
        segments: list[FlightSegment] = []
        for seg in segments_raw:
            segments.append(FlightSegment(
                airline=seg.get("operatingAirlineCode", "G3"),
                airline_name="GOL",
                flight_no=f"G3{seg.get('flightNumber', '')}",
                origin=seg.get("origin", req.origin),
                destination=seg.get("destination", req.destination),
                departure=self._parse_dt(seg.get("departure", "")),
                arrival=self._parse_dt(seg.get("arrival", "")),
                duration_seconds=seg.get("duration", 0) * 60,
                cabin_class=_g3_cabin_name,
            ))

        if not segments:
            return None

        total_dur = itinerary.get("totalDuration", 0) * 60  # minutes → seconds
        stops = itinerary.get("stops", max(len(segments) - 1, 0))

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=total_dur,
            stopovers=stops,
        )

        dep = itinerary.get("departure", "")
        flight_nums = "-".join(str(s.get("flightNumber", "")) for s in segments_raw)
        rt_tag = "_rt" if is_return else ""
        offer_key = f"{dep}_{flight_nums}{rt_tag}"

        return FlightOffer(
            id=f"g3_{hashlib.md5(offer_key.encode()).hexdigest()[:12]}",
            price=round(best_price, 2),
            currency=best_currency,
            price_formatted=f"{best_price:.2f} {best_currency}",
            outbound=None if is_return else route,
            inbound=route if is_return else None,
            airlines=list(set(s.airline for s in segments)),
            owner_airline="G3",
            conditions=conditions,
            bags_price=bags_price,
            booking_url=booking_url,
            is_locked=False,
            source="gol_direct",
            source_tier="protocol",
        )

    @staticmethod
    def _extract_gol_bag_info(fare: dict, currency: str) -> tuple[dict, dict]:
        """Extract bag allowance from a GOL fareFamily entry."""
        conditions: dict[str, str] = {}
        bags_price: dict[str, float] = {}
        if not fare:
            return conditions, bags_price

        fare_name = str(fare.get("name") or fare.get("code") or fare.get("fareCode") or "")
        if fare_name:
            conditions["fare_family"] = fare_name

        # Try structured bag allowance field (several possible names)
        bag_allow = (
            fare.get("baggageAllowance")
            or fare.get("baggage")
            or fare.get("checkedBaggage")
            or fare.get("luggageAllowance")
            or {}
        )
        if isinstance(bag_allow, dict):
            qty = (
                bag_allow.get("quantity") or bag_allow.get("pieces")
                or bag_allow.get("count") or bag_allow.get("numberOfBags")
            )
            weight = bag_allow.get("weight") or bag_allow.get("maxWeight") or 23
            try:
                qty_int = int(qty) if qty is not None else -1
            except (TypeError, ValueError):
                qty_int = -1

            if qty_int == 0:
                conditions["checked_bag"] = "no free checked bag"
                add_bag = (
                    fare.get("additionalBaggage")
                    or fare.get("extraBaggage")
                    or fare.get("upgradeBaggage")
                    or {}
                )
                if isinstance(add_bag, dict):
                    add_price = (
                        add_bag.get("totalAmount") or add_bag.get("amount")
                        or (add_bag.get("price") or {}).get("total")
                    )
                    if add_price:
                        try:
                            p = float(add_price)
                            bags_price["checked_bag"] = p
                            conditions["checked_bag"] += f"; add-on from +{currency} {p:.0f}"
                        except (TypeError, ValueError):
                            pass
            elif qty_int > 0:
                conditions["checked_bag"] = f"{qty_int}x {weight}kg bag included"
                bags_price["checked_bag"] = 0.0
        elif isinstance(bag_allow, (int, float)):
            qty_int = int(bag_allow)
            if qty_int == 0:
                conditions["checked_bag"] = "no free checked bag"
            else:
                conditions["checked_bag"] = f"{qty_int}x bag included"
                bags_price["checked_bag"] = 0.0

        # Fallback: infer from GOL fare family name
        if "checked_bag" not in conditions and fare_name:
            name_upper = fare_name.upper()
            if "LIGHT" in name_upper or name_upper in ("LI", "LITE", "PROMO"):
                conditions["checked_bag"] = "no free checked bag (Light/Promo fare)"
            elif name_upper in ("SMART", "SM", "PLUS", "PL", "EC", "REGULAR") or any(
                k in name_upper for k in ("SMART", "PLUS", "REGULAR", "ECONOMY")
            ):
                conditions["checked_bag"] = "1x 23kg bag included"
                bags_price["checked_bag"] = 0.0
            elif name_upper in ("MAX", "MX", "TOP", "PREMIUM") or any(
                k in name_upper for k in ("MAX", "TOP", "PREMIUM", "EXECUTIVO")
            ):
                conditions["checked_bag"] = "2x 23kg bags included"
                bags_price["checked_bag"] = 0.0

        # Carry-on: infer from GOL fare family name
        if "carry_on" not in conditions:
            name_upper = fare_name.upper() if fare_name else ""
            if "LIGHT" in name_upper or name_upper in ("LI", "LITE", "PROMO"):
                conditions["carry_on"] = "1x 10kg carry-on included (no free checked bag on Light fare)"
                bags_price["carry_on"] = 0.0
            elif any(k in name_upper for k in ("MAX", "EXECUTIVO", "PREMIUM")):
                conditions["carry_on"] = "2x carry-on bags included"
                bags_price["carry_on"] = 0.0
            else:
                conditions["carry_on"] = "1x 10kg carry-on included"
                bags_price["carry_on"] = 0.0

        # Seat selection
        if "seat" not in conditions:
            conditions["seat"] = f"seat selection from ~{currency} 30 — add at checkout"

        return conditions, bags_price

    def _build_response(
        self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float
    ) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info(
            "GOL %s→%s returned %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )
        h = hashlib.md5(
            f"gol{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else req.currency,
            offers=offers,
            total_results=len(offers),
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
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(s[:len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        url = (
            f"{_GOL_BASE}/compra/selecao-de-voo2/ida"
            f"?origin={req.origin}&destination={req.destination}"
            f"&departure={dep}&adults={req.adults}"
        )
        if req.return_from:
            url += f"&returnDate={req.return_from.strftime('%Y-%m-%d')}&journeyType=round-trip"
        return url

    def _build_rt_combos(
        self,
        outbound: list[FlightOffer],
        inbound: list[FlightOffer],
        req: FlightSearchRequest,
    ) -> list[FlightOffer]:
        combos: list[FlightOffer] = []
        booking_url = self._build_booking_url(req)
        for ob in outbound[:15]:
            for ib in inbound[:10]:
                total = (ob.price or 0) + (ib.price or 0)
                combo_hash = hashlib.md5(
                    f"{ob.id}_{ib.id}".encode()
                ).hexdigest()[:10]
                combos.append(FlightOffer(
                    id=f"g3_rt_{combo_hash}",
                    price=round(total, 2),
                    currency=ob.currency,
                    price_formatted=f"{total:.2f} {ob.currency}",
                    outbound=ob.outbound,
                    inbound=ib.inbound,
                    airlines=["G3"],
                    owner_airline="G3",
                    booking_url=booking_url,
                    is_locked=False,
                    source="gol_direct",
                    source_tier="protocol",
                ))
        combos.sort(key=lambda o: o.price or 0)
        return combos[:50]

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"gol{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
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
                    id=f"rt_gol_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
