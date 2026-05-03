"""
Almundo connector — Playwright browser + API response interception.

Almundo (part of CVC Corp group) is a major Latin American OTA
covering Argentina, Brazil, Colombia, and other LatAm markets.
The SPA behind WAF (returns 403 on direct HTTP) fires JSON search
APIs after page load.

Strategy:
1.  Launch Playwright browser (non-headless).
2.  Navigate to Almundo flight search results URL.
3.  Intercept JSON API responses containing flight data.
4.  Parse response into FlightOffers.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from datetime import datetime
from typing import Any

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)


def _parse_dt(s: Any) -> datetime:
    if not s:
        return datetime(2000, 1, 1)
    s = str(s)
    try:
        clean = s.split("+")[0] if "+" in s and "T" in s else s
        clean = clean.split(".")[0] if "." in clean else clean
        return datetime.fromisoformat(clean)
    except Exception:
        return datetime(2000, 1, 1)


def _extract_price(item: dict) -> tuple[float, str]:
    """Extract (price, currency) from an Almundo item dict."""
    price = 0.0
    currency = "ARS"
    for pkey in ("fare", "price", "totalFare", "totalPrice",
                 "amount", "total", "precio"):
        val = item.get(pkey)
        if isinstance(val, (int, float)) and val > 0:
            price = float(val)
            break
        if isinstance(val, dict):
            for ikey in ("total", "amount", "value", "totalFare", "final"):
                iv = val.get(ikey)
                if isinstance(iv, (int, float)) and iv > 0:
                    price = float(iv)
                    break
            if price > 0:
                break
    cur = item.get("currency") or item.get("currencyCode")
    if isinstance(cur, str) and len(cur) == 3:
        currency = cur
    return price, currency


def _extract_segments(item: dict, req: FlightSearchRequest) -> list[FlightSegment]:
    """Extract FlightSegment list from an Almundo item dict."""
    segs = (
        item.get("segments") or item.get("legs") or
        item.get("flights") or item.get("tramos") or []
    )
    if not isinstance(segs, list):
        segs = [segs] if isinstance(segs, dict) else []
    if not segs:
        segs = [{
            "airline": item.get("airline") or item.get("carrier", ""),
            "flightNo": item.get("flightNumber") or item.get("flightNo", ""),
            "origin": req.origin,
            "destination": req.destination,
            "departure": item.get("departure") or item.get("salida", ""),
            "arrival": item.get("arrival") or item.get("llegada", ""),
        }]
    result: list[FlightSegment] = []
    for seg in segs:
        if not isinstance(seg, dict):
            continue
        airline = (
            seg.get("airline") or seg.get("airlineCode") or
            seg.get("carrier") or seg.get("aerolinea", "")
        )
        if isinstance(airline, dict):
            airline = airline.get("code", "") or airline.get("name", "")
        fno = seg.get("flightNo") or seg.get("flightNumber") or seg.get("numero", "")
        origin = seg.get("origin") or seg.get("from") or seg.get("origen", req.origin)
        dest = seg.get("destination") or seg.get("to") or seg.get("destino", req.destination)
        if isinstance(origin, dict):
            origin = origin.get("code", "") or origin.get("iata", "")
        if isinstance(dest, dict):
            dest = dest.get("code", "") or dest.get("iata", "")
        dep = seg.get("departure") or seg.get("salida", "")
        arr = seg.get("arrival") or seg.get("llegada", "")
        result.append(FlightSegment(
            airline=str(airline),
            flight_no=f"{airline}{fno}" if fno else str(airline),
            origin=str(origin),
            destination=str(dest),
            departure=_parse_dt(dep),
            arrival=_parse_dt(arr),
        ))
    return result


class AlmundoConnectorClient:
    """Almundo — Latin American OTA, Playwright + API interception."""

    def __init__(self, timeout: float = 55.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(
        self, req: FlightSearchRequest
    ) -> FlightSearchResponse:
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
        """Almundo is WAF-protected; ancillary data not available."""
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

    async def _search_ow(
        self, req: FlightSearchRequest
    ) -> FlightSearchResponse:
        t0 = time.monotonic()

        for attempt in range(2):
            try:
                offers = await self._do_search(req)
                if offers is not None:
                    offers.sort(
                        key=lambda o: o.price if o.price > 0 else float("inf")
                    )
                    elapsed = time.monotonic() - t0
                    logger.info(
                        "ALMUNDO %s→%s: %d offers in %.1fs",
                        req.origin, req.destination, len(offers), elapsed,
                    )
                    h = hashlib.md5(
                        f"alm{req.origin}{req.destination}{req.date_from}".encode()
                    ).hexdigest()[:12]
                    return FlightSearchResponse(
                        search_id=f"fs_alm_{h}",
                        origin=req.origin,
                        destination=req.destination,
                        currency=req.currency,
                        offers=offers,
                        total_results=len(offers),
                    )
            except Exception as e:
                logger.warning("ALMUNDO attempt %d failed: %s", attempt, e)

        return self._empty(req)

    async def _do_search(
        self, req: FlightSearchRequest
    ) -> list[FlightOffer] | None:
        from playwright.async_api import async_playwright

        api_data: list[dict] = []

        async def on_response(response):
            url = response.url
            try:
                if response.status != 200:
                    return
                ct = response.headers.get("content-type", "")
                if "json" not in ct:
                    return
                keywords = ("search", "flight", "fare", "result", "vuelo",
                            "itinerar", "offer", "avail")
                if not any(k in url.lower() for k in keywords):
                    return
                body = await response.text()
                if len(body) < 3000:
                    return
                data = json.loads(body)
                if isinstance(data, dict):
                    for key in ("data", "result", "results", "flights",
                                "itineraries", "offers", "vuelos",
                                "flightResults", "items"):
                        val = data.get(key)
                        if isinstance(val, list) and len(val) > 2:
                            api_data.append(data)
                            logger.info("ALMUNDO: captured %s (%d items)", key, len(val))
                            return
                        if isinstance(val, dict):
                            for inner in ("flights", "itineraries", "offers",
                                          "outbound", "results", "items"):
                                iv = val.get(inner)
                                if isinstance(iv, list) and len(iv) > 2:
                                    api_data.append(data)
                                    logger.info("ALMUNDO: captured %s.%s", key, inner)
                                    return
            except Exception:
                pass

        pw = await async_playwright().start()
        try:
            from .browser import get_proxy
            proxy = get_proxy("ALMUNDO_PROXY")
            launch_kw: dict = {
                "headless": False,
                "args": [
                    "--window-position=-2400,-2400",
                    "--window-size=1366,768",
                    "--disable-blink-features=AutomationControlled",
                ],
            }
            if proxy:
                launch_kw["proxy"] = proxy
            browser = await pw.chromium.launch(**launch_kw)
            ctx = await browser.new_context(
                viewport={"width": 1366, "height": 768},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/135.0.0.0 Safari/537.36"
                ),
                locale="es-AR",
            )
            page = await ctx.new_page()
            if proxy:
                from .browser import auto_block_if_proxied
                await auto_block_if_proxied(page)
            page.on("response", on_response)

            # Almundo URL format
            dep = req.date_from.strftime("%Y-%m-%d")
            url = (
                f"https://www.almundo.com.ar/flights/results"
                f"/oneway/{req.origin}/{req.destination}/{dep}"
                f"/{req.adults or 1}/{req.children or 0}/0"
            )
            if req.return_from:
                ret = req.return_from.strftime("%Y-%m-%d")
                url = (
                    f"https://www.almundo.com.ar/flights/results"
                    f"/roundtrip/{req.origin}/{req.destination}"
                    f"/{dep}/{ret}"
                    f"/{req.adults or 1}/{req.children or 0}/0"
                )

            await page.goto(url, wait_until="domcontentloaded", timeout=30000)

            # Dismiss popups / cookie banners
            for sel in [
                "button:has-text('Acepto')",
                "button:has-text('Aceptar')",
                "button:has-text('Accept')",
                ".close-btn",
                ".modal-close",
                "[data-dismiss]",
            ]:
                try:
                    btn = page.locator(sel)
                    if await btn.count() > 0:
                        await btn.first.click(force=True, timeout=2000)
                        break
                except Exception:
                    pass

            # Wait for API responses
            for _ in range(8):
                await page.wait_for_timeout(5000)
                if api_data:
                    await page.wait_for_timeout(3000)
                    break

            await page.close()
            await ctx.close()
            await browser.close()
        except Exception as e:
            logger.error("ALMUNDO browser error: %s", e)
            return None
        finally:
            try:
                await pw.stop()
            except Exception:
                pass

        if not api_data:
            logger.warning("ALMUNDO: no API response captured")
            return None

        return _parse_almundo(api_data[0], req)

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        return FlightSearchResponse(
            search_id="",
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
                    id=f"rt_alm_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

def _parse_almundo(data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
    """Parse Almundo API response."""
    offers: list[FlightOffer] = []

    flights = None
    for key in ("data", "result", "results", "flights", "itineraries",
                "offers", "vuelos", "flightResults", "items"):
        val = data.get(key)
        if isinstance(val, list) and len(val) > 0:
            flights = val
            break
        if isinstance(val, dict):
            for inner in ("flights", "itineraries", "offers", "outbound",
                          "results", "items"):
                iv = val.get(inner)
                if isinstance(iv, list) and len(iv) > 0:
                    flights = iv
                    break
            if flights:
                break

    if not flights:
        return offers

    # ── Classify items into OB vs IB for RT ──
    items_ob = []
    items_ib = []
    if req.return_from:
        for item in flights:
            if not isinstance(item, dict):
                continue
            segs = (
                item.get("segments") or item.get("legs") or
                item.get("flights") or item.get("tramos") or []
            )
            if isinstance(segs, list) and segs:
                seg0 = segs[0] if isinstance(segs[0], dict) else {}
                first_origin = seg0.get("origin") or seg0.get("from") or seg0.get("origen", "")
                if isinstance(first_origin, dict):
                    first_origin = first_origin.get("code", "") or first_origin.get("iata", "")
                if str(first_origin).upper() == req.destination.upper():
                    items_ib.append(item)
                    continue
            items_ob.append(item)
    else:
        items_ob = [i for i in flights if isinstance(i, dict)]

    # ── Find cheapest IB ──
    ib_route = None
    ib_price = 0.0
    if items_ib and req.return_from:
        best_ib_price = float("inf")
        best_ib_segs: list[FlightSegment] = []
        best_ib_dur = 0
        for item in items_ib:
            p, _ = _extract_price(item)
            if p <= 0 or p >= best_ib_price:
                continue
            parsed = _extract_segments(item, req)
            if parsed:
                best_ib_price = p
                best_ib_segs = parsed
                best_ib_dur = item.get("duration") or item.get("totalDuration") or item.get("duracion") or 0
        if best_ib_segs and best_ib_price < float("inf"):
            ib_price = best_ib_price
            d = int(best_ib_dur) * 60 if isinstance(best_ib_dur, (int, float)) and best_ib_dur < 2000 else int(best_ib_dur) if isinstance(best_ib_dur, (int, float)) else 0
            ib_route = FlightRoute(
                segments=best_ib_segs,
                total_duration_seconds=d,
                stopovers=max(0, len(best_ib_segs) - 1),
            )

    for item in items_ob:
        try:
            if not isinstance(item, dict):
                continue

            price, currency = _extract_price(item)
            if price <= 0:
                continue

            flight_segments = _extract_segments(item, req)
            if not flight_segments:
                continue

            duration = item.get("duration") or item.get("totalDuration") or item.get("duracion") or 0
            stopovers = max(0, len(flight_segments) - 1)
            outbound = FlightRoute(
                segments=flight_segments,
                total_duration_seconds=int(duration) * 60 if isinstance(duration, (int, float)) and duration < 2000 else int(duration) if isinstance(duration, (int, float)) else 0,
                stopovers=stopovers,
            )

            airlines = list({s.airline for s in flight_segments if s.airline})
            combined = round(price + ib_price, 2) if ib_route else price
            h = hashlib.md5(
                f"alm{req.origin}{req.destination}{combined}{airlines}".encode()
            ).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"off_alm_rt_{h}" if ib_route else f"off_alm_{h}",
                price=combined,
                currency=currency,
                outbound=outbound,
                inbound=ib_route,
                airlines=airlines,
                owner_airline=airlines[0] if airlines else "Almundo",
                source="almundo",
                source_tier="ota",
                booking_url="https://www.almundo.com.ar/",
            ))
        except Exception as e:
            logger.debug("ALMUNDO: skipped item: %s", e)
            continue

    return offers
