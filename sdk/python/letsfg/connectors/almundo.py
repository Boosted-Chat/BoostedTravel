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


class AlmundoConnectorClient:
    """Almundo — Latin American OTA, Playwright + API interception."""

    def __init__(self, timeout: float = 55.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(
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
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
                locale="es-AR",
            )
            page = await ctx.new_page()
            if proxy:
                from .browser import block_heavy_resources
                await block_heavy_resources(page)
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

    for item in flights:
        try:
            if not isinstance(item, dict):
                continue

            price = 0.0
            currency = "ARS"
            for pkey in ("fare", "price", "totalFare", "totalPrice",
                         "amount", "total", "precio"):
                val = item.get(pkey)
                if isinstance(val, (int, float)) and val > 0:
                    price = float(val)
                    break
                if isinstance(val, dict):
                    for ikey in ("total", "amount", "value",
                                 "totalFare", "final"):
                        iv = val.get(ikey)
                        if isinstance(iv, (int, float)) and iv > 0:
                            price = float(iv)
                            break
                    if price > 0:
                        break

            if price <= 0:
                continue

            cur = item.get("currency") or item.get("currencyCode")
            if isinstance(cur, str) and len(cur) == 3:
                currency = cur

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

            flight_segments: list[FlightSegment] = []
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

                flight_segments.append(FlightSegment(
                    airline=str(airline),
                    flight_no=f"{airline}{fno}" if fno else str(airline),
                    origin=str(origin),
                    destination=str(dest),
                    departure=_parse_dt(dep),
                    arrival=_parse_dt(arr),
                ))

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
            h = hashlib.md5(
                f"alm{req.origin}{req.destination}{price}{airlines}".encode()
            ).hexdigest()[:12]

            _dep = req.date_from.strftime("%Y-%m-%d")
            offers.append(FlightOffer(
                id=f"off_alm_{h}",
                price=price,
                currency=currency,
                outbound=outbound,
                inbound=None,
                airlines=airlines,
                owner_airline=airlines[0] if airlines else "Almundo",
                source="almundo",
                source_tier="ota",
                booking_url=f"https://www.almundo.com.ar/flights/results/{req.origin}/{req.destination}/{_dep}/{req.adults or 1}/0/0",
            ))
        except Exception as e:
            logger.debug("ALMUNDO: skipped item: %s", e)
            continue

    return offers
