"""
Almosafer connector — Playwright browser + API interception.

Almosafer (global.almosafer.com) is a major MENA OTA owned by Saudi Tourism
Authority.  Also operates as Tajawal and Flyin.  Built on Next.js with
client-side flight search via XHR.

Strategy:
1.  Launch Playwright browser to homepage (gets session cookies).
2.  Dismiss MUI modal overlay that blocks interaction.
3.  Fill search form: origin, destination, date.
4.  Submit → Next.js navigates to search-results page and polls API.
5.  Intercept ``/api/v3/flights/flight/search`` JSON responses containing
    ``airItineraries``.  Fall back to DOM scraping.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
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
from .browser import get_proxy

logger = logging.getLogger(__name__)

# Common IATA → airline name map for MENA/EU routes
_AIRLINE_NAMES: dict[str, str] = {
    "SV": "Saudia", "XY": "flynas", "F3": "flyadeal", "EK": "Emirates",
    "QR": "Qatar Airways", "EY": "Etihad", "GF": "Gulf Air", "G9": "Air Arabia",
    "J9": "Jazeera Airways", "KU": "Kuwait Airways", "WY": "Oman Air",
    "RJ": "Royal Jordanian", "MS": "EgyptAir", "TK": "Turkish Airlines",
    "BA": "British Airways", "AF": "Air France", "LH": "Lufthansa",
    "KL": "KLM", "IB": "Iberia", "VY": "Vueling", "FR": "Ryanair",
    "U2": "easyJet", "W6": "Wizz Air", "LX": "SWISS", "OS": "Austrian",
    "AZ": "ITA Airways", "SK": "SAS", "AY": "Finnair", "TP": "TAP Portugal",
    "LO": "LOT Polish", "RO": "TAROM", "PC": "Pegasus", "SU": "Aeroflot",
    "DL": "Delta", "AA": "American Airlines", "UA": "United Airlines",
    "AC": "Air Canada", "QF": "Qantas", "SQ": "Singapore Airlines",
    "CX": "Cathay Pacific", "NH": "ANA", "JL": "Japan Airlines",
    "AI": "Air India", "6E": "IndiGo", "WS": "WestJet",
    "HR": "Hahn Air", "UX": "Air Europa", "A3": "Aegean Airlines",
}


def _parse_dt(s: Any) -> datetime:
    if not s:
        return datetime(2000, 1, 1)
    s = str(s)
    try:
        clean = s.split("+")[0] if "+" in s and "T" in s else s
        clean = clean.split(".")[0] if "." in clean else clean
        return datetime.fromisoformat(clean)
    except (ValueError, AttributeError):
        return datetime(2000, 1, 1)


class AlmosaferConnectorClient:
    """Almosafer / Tajawal / Flyin — MENA OTA, Playwright + API interception."""

    def __init__(self, timeout: float = 60.0):
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
                    offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
                    elapsed = time.monotonic() - t0
                    logger.info(
                        "ALMOSAFER %s→%s: %d offers in %.1fs",
                        req.origin, req.destination, len(offers), elapsed,
                    )
                    h = hashlib.md5(
                        f"alm{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
                    ).hexdigest()[:12]
                    return FlightSearchResponse(
                        search_id=f"fs_alm_{h}",
                        origin=req.origin,
                        destination=req.destination,
                        currency=offers[0].currency if offers else req.currency,
                        offers=offers,
                        total_results=len(offers),
                    )
            except Exception as e:
                logger.warning("ALMOSAFER attempt %d failed: %s", attempt, e)

        return self._empty(req)

    async def _do_search(self, req: FlightSearchRequest):
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
                # Only capture the async-search-result endpoint (the real flight data)
                # and the resources endpoint (airline names).
                if "async-search-result" in url or "iron-bank/api/resources" in url:
                    body = await response.text()
                    if len(body) > 500:
                        data = json.loads(body)
                        if isinstance(data, dict):
                            api_data.append(data)
            except Exception:
                pass

        pw = await async_playwright().start()
        try:
            proxy = get_proxy("ALMOSAFER_PROXY")
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
            )
            page = await ctx.new_page()
            if proxy:
                from .browser import block_heavy_resources
                await block_heavy_resources(page)
            page.on("response", on_response)

            # Direct URL navigation — bypasses form fill + MUI modal issues
            dep_date = req.date_from.isoformat()
            pax = f"{req.adults}Adult"
            search_url = (
                f"https://global.almosafer.com/en/flights/"
                f"{req.origin}-{req.destination}/{dep_date}/Economy/{pax}"
            )
            await page.goto(search_url, wait_until="domcontentloaded", timeout=30000)

            # Wait for async-search-result polls to return flight data
            deadline = time.monotonic() + min(self.timeout, 50)
            offers: list[FlightOffer] = []

            for _ in range(20):
                await page.wait_for_timeout(2500)

                # Parse flight data from async-search-result
                for data in api_data:
                    res = data.get("res")
                    if not isinstance(res, list):
                        continue
                    for provider_result in res:
                        if provider_result.get("status") != 200:
                            continue
                        pdata = provider_result.get("data", {})
                        if not isinstance(pdata, dict):
                            continue
                        parsed = self._parse_provider_data(pdata, req, dep_date)
                        offers.extend(parsed)

                if offers:
                    break

                # DOM fallback
                try:
                    dom_offers = await self._scrape_dom(page, req, dep_date)
                    if dom_offers:
                        offers = dom_offers
                        break
                except Exception:
                    pass

                if time.monotonic() > deadline:
                    break

            await page.close()
            await ctx.close()
            await browser.close()
        except Exception as e:
            logger.error("ALMOSAFER browser error: %s", e)
            return None
        finally:
            try:
                await pw.stop()
            except Exception:
                pass

        return offers if offers else None

    def _parse_provider_data(
        self, pdata: dict, req: FlightSearchRequest, date_str: str,
    ) -> list[FlightOffer]:
        """Parse a provider result from ``async-search-result``.

        ``pdata`` contains relational arrays:
        - ``itinerary`` – list of offers with ``price.totals``, ``legId``, ``validatingCarrier``
        - ``leg``       – list of legs with ``departure``, ``arrival``, ``originId``, ``destinationId``
        - ``segment``   – list of flight segments with carrier info
        """
        offers: list[FlightOffer] = []
        itins = pdata.get("itinerary")
        legs = pdata.get("leg")
        segments = pdata.get("segment")

        if not isinstance(itins, list) or not isinstance(legs, list):
            return offers

        # Build lookup maps
        leg_map: dict[str, dict] = {}
        for lg in legs:
            if isinstance(lg, dict) and "id" in lg:
                leg_map[lg["id"]] = lg

        seg_map: dict[str, dict] = {}
        if isinstance(segments, list):
            for sg in segments:
                if isinstance(sg, dict) and "id" in sg:
                    seg_map[sg["id"]] = sg

        for itin in itins[:50]:
            try:
                if not isinstance(itin, dict):
                    continue

                # Price — ``price.totals.total`` and ``price.totals.currency``
                price_obj = itin.get("price", {})
                price = 0.0
                currency = req.currency or "USD"
                if isinstance(price_obj, dict):
                    totals = price_obj.get("totals", {})
                    if isinstance(totals, dict):
                        price = float(totals.get("total") or 0)
                        currency = totals.get("currency", currency)
                    if price <= 0:
                        price = float(price_obj.get("total") or price_obj.get("amount") or 0)
                        currency = price_obj.get("currency", currency)
                elif isinstance(price_obj, (int, float)):
                    price = float(price_obj)

                if price <= 0:
                    continue

                # Leg — ``legId`` is a list of IDs
                leg_ids = itin.get("legId", [])
                if isinstance(leg_ids, str):
                    leg_ids = [leg_ids]
                leg = leg_map.get(leg_ids[0], {}) if leg_ids else {}

                # Validating carrier
                carrier = itin.get("validatingCarrier") or leg.get("validatingCarrier") or ""

                dep_str = leg.get("departure", "")
                arr_str = leg.get("arrival", "")
                dep_dt = _parse_dt(dep_str) if dep_str else _parse_dt(f"{date_str}T00:00:00")
                arr_dt = _parse_dt(arr_str) if arr_str else _parse_dt(f"{date_str}T00:00:00")

                dur_sec = 0
                if dep_dt and arr_dt and arr_dt > dep_dt:
                    dur_sec = int((arr_dt - dep_dt).total_seconds())

                origin_id = leg.get("originId", req.origin)
                dest_id = leg.get("destinationId", req.destination)

                # Stops — from itinerary or leg
                stops = int(itin.get("totalStops") or leg.get("stopCount") or 0)

                # Airline
                airline_code = carrier or ""
                airline_name = _AIRLINE_NAMES.get(airline_code, airline_code) or "Almosafer"

                # Flight path for display
                flight_codes = itin.get("path") or itin.get("flightCodes") or []
                if isinstance(flight_codes, str):
                    flight_codes = [flight_codes]
                flight_no = str(flight_codes[0]) if flight_codes else ""

                flight_no = str(flight_codes[0]) if flight_codes else ""

                seg_list = [FlightSegment(
                    airline=airline_name, flight_no=flight_no,
                    origin=str(origin_id), destination=str(dest_id),
                    departure=dep_dt, arrival=arr_dt,
                    duration_seconds=dur_sec,
                )]
                route = FlightRoute(
                    segments=seg_list,
                    total_duration_seconds=dur_sec,
                    stopovers=stops,
                )

                oid = hashlib.md5(
                    f"alm_{req.origin}{req.destination}{price}_{itin.get('id', id(itin))}".encode()
                ).hexdigest()[:12]

                book_url = (
                    f"https://global.almosafer.com/en/flights/"
                    f"{req.origin}-{req.destination}/{date_str}/Economy/{req.adults}Adult"
                )
                offers.append(FlightOffer(
                    id=f"alm_{oid}", price=round(price, 2), currency=str(currency),
                    price_formatted=f"{price:.2f} {currency}",
                    outbound=route, inbound=None,
                    airlines=[airline_name], owner_airline=airline_name,
                    booking_url=book_url,
                    is_locked=False, source="almosafer_ota", source_tier="free",
                ))
            except Exception:
                continue
        return offers

    def _parse_next_data(self, nd: dict, req: FlightSearchRequest, date_str: str) -> list[FlightOffer]:
        """Extract flight data from Next.js __NEXT_DATA__ props."""
        try:
            props = nd.get("props", {}).get("pageProps", {})
            return self._parse_api(props, req, date_str)
        except Exception:
            return []

    async def _scrape_dom(self, page, req: FlightSearchRequest, date_str: str) -> list[FlightOffer]:
        """DOM scraping fallback for Almosafer results page."""
        offers: list[FlightOffer] = []
        try:
            cards = page.locator(
                '[data-testid*="flight"], .flight-card, .result-card, '
                '.itinerary-card, .offer-card, [class*="FlightCard"], '
                '[class*="flight-result"]'
            )
            count = await cards.count()
            if count == 0:
                return offers

            for i in range(min(count, 50)):
                try:
                    card = cards.nth(i)
                    text = await card.inner_text()

                    # Price extraction (SAR, USD, EUR, GBP, AED, KWD, BHD, OMR, QAR)
                    price_m = re.search(r'(?:SAR|USD|EUR|GBP|AED|KWD|BHD|OMR|QAR)?\s*([\d,.]+)', text)
                    if not price_m:
                        continue
                    price = float(price_m.group(1).replace(",", ""))
                    if price <= 0 or price > 100000:
                        continue

                    # Currency detection
                    currency = "SAR"
                    for cur in ["USD", "EUR", "GBP", "AED", "KWD", "BHD", "OMR", "QAR"]:
                        if cur in text:
                            currency = cur
                            break

                    times = re.findall(r'(\d{1,2}:\d{2})', text)
                    dep_time = times[0] if times else "00:00"
                    arr_time = times[1] if len(times) > 1 else "00:00"

                    airline = "Almosafer"
                    for known in ["Saudia", "flynas", "flyadeal", "Gulf Air", "Emirates",
                                  "Qatar", "Etihad", "Air Arabia", "Jazeera", "Kuwait Airways"]:
                        if known.lower() in text.lower():
                            airline = known
                            break

                    dep_dt = _parse_dt(f"{date_str}T{dep_time}:00")
                    arr_dt = _parse_dt(f"{date_str}T{arr_time}:00")
                    dur = 0
                    if dep_dt.hour and arr_dt.hour:
                        dur = int((arr_dt - dep_dt).total_seconds())
                        if dur < 0:
                            dur += 86400

                    segments = [FlightSegment(
                        airline=airline, flight_no="",
                        origin=req.origin, destination=req.destination,
                        departure=dep_dt, arrival=arr_dt, duration_seconds=max(dur, 0),
                    )]
                    route = FlightRoute(segments=segments, total_duration_seconds=max(dur, 0), stopovers=0)
                    oid = hashlib.md5(f"alm_{i}_{price}".encode()).hexdigest()[:12]

                    offers.append(FlightOffer(
                        id=f"alm_{oid}", price=round(price, 2), currency=currency,
                        price_formatted=f"{price:.2f} {currency}",
                        outbound=route, inbound=None,
                        airlines=[airline], owner_airline=airline,
                        booking_url=f"https://global.almosafer.com/en/flights/{req.origin}-{req.destination}/{date_str}/Economy/{req.adults}Adult",
                        is_locked=False, source="almosafer_ota", source_tier="free",
                    ))
                except Exception:
                    continue
        except Exception:
            pass
        return offers

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        return FlightSearchResponse(
            search_id="",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=[],
            total_results=0,
        )
