"""
Samoa Airways CDP Chrome connector — TFLite booking form.

Samoa Airways (IATA: PH / marketing as Samoa Airways) is the national
airline of Samoa. Booking engine at apps1.tflite.com/Public/sma.
No JSON API — only web form. Requires CDP Chrome to render and scrape.

Strategy (CDP Chrome + form fill + DOM scraping):
1. Launch Playwright headless (no WAF detected on TFLite).
2. Navigate to TFLite booking form.
3. Fill search form (origin, destination, date, passengers).
4. Submit → wait for results page.
5. Scrape flight results from rendered DOM.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import time
from datetime import datetime, timedelta
from typing import Optional

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import acquire_browser_slot, release_browser_slot

logger = logging.getLogger(__name__)

_SEARCH_URL = "https://apps1.tflite.com/Public/sma/Booking/Search"

# Samoa Airways route network — inter-island only (TFLite booking engine)
# Map IATA code → TFLite <select> option value (airport name as shown in form)
_IATA_TO_NAME: dict[str, str] = {
    "AAU": "Asau",
    "FGI": "Fagalii Airport",
    "APW": "Faleolo International Airport",
    "FTI": "Fitiuta",
    "MXS": "Maota",
    "OFU": "Ofu",
    "PPG": "Pago Pago International Airport",
}
_VALID_IATA: set[str] = set(_IATA_TO_NAME.keys())


class SamoaAirwaysConnectorClient:
    """Samoa Airways — TFLite booking via Playwright headless."""

    def __init__(self, timeout: float = 40.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        if req.origin not in _VALID_IATA or req.destination not in _VALID_IATA:
            return self._empty(req)

        await acquire_browser_slot()
        try:
            return await self._search_with_browser(req, t0)
        finally:
            release_browser_slot()

    async def _search_with_browser(
        self, req: FlightSearchRequest, t0: float
    ) -> FlightSearchResponse:
        from playwright.async_api import async_playwright

        pw = await async_playwright().start()
        try:
            browser = await pw.chromium.launch(headless=True)
            context = await browser.new_context(
                viewport={"width": 1366, "height": 768},
                locale="en-US",
            )
            page = await context.new_page()

            search_data: dict = {}

            async def _on_response(response):
                url = response.url
                status = response.status
                ct = response.headers.get("content-type", "")
                if status == 200 and "json" in ct:
                    if any(kw in url.lower() for kw in [
                        "flight", "search", "avail", "fare", "schedule",
                    ]):
                        try:
                            data = await response.json()
                            if isinstance(data, (dict, list)):
                                search_data["api"] = data
                                logger.info("Samoa: captured API from %s", url[:80])
                        except Exception:
                            pass

            page.on("response", _on_response)

            try:
                logger.info("Samoa: loading TFLite for %s->%s", req.origin, req.destination)
                await page.goto(_SEARCH_URL, wait_until="domcontentloaded", timeout=20000)
                await asyncio.sleep(3)

                # Fill search form
                ok = await self._fill_form(page, req)
                if not ok:
                    logger.warning("Samoa: form fill failed")
                    return self._empty(req)

                # Click search
                clicked = await self._click_search(page)
                if not clicked:
                    logger.warning("Samoa: could not click search")
                    return self._empty(req)

                # Wait for results
                remaining = max(self.timeout - (time.monotonic() - t0), 8)
                await asyncio.sleep(min(remaining, 10))

                # Parse results
                offers = []
                if "api" in search_data:
                    offers = self._parse_api(search_data["api"], req)
                if not offers:
                    html = await page.content()
                    offers = self._parse_html(html, req)

                offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
                elapsed = time.monotonic() - t0
                logger.info("Samoa %s->%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

                h = hashlib.md5(
                    f"samoa{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
                ).hexdigest()[:12]
                return FlightSearchResponse(
                    search_id=f"fs_{h}",
                    origin=req.origin,
                    destination=req.destination,
                    currency="WST",
                    offers=offers,
                    total_results=len(offers),
                )

            except Exception as e:
                logger.error("Samoa CDP error: %s", e)
                return self._empty(req)
            finally:
                try:
                    await page.close()
                except Exception:
                    pass

        except Exception as e:
            logger.error("Samoa browser error: %s", e)
            return self._empty(req)
        finally:
            try:
                await pw.stop()
            except Exception:
                pass

    async def _fill_form(self, page, req: FlightSearchRequest) -> bool:
        """Fill the TFLite booking search form.

        Form fields (discovered via probe):
        - #oneway : checkbox for one-way
        - #from / #to : <select> with airport name values
        - #depart_date : text input for date
        - #submit : submit button
        """
        try:
            # Check one-way checkbox
            ow = page.locator("#oneway")
            if await ow.count() > 0:
                checked = await ow.is_checked()
                if not checked:
                    await ow.check(timeout=2000)
                logger.info("Samoa: one-way checked")
            await asyncio.sleep(0.3)

            # Origin — select by airport name
            origin_name = _IATA_TO_NAME.get(req.origin)
            if not origin_name:
                logger.warning("Samoa: unknown origin %s", req.origin)
                return False
            from_sel = page.locator("#from")
            await from_sel.select_option(value=origin_name, timeout=3000)
            logger.info("Samoa: origin = %s (%s)", origin_name, req.origin)
            await asyncio.sleep(0.3)

            # Destination — select by airport name
            dest_name = _IATA_TO_NAME.get(req.destination)
            if not dest_name:
                logger.warning("Samoa: unknown destination %s", req.destination)
                return False
            to_sel = page.locator("#to")
            await to_sel.select_option(value=dest_name, timeout=3000)
            logger.info("Samoa: destination = %s (%s)", dest_name, req.destination)
            await asyncio.sleep(0.3)

            # Date — text field #depart_date
            dep_date = datetime.strptime(str(req.date_from), "%Y-%m-%d")
            date_el = page.locator("#depart_date")
            if await date_el.count() > 0:
                # Try multiple date formats — TFLite typically uses dd/mm/yyyy
                for fmt in [
                    dep_date.strftime("%d/%m/%Y"),
                    dep_date.strftime("%Y-%m-%d"),
                    dep_date.strftime("%d %b %Y"),
                ]:
                    await date_el.click(timeout=2000)
                    await date_el.fill(fmt)
                    logger.info("Samoa: date = %s", fmt)
                    break
            else:
                logger.warning("Samoa: date field not found")

            return True

        except Exception as e:
            logger.error("Samoa form fill error: %s", e)
            return False

    async def _click_search(self, page) -> bool:
        btn = page.locator("#submit")
        if await btn.count() > 0:
            await btn.click(timeout=5000)
            logger.info("Samoa: clicked search")
            return True
        # Fallback
        for selector in ['button[type="submit"]', 'button:has-text("Search")']:
            try:
                el = page.locator(selector).first
                if await el.count() > 0:
                    await el.click(timeout=3000)
                    logger.info("Samoa: clicked search (fallback)")
                    return True
            except Exception:
                continue
        return False

    def _parse_api(self, data, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse any JSON API data captured."""
        offers = []
        dep_date = datetime.strptime(str(req.date_from), "%Y-%m-%d")

        items = data if isinstance(data, list) else data.get("flights", data.get("results", []))
        if not isinstance(items, list):
            return []

        for item in items:
            price = None
            for k in ["totalPrice", "price", "fare", "amount"]:
                v = item.get(k)
                if v:
                    try:
                        price = float(str(v).replace(",", ""))
                        break
                    except (ValueError, TypeError):
                        continue
            if not price or price <= 0:
                continue

            currency = item.get("currency", "WST")
            flight_no = item.get("flightNumber", item.get("flightNo", ""))

            dep_str = item.get("departureTime", item.get("departure", ""))
            arr_str = item.get("arrivalTime", item.get("arrival", ""))
            dep_dt = dep_date
            arr_dt = dep_date
            for dt_str, is_dep in [(dep_str, True), (arr_str, False)]:
                if dt_str:
                    p = self._parse_dt(dt_str, dep_date)
                    if p:
                        if is_dep:
                            dep_dt = p
                        else:
                            arr_dt = p
            if arr_dt < dep_dt:
                arr_dt += timedelta(days=1)
            dur = int((arr_dt - dep_dt).total_seconds()) if arr_dt > dep_dt else 0

            segment = FlightSegment(
                airline="PH", airline_name="Samoa Airways",
                flight_no=str(flight_no),
                origin=req.origin, destination=req.destination,
                departure=dep_dt, arrival=arr_dt,
                duration_seconds=dur, cabin_class="economy",
            )
            route = FlightRoute(segments=[segment], total_duration_seconds=dur, stopovers=0)
            fid = hashlib.md5(f"ph_{flight_no}_{price}_{req.date_from}".encode()).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"ph_{fid}",
                price=round(price, 2), currency=currency,
                price_formatted=f"{currency} {price:,.2f}",
                outbound=route, inbound=None,
                airlines=["Samoa Airways"], owner_airline="PH",
                booking_url=f"{_SEARCH_URL}?from={req.origin}&to={req.destination}&dd={req.date_from}&ADT={req.adults or 1}", is_locked=False,
                source="samoaairways_direct", source_tier="free",
            ))

        return offers

    def _parse_html(self, html: str, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse TFLite rendered HTML for flight results."""
        offers = []
        dep_date = datetime.strptime(str(req.date_from), "%Y-%m-%d")

        # TFLite typically renders fare tables
        cards = re.findall(
            r'<(?:div|tr|li)[^>]*class="[^"]*(?:flight|fare|avail|result|journey|itinerary)[^"]*"[^>]*>(.*?)</(?:div|tr|li)>',
            html, re.S | re.I,
        )

        for card in cards:
            # Price (WST, NZD, AUD, USD)
            price_m = re.search(
                r'(?:WST|NZD|AUD|USD|SAT|\$)\s*([\d,]+(?:\.\d{2})?)',
                card, re.I,
            )
            if not price_m:
                price_m = re.search(r'([\d,]+\.\d{2})', card)
            if not price_m:
                continue
            try:
                price = float(price_m.group(1).replace(",", ""))
            except (ValueError, TypeError):
                continue
            if price <= 0:
                continue

            curr_m = re.search(r'\b(WST|NZD|AUD|USD)\b', card, re.I)
            currency = curr_m.group(1).upper() if curr_m else "WST"

            times = re.findall(r'(\d{1,2}:\d{2})', card)
            dep_dt = dep_date
            arr_dt = dep_date
            if len(times) >= 2:
                try:
                    dep_dt = datetime.strptime(f"{dep_date.strftime('%Y-%m-%d')} {times[0]}", "%Y-%m-%d %H:%M")
                    arr_dt = datetime.strptime(f"{dep_date.strftime('%Y-%m-%d')} {times[1]}", "%Y-%m-%d %H:%M")
                    if arr_dt < dep_dt:
                        arr_dt += timedelta(days=1)
                except ValueError:
                    pass

            dur = int((arr_dt - dep_dt).total_seconds()) if arr_dt > dep_dt else 0
            fn_m = re.search(r'\b(PH\s*\d+|OL\s*\d+)\b', card)
            flight_no = fn_m.group(1).replace(" ", "") if fn_m else ""

            segment = FlightSegment(
                airline="PH", airline_name="Samoa Airways",
                flight_no=flight_no,
                origin=req.origin, destination=req.destination,
                departure=dep_dt, arrival=arr_dt,
                duration_seconds=dur, cabin_class="economy",
            )
            route = FlightRoute(segments=[segment], total_duration_seconds=dur, stopovers=0)
            fid = hashlib.md5(f"ph_{flight_no}_{price}_{req.date_from}".encode()).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"ph_{fid}",
                price=round(price, 2), currency=currency,
                price_formatted=f"{currency} {price:,.2f}",
                outbound=route, inbound=None,
                airlines=["Samoa Airways"], owner_airline="PH",
                booking_url=f"{_SEARCH_URL}?from={req.origin}&to={req.destination}&dd={req.date_from}&ADT={req.adults or 1}", is_locked=False,
                source="samoaairways_direct", source_tier="free",
            ))

        return offers

    @staticmethod
    def _parse_dt(dt_str: str, fallback: datetime) -> Optional[datetime]:
        for fmt in [
            "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M",
            "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
            "%d/%m/%Y %H:%M", "%H:%M",
        ]:
            try:
                if fmt == "%H:%M":
                    t = datetime.strptime(dt_str.strip(), fmt)
                    return fallback.replace(hour=t.hour, minute=t.minute, second=0)
                return datetime.strptime(dt_str.strip(), fmt)
            except ValueError:
                continue
        return None

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"samoa{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="WST",
            offers=[],
            total_results=0,
        )
