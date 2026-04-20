"""
Link Airways connector — Playwright CDP search via ASP.NET WebForms.

Link Airways (IATA: FC) is an Australian regional airline based in Dubbo, NSW.
Operates ~17 Australian airports including BNE, MEL, SYD (via connections),
CBR, HBA, LST, NTL and regional NSW/QLD cities.

Strategy (Playwright CDP — ASP.NET WebForms):
  1. Launch headless Playwright browser
  2. Navigate to StartOver.aspx, fill departure/arrival selects, date, passengers
  3. Click Search → ASP.NET postback redirects to Flight.aspx with results
  4. Parse HTML flight results → fare classes with prices
  5. Build FlightOffers for each flight × fare combination
"""

from __future__ import annotations

import asyncio
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
from .browser import acquire_browser_slot, auto_block_if_proxied, release_browser_slot, stealth_args

logger = logging.getLogger(__name__)

_BASE_URL = "https://search.linkairways.com/FC/StartOver.aspx"
_AIRLINE_NAME = "Link Airways"
_AIRLINE_IATA = "FC"
_CURRENCY = "AUD"

# All Link Airways airports (departure cities)
_AIRPORTS = {
    "ARM", "ZBL", "BNE", "BDB", "CBR", "CFS", "DBO",
    "HBA", "IVR", "LST", "MEL", "NAA", "NTL", "OAG",
    "TMW", "WOL",
}


class LinkAirwaysConnectorClient:
    """Link Airways (FC) — Playwright ASP.NET WebForms search."""

    def __init__(self, timeout: float = 30.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """
        Search Link Airways via Playwright form submission.

        1. Navigate to StartOver.aspx
        2. Select departure/arrival, set date, passengers
        3. Click Search → navigates to Flight.aspx with results
        4. Parse HTML fare cards → FlightOffers
        """
        t0 = time.monotonic()

        if req.origin not in _AIRPORTS or req.destination not in _AIRPORTS:
            logger.debug(
                "LinkAirways: %s or %s not in network", req.origin, req.destination
            )
            return self._empty(req)

        # Format date as DD/MM/YYYY
        try:
            if isinstance(req.date_from, str):
                dt = datetime.strptime(req.date_from, "%Y-%m-%d")
            else:
                dt = datetime.combine(req.date_from, datetime.min.time())
            date_str = dt.strftime("%d/%m/%Y")
            date_iso = dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            logger.warning("LinkAirways: bad date format %s", req.date_from)
            return self._empty(req)

        adults = req.adults if req.adults else 1
        children = req.children if req.children else 0
        infants = req.infants if req.infants else 0

        from .browser import acquire_browser_slot, release_browser_slot

        await acquire_browser_slot()
        try:
            offers = await self._search_with_browser(
                req.origin, req.destination, date_str, date_iso,
                adults, children, infants, req,
            )
        finally:
            release_browser_slot()

        # --- RT: second OW search for inbound ---
        if req.return_from and offers:
            ib_dt = (
                datetime.combine(req.return_from, datetime.min.time())
                if not isinstance(req.return_from, str)
                else datetime.strptime(req.return_from, "%Y-%m-%d")
            )
            ib_date_str = ib_dt.strftime("%d/%m/%Y")
            ib_date_iso = ib_dt.strftime("%Y-%m-%d")
            ib_req = req.model_copy(update={
                "origin": req.destination,
                "destination": req.origin,
                "date_from": req.return_from,
                "return_from": None,
            })
            await acquire_browser_slot()
            try:
                ib_offers = await self._search_with_browser(
                    req.destination, req.origin, ib_date_str, ib_date_iso,
                    adults, children, infants, ib_req,
                )
            finally:
                release_browser_slot()
            if ib_offers:
                offers = self._combine_rt(offers, ib_offers, req)

        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
        elapsed = time.monotonic() - t0
        logger.info(
            "LinkAirways %s→%s: %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        h = hashlib.md5(
            f"linkairways{req.origin}{req.destination}{date_iso}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=_CURRENCY,
            offers=offers,
            total_results=len(offers),
        )

    async def _search_with_browser(
        self,
        origin: str,
        destination: str,
        date_str: str,
        date_iso: str,
        adults: int,
        children: int,
        infants: int,
        req: FlightSearchRequest,
    ) -> list[FlightOffer]:
        """Run Playwright search and parse results."""
        from playwright.async_api import async_playwright
        from .browser import stealth_args

        pw = await async_playwright().start()
        browser = None
        try:
            browser = await pw.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    *stealth_args(),
                ],
            )
            page = await browser.new_page()
            await auto_block_if_proxied(page)

            # Step 1: Load the StartOver page
            logger.info("LinkAirways: loading StartOver.aspx for %s→%s", origin, destination)
            await page.goto(_BASE_URL, wait_until="domcontentloaded", timeout=15000)
            await asyncio.sleep(2)

            # Step 2: Fill form fields
            # Select departure city
            try:
                await page.select_option("#ucMiniSearch_depCity", origin)
            except Exception as e:
                logger.warning("LinkAirways: failed to select departure %s: %s", origin, e)
                return []
            await asyncio.sleep(2)  # Wait for arrival options to populate

            # Select arrival city
            try:
                await page.select_option("#ucMiniSearch_arrCity", destination)
            except Exception as e:
                logger.warning("LinkAirways: failed to select arrival %s: %s", destination, e)
                return []
            await asyncio.sleep(0.5)

            # Set one-way journey type
            try:
                await page.click("#ucMiniSearch_rdoJourneyType_0")
            except Exception as e:
                logger.debug("LinkAirways: one-way radio click: %s", e)
            await asyncio.sleep(0.3)

            # Set departure date and hidden fields via JS
            await page.evaluate(
                """(args) => {
                    const dpd = document.querySelector('#ucMiniSearch_dpd1');
                    if (dpd) {
                        dpd.value = args.date;
                        dpd.dispatchEvent(new Event('change', {bubbles: true}));
                    }
                    const hfDep = document.querySelector('#ucMiniSearch_hfdepCity');
                    if (hfDep) hfDep.value = args.dep;
                    const hfArr = document.querySelector('#ucMiniSearch_hfarrCity');
                    if (hfArr) hfArr.value = args.arr;
                    const hdnDep = document.querySelector('#ucMiniSearch_hdnDepDate');
                    if (hdnDep) hdnDep.value = args.date;
                }""",
                {"dep": origin, "arr": destination, "date": date_str},
            )
            await asyncio.sleep(0.3)

            # Set passenger counts
            if adults > 1:
                try:
                    await page.select_option("#ucMiniSearch_ddlAdult", str(adults))
                except Exception:
                    pass
            if children > 0:
                try:
                    await page.select_option("#ucMiniSearch_ddlChild", str(children))
                except Exception:
                    pass
            if infants > 0:
                try:
                    await page.select_option("#ucMiniSearch_ddlInfant", str(infants))
                except Exception:
                    pass

            # Step 3: Click Search and wait for navigation
            logger.info("LinkAirways: submitting search %s→%s %s", origin, destination, date_str)
            try:
                async with page.expect_navigation(
                    timeout=int(self.timeout * 1000),
                    wait_until="domcontentloaded",
                ):
                    await page.click("#btnminiSearch")
            except Exception as e:
                logger.warning("LinkAirways: search navigation failed: %s", e)
                return []

            await asyncio.sleep(2)

            # Check if we got results
            current_url = page.url
            if "Flight.aspx" not in current_url:
                logger.warning("LinkAirways: didn't reach Flight.aspx, URL: %s", current_url)
                return []

            # Step 4: Parse results
            html = await page.content()
            return self._parse_results(html, req, date_iso)

        except Exception as e:
            logger.error("LinkAirways browser error: %s", e)
            return []
        finally:
            if browser:
                try:
                    await browser.close()
                except Exception:
                    pass
            try:
                await pw.stop()
            except Exception:
                pass

    def _parse_results(self, html: str, req: FlightSearchRequest, date_iso: str) -> list[FlightOffer]:
        """Parse Link Airways Flight.aspx HTML for fare offers.

        HTML structure:
          - divOBFlightResults contains flight blocks (.list-item)
          - Each flight: dep/arr times in <h4>, flight no in <p>, duration in span
          - Fare prices in <label class="ffare"> with <small>AUD</small> NNN.NN
          - Booking codes in <label class="BkClsCode">
          - Fare names (Deal/Standard/Freedom/Flexible) in header labels
        """
        offers: list[FlightOffer] = []
        booking_url_base = "https://search.linkairways.com/FC/Flight.aspx"

        # Only parse within divOBFlightResults to avoid calendar prices
        results_marker = 'id="divOBFlightResults"'
        results_idx = html.find(results_marker)
        if results_idx < 0:
            logger.info("LinkAirways: divOBFlightResults not found")
            return []
        results_html = html[results_idx:]

        # Extract flight blocks — each starts with class containing "list-item"
        # and contains FC XXXX flight number
        flight_blocks_raw = re.findall(
            r'FC\s*(\d{3,5})', results_html
        )
        flight_numbers = list(dict.fromkeys(flight_blocks_raw))

        # Extract fare prices from <label class="ffare"> — only real ones with AUD + number
        ffare_matches = re.findall(
            r'class="ffare"[^>]*>[\s\S]*?</label>',
            results_html,
        )
        fare_prices: list[float] = []
        for m in ffare_matches:
            # Strip HTML tags, check for real AUD price (not JS template)
            text = re.sub(r'<[^>]+>', '', m).strip()
            price_match = re.search(r'AUD\s+([\d,]+(?:\.\d{2})?)', text)
            if price_match:
                try:
                    fare_prices.append(float(price_match.group(1).replace(",", "")))
                except ValueError:
                    pass

        # Extract booking class codes from <label class="BkClsCode">
        bkclass_matches = re.findall(
            r'class="BkClsCode"[^>]*>[\s\S]*?</label>',
            results_html,
        )
        booking_classes: list[str] = []
        for m in bkclass_matches:
            text = re.sub(r'<[^>]+>', '', m).strip()
            bc_match = re.search(r'([A-Z]),\s+([A-Z]{4,7})', text)
            if bc_match:
                booking_classes.append(bc_match.group(2))

        # Extract times from <h4> tags (dep/arr) within flight results
        h4_times = re.findall(r'<h4>(\d{2}:\d{2})</h4>', results_html)

        # Extract durations
        durations = re.findall(r'(\d+)h\s+(\d+)mins?', results_html)

        # Fare class names — detect from page header or use defaults
        fare_names = ["Deal", "Standard", "Freedom", "Flexible"]
        # Try to detect actual fare column names from header
        header_fares = re.findall(
            r'data-original-title="">\s*<label>(\w+)</label>\s*<ul class="fareicons',
            html,
        )
        if header_fares:
            fare_names = header_fares

        num_fares = len(fare_names)

        logger.info(
            "LinkAirways parse: %d flights, %d fare prices, %d times, %d durations, %d fare classes",
            len(flight_numbers), len(fare_prices), len(h4_times), len(durations), num_fares,
        )

        if not flight_numbers:
            logger.info("LinkAirways: no flights found in HTML")
            return []

        for i, fn_digits in enumerate(flight_numbers):
            # Get dep/arr times — each flight has 4 h4 times (2 main + 2 popover)
            t_start = i * 4
            if t_start + 1 >= len(h4_times):
                continue
            dep_time = h4_times[t_start]
            arr_time = h4_times[t_start + 1]

            # Get duration (2 per flight: main + popover)
            dur_hours, dur_mins = 0, 0
            dur_idx = i * 2
            if dur_idx < len(durations):
                dur_hours = int(durations[dur_idx][0])
                dur_mins = int(durations[dur_idx][1])
            total_seconds = dur_hours * 3600 + dur_mins * 60

            # Build datetimes
            try:
                dep_dt = datetime.strptime(f"{date_iso} {dep_time}", "%Y-%m-%d %H:%M")
                arr_dt = datetime.strptime(f"{date_iso} {arr_time}", "%Y-%m-%d %H:%M")
                if arr_dt <= dep_dt:
                    from datetime import timedelta
                    arr_dt += timedelta(days=1)
            except ValueError:
                dep_dt = datetime.strptime(date_iso, "%Y-%m-%d")
                arr_dt = dep_dt

            segment = FlightSegment(
                airline=_AIRLINE_NAME,
                flight_no=f"FC{fn_digits}",
                origin=req.origin,
                destination=req.destination,
                departure=dep_dt,
                arrival=arr_dt,
                duration_seconds=total_seconds or None,
            )

            route = FlightRoute(
                segments=[segment],
                total_duration_seconds=total_seconds,
                stopovers=0,
            )

            # Get fares for this flight (num_fares per flight)
            p_start = i * num_fares
            for j in range(num_fares):
                p_idx = p_start + j
                if p_idx >= len(fare_prices):
                    break
                price = fare_prices[p_idx]
                fare_name = fare_names[j] if j < len(fare_names) else f"Fare{j}"
                bc_idx = p_start + j
                booking_class = booking_classes[bc_idx] if bc_idx < len(booking_classes) else ""

                offer_id = hashlib.md5(
                    f"fc{fn_digits}{date_iso}{fare_name}{price}".encode()
                ).hexdigest()[:12]

                offers.append(FlightOffer(
                    id=f"off_{offer_id}",
                    price=price,
                    currency=_CURRENCY,
                    airlines=[_AIRLINE_NAME],
                    owner_airline=_AIRLINE_NAME,
                    outbound=route,
                    inbound=None,
                    booking_url=f"{booking_url_base}?depCity={req.origin}&arrCity={req.destination}&depDate={date_iso}",
                    conditions={
                        "fare_type": fare_name,
                        "fare_basis": booking_class,
                        "baggage": "22kg" if fare_name in ("Deal", "Standard") else "30kg",
                    },
                    source="linkairways_direct",
                ))

        return offers

    def _combine_rt(
        self,
        ob_offers: list[FlightOffer],
        ib_offers: list[FlightOffer],
        req: FlightSearchRequest,
    ) -> list[FlightOffer]:
        """Cross-multiply OB x IB one-way offers into RT combos."""
        ob_sorted = sorted(ob_offers, key=lambda o: o.price)[:15]
        ib_sorted = sorted(ib_offers, key=lambda o: o.price)[:10]
        date_iso = req.date_from.strftime("%Y-%m-%d") if hasattr(req.date_from, "strftime") else str(req.date_from)
        ret_iso = req.return_from.strftime("%Y-%m-%d") if hasattr(req.return_from, "strftime") else str(req.return_from)
        bk_url = (
            f"https://search.linkairways.com/FC/Flight.aspx"
            f"?depCity={req.origin}&arrCity={req.destination}"
            f"&depDate={date_iso}&retDate={ret_iso}"
        )
        combined: list[FlightOffer] = []
        for ob in ob_sorted:
            for ib in ib_sorted:
                total = round(ob.price + ib.price, 2)
                key = f"{ob.id}_{ib.id}"
                cid = f"fc_rt_{hashlib.md5(key.encode()).hexdigest()[:12]}"
                combined.append(FlightOffer(
                    id=cid,
                    price=total,
                    currency=_CURRENCY,
                    airlines=[_AIRLINE_NAME],
                    owner_airline=_AIRLINE_NAME,
                    outbound=ob.outbound,
                    inbound=ib.outbound,
                    booking_url=bk_url,
                    source="linkairways_direct",
                ))
        combined.sort(key=lambda o: o.price)
        return combined

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"linkairways{req.origin}{req.destination}{str(req.date_from)}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=_CURRENCY,
            offers=[],
            total_results=0,
        )
