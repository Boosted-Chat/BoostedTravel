"""
Asiana Airlines (OZ) — CDP Chrome connector — form fill + API intercept.

Asiana Airlines's website at flyasiana.com uses a search widget with autocomplete
airport fields and calendar date picker. Direct API calls are blocked;
headed CDP Chrome with form fill + API interception is required.

Strategy (CDP Chrome + API interception):
1. Launch headed Chrome via CDP (off-screen, stealth).
2. Navigate to flyasiana.com → SPA loads with search widget.
3. Accept cookies → set one-way → fill origin/dest → select date → search.
4. Intercept the search API response (flight availability JSON).
5. If API not captured, fall back to DOM scraping on results page.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import time
from datetime import datetime, date, timedelta
from typing import Optional

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import find_chrome, stealth_popen_kwargs, _launched_procs, proxy_chrome_args, auto_block_if_proxied

logger = logging.getLogger(__name__)

_DEBUG_PORT = 9495
_USER_DATA_DIR = os.path.join(
    os.environ.get("TEMP", os.environ.get("TMPDIR", "/tmp")), ".asiana_chrome_data"
)

_browser = None
_context = None
_pw_instance = None
_chrome_proc = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_context():
    global _browser, _context, _pw_instance, _chrome_proc
    lock = _get_lock()
    async with lock:
        if _browser:
            try:
                if _browser.is_connected():
                    if _context:
                        try:
                            _ = _context.pages
                            return _context
                        except Exception:
                            pass
                    contexts = _browser.contexts
                    if contexts:
                        _context = contexts[0]
                        return _context
            except Exception:
                pass

        from playwright.async_api import async_playwright

        pw = None
        try:
            pw = await async_playwright().start()
            _browser = await pw.chromium.connect_over_cdp(
                f"http://127.0.0.1:{_DEBUG_PORT}"
            )
            _pw_instance = pw
            logger.info("Asiana: connected to existing Chrome on port %d", _DEBUG_PORT)
        except Exception:
            if pw:
                try:
                    await pw.stop()
                except Exception:
                    pass
            chrome = find_chrome()
            os.makedirs(_USER_DATA_DIR, exist_ok=True)
            args = [
                chrome,
                f"--remote-debugging-port={_DEBUG_PORT}",
                f"--user-data-dir={_USER_DATA_DIR}",
                "--no-first-run",
                *proxy_chrome_args(),
                "--no-default-browser-check",
                "--disable-blink-features=AutomationControlled",
                "--window-position=-2400,-2400",
                "--window-size=1400,900",
                "about:blank",
            ]
            _chrome_proc = subprocess.Popen(args, **stealth_popen_kwargs())
            _launched_procs.append(_chrome_proc)
            await asyncio.sleep(2.0)
            pw = await async_playwright().start()
            _pw_instance = pw
            _browser = await pw.chromium.connect_over_cdp(
                f"http://127.0.0.1:{_DEBUG_PORT}"
            )
            logger.info("Asiana: Chrome launched on CDP port %d", _DEBUG_PORT)

        contexts = _browser.contexts
        _context = contexts[0] if contexts else await _browser.new_context()
        return _context


async def _reset_profile():
    global _browser, _context, _pw_instance, _chrome_proc
    try:
        if _browser:
            await _browser.close()
    except Exception:
        pass
    try:
        if _pw_instance:
            await _pw_instance.stop()
    except Exception:
        pass
    if _chrome_proc:
        try:
            _chrome_proc.terminate()
        except Exception:
            pass
    _browser = _context = _pw_instance = _chrome_proc = None
    if os.path.isdir(_USER_DATA_DIR):
        try:
            shutil.rmtree(_USER_DATA_DIR)
        except Exception:
            pass


async def _dismiss_overlays(page) -> None:
    try:
        # Asiana has a cookie consent with an "OK" button
        await page.evaluate("""() => {
            // Click cookie OK button
            const btns = document.querySelectorAll('button, a');
            for (const b of btns) {
                const t = b.textContent.trim();
                if (t === 'OK' || t === 'Accept' || t === 'Agree') {
                    if (b.offsetHeight > 0 && b.offsetHeight < 100) { b.click(); break; }
                }
            }
            // Remove overlay elements
            document.querySelectorAll(
                '#onetrust-consent-sdk, .cookie-banner, [class*="cookie"], [class*="consent"]'
            ).forEach(el => el.remove());
        }""")
    except Exception:
        pass


class AsianaConnectorClient:
    """Asiana Airlines (OZ) CDP Chrome connector."""

    IATA = "OZ"
    AIRLINE_NAME = "Asiana Airlines"
    SOURCE = "asiana_direct"
    HOMEPAGE = "https://flyasiana.com/C/US/EN/index"
    DEFAULT_CURRENCY = "KRW"

    def __init__(self, timeout: float = 55.0):
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
        context = await _get_context()
        page = await context.new_page()
        await auto_block_if_proxied(page)
        page.on("dialog", lambda d: asyncio.ensure_future(d.accept()))

        try:
            logger.info("Asiana: loading homepage for %s→%s", req.origin, req.destination)
            await page.goto(self.HOMEPAGE, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(3.0)
            await _dismiss_overlays(page)
            await asyncio.sleep(1.0)

            # Select One-Way trip type
            await page.evaluate("""() => {
                const jq = (typeof $ !== 'undefined') ? $ : jQuery;
                jq('.tab_triptype li').each(function() {
                    if (jq(this).text().trim().toLowerCase().includes('one-way'))
                        jq(this).find('a').trigger('click');
                });
                if (typeof bookConditionJSON !== 'undefined') bookConditionJSON.tripType = 'OW';
            }""")
            await asyncio.sleep(1.0)

            # Fill departure via jQuery UI autocomplete keyboard interaction.
            # Clicking autocomplete items via JS doesn't populate the paired
            # hidden fields (#departureAirportR, #departureAreaR, etc.).
            # ArrowDown + Enter triggers the internal select handler properly.
            dep_field = page.locator("#txtDepartureAirportR")
            await dep_field.click(timeout=5000)
            await dep_field.fill("")
            await dep_field.type(req.origin, delay=100)
            await asyncio.sleep(2.0)
            await page.keyboard.press("ArrowDown")
            await asyncio.sleep(0.3)
            await page.keyboard.press("Enter")
            await asyncio.sleep(1.0)
            logger.info("Asiana: departure → %s", req.origin)

            # Fill arrival
            arr_field = page.locator("#txtArrivalAirportR")
            await arr_field.click(timeout=5000)
            await arr_field.fill("")
            await arr_field.type(req.destination, delay=100)
            await asyncio.sleep(2.0)
            await page.keyboard.press("ArrowDown")
            await asyncio.sleep(0.3)
            await page.keyboard.press("Enter")
            await asyncio.sleep(1.0)
            logger.info("Asiana: arrival → %s", req.destination)

            # Set date — write directly into the hidden departureDateR field.
            # The datepicker UI uses non-standard navigation; setting the
            # hidden field is reliable and what registTravelV() reads.
            ok = await self._fill_date(page, req)
            if not ok:
                return self._empty(req)

            # Submit via the JS function (runs validation + NetFunnel queue)
            await page.evaluate("() => registTravelV()")
            logger.info("Asiana: registTravelV() called")

            # Wait for navigation to the results page
            try:
                await page.wait_for_url("**/Revenue*", timeout=20000)
            except Exception:
                pass
            # Wait for AJAX flight data to load
            await asyncio.sleep(8.0)

            offers = await self._scrape_dom(page, req)
            offers.sort(key=lambda o: o.price)
            elapsed = time.monotonic() - t0
            logger.info("Asiana %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

            search_hash = hashlib.md5(
                f"asiana{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
            ).hexdigest()[:12]
            currency = offers[0].currency if offers else self.DEFAULT_CURRENCY
            return FlightSearchResponse(
                search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
                currency=currency, offers=offers, total_results=len(offers),
            )
        except Exception as e:
            logger.error("Asiana error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        """Set the departure date by writing the hidden #departureDate1 field.

        The datepicker UI uses non-standard month navigation that is fragile to
        automate.  registTravelV() reads ``$('[name=departureDateR]').val()``
        which maps to the hidden ``#departureDate1`` input.  Writing it directly
        is reliable.
        """
        try:
            dt = req.date_from if isinstance(req.date_from, (datetime, date)) else datetime.strptime(str(req.date_from), "%Y-%m-%d")
        except (ValueError, TypeError):
            return False
        date_str = dt.strftime("%Y%m%d")  # "20260715"
        display_str = dt.strftime("%y.%m.%d")  # "26.07.15"
        await page.evaluate("""(args) => {
            const [dateStr, displayStr] = args;
            const jq = (typeof $ !== 'undefined') ? $ : jQuery;
            jq('#departureDate1').val(dateStr);
            jq('#sCalendarR').val(displayStr);
        }""", [date_str, display_str])
        logger.info("Asiana: date set %s", dt.strftime("%Y-%m-%d"))
        return True

    async def _scrape_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Extract flight offers from the Asiana results table.

        The results page at ``RevenueInternationalFareDrivenFlightsSelect.do``
        renders an ``<table class="table_list airline_ticketing">`` with one
        ``<tr>`` per flight.  Each row contains departure/arrival times, flight
        number, aircraft type, and fare class prices with seat counts.  The
        cheapest economy fare per flight becomes an offer.
        """
        flights = await page.evaluate(r"""(params) => {
            const [origin, destination] = params;
            const results = [];
            // Primary: parse the fare/flight selection table
            const rows = document.querySelectorAll('table.airline_ticketing tbody tr, table.table_list.airline_ticketing tbody tr');
            for (const row of rows) {
                if (row.offsetHeight === 0) continue;
                const text = row.innerText || '';
                if (text.length < 20) continue;
                // Extract times: "08:25" and "10:50"
                const times = text.match(/\b(\d{1,2}:\d{2})\b/g) || [];
                if (times.length < 2) continue;
                // Extract flight number: OZ102
                const fnMatch = text.match(/\b(OZ\s*\d{2,4})\b/i);
                if (!fnMatch) continue;
                // Extract cheapest price from <td> cells
                const cells = row.querySelectorAll('td');
                let cheapest = Infinity;
                let currency = 'KRW';
                let seats = 0;
                for (const cell of cells) {
                    const ct = cell.textContent || '';
                    // Match "KRW 263,900" or "USD 450" or "EUR 305,900"
                    const pm = ct.match(/(KRW|USD|EUR)\s*([\d,]+)/);
                    if (pm) {
                        const p = parseFloat(pm[2].replace(/,/g, ''));
                        if (p > 0 && p < cheapest && !ct.toLowerCase().includes('sold')) {
                            cheapest = p;
                            currency = pm[1];
                            const sm = ct.match(/(\d+)\s*Seat/i);
                            seats = sm ? parseInt(sm[1], 10) : 0;
                        }
                    }
                }
                if (cheapest === Infinity) continue;
                // Extract duration text: "2hr25min"
                const durMatch = text.match(/(\d+)\s*hr\s*(\d+)\s*min/i);
                let durSec = 0;
                if (durMatch) durSec = parseInt(durMatch[1], 10) * 3600 + parseInt(durMatch[2], 10) * 60;
                // Check for stops
                const isNonstop = /non-?stop/i.test(text);
                results.push({
                    depTime: times[0], arrTime: times[1],
                    flightNo: fnMatch[1].replace(/\s/g, ''),
                    price: cheapest, currency: currency,
                    duration: durSec, nonstop: isNonstop, seats: seats,
                });
            }
            return results;
        }""", [req.origin, req.destination])

        offers = []
        for f in (flights or []):
            offer = self._build_dom_offer(f, req)
            if offer:
                offers.append(offer)
        return offers

    def _build_dom_offer(self, f: dict, req: FlightSearchRequest) -> Optional[FlightOffer]:
        price = f.get("price", 0)
        if price <= 0:
            return None
        try:
            dt = req.date_from if isinstance(req.date_from, (datetime, date)) else datetime.strptime(str(req.date_from), "%Y-%m-%d")
            dep_date = dt.date() if isinstance(dt, datetime) else dt if isinstance(dt, date) else date.today()
        except (ValueError, TypeError):
            dep_date = date.today()

        dep_time = f.get("depTime", "00:00")
        arr_time = f.get("arrTime", "00:00")
        try:
            h, m = dep_time.split(":")
            dep_dt = datetime(dep_date.year, dep_date.month, dep_date.day, int(h), int(m))
        except (ValueError, IndexError):
            dep_dt = datetime(dep_date.year, dep_date.month, dep_date.day)
        try:
            h, m = arr_time.split(":")
            arr_dt = datetime(dep_date.year, dep_date.month, dep_date.day, int(h), int(m))
            if arr_dt <= dep_dt:
                arr_dt += timedelta(days=1)
        except (ValueError, IndexError):
            arr_dt = dep_dt

        flight_no = f.get("flightNo", self.IATA)
        currency = f.get("currency", self.DEFAULT_CURRENCY)
        dur_sec = f.get("duration", 0) or int((arr_dt - dep_dt).total_seconds())
        stopovers = 0 if f.get("nonstop", True) else 1
        offer_id = hashlib.md5(f"{self.IATA.lower()}_{req.origin}_{req.destination}_{dep_date}_{flight_no}_{price}".encode()).hexdigest()[:12]

        _oz_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
        segment = FlightSegment(
            airline=self.IATA, airline_name=self.AIRLINE_NAME, flight_no=flight_no,
            origin=req.origin, destination=req.destination, departure=dep_dt, arrival=arr_dt, cabin_class=_oz_cabin,
        )
        route = FlightRoute(segments=[segment], total_duration_seconds=dur_sec, stopovers=stopovers)
        return FlightOffer(
            id=f"{self.IATA.lower()}_{offer_id}", price=round(price, 2), currency=currency,
            price_formatted=f"{currency} {price:,.0f}", outbound=route, inbound=None,
            airlines=[self.AIRLINE_NAME], owner_airline=self.IATA,
            booking_url=self._booking_url(req), is_locked=False, source=self.SOURCE, source_tier="free",
        )

    def _booking_url(self, req: FlightSearchRequest) -> str:
        try:
            date_str = req.date_from.strftime("%Y-%m-%d") if hasattr(req.date_from, "strftime") else str(req.date_from)
        except Exception:
            date_str = ""
        return f"https://flyasiana.com/C/US/EN/index?from={req.origin}&to={req.destination}&date={date_str}"

    @staticmethod
    def _combine_rt(ob: list, ib: list, req) -> list:
        combos = []
        for o in sorted(ob, key=lambda x: x.price)[:15]:
            for i in sorted(ib, key=lambda x: x.price)[:10]:
                combos.append(FlightOffer(
                    id=f"oz_rt_{o.id}_{i.id}",
                    price=round(o.price + i.price, 2),
                    currency=o.currency,
                    outbound=o.outbound,
                    inbound=i.outbound,
                    owner_airline=o.owner_airline,
                    airlines=list(set(o.airlines + i.airlines)),
                    source=o.source,
                    booking_url=o.booking_url,
                    conditions=o.conditions,
                ))
        combos.sort(key=lambda x: x.price)
        return combos[:20]

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(f"asiana{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
            currency=self.DEFAULT_CURRENCY, offers=[], total_results=0,
        )


# ── Module-level interface (required by connector loader) ────────────────────

_client = AsianaConnectorClient()


async def search(request: FlightSearchRequest) -> FlightSearchResponse:
    return await _client.search_flights(request)
