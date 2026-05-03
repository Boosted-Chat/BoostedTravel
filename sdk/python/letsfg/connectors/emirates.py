"""
Emirates (EK) CDP Chrome connector — form fill + DOM results scraping.

Emirates' booking flow at /english/book/ is a Next.js app behind Akamai WAF.
Direct API calls are blocked; the ONLY reliable path is form-triggered browsing.

Strategy (CDP Chrome + DOM scraping):
1. Launch REAL Chrome (--remote-debugging-port, --user-data-dir).
2. Connect via Playwright CDP.
3. Navigate to /english/book/ → remove disruption modal → dismiss cookies.
4. Click "One way" → fill departure/arrival via auto-suggest typeahead
   → select date via DayPicker calendar widget → click "Search flights".
5. Page navigates to /booking/search-results/?searchRequest=<base64>.
6. Wait for DOM with flight cards → scrape flight details (flight no,
   dep/arr times, duration, stops, airports, price, aircraft type).
7. Also capture /service/search-results/flexi-fares API as pricing fallback.

Discovered Mar 2026:
  - /english/book/ inputs: auto-suggest (typed with delay), DayPicker calendar.
  - Results page: flight cards with EK flight numbers, times, prices in AED.
  - API: /service/search-results/flexi-fares → calendar pricing for ±3 days.
  - API: /service/search-results/simplified-fare-rules → fare brand details.
  - Akamai WAF blocks headless browsers; CDP headed Chrome works.
"""

from __future__ import annotations

import asyncio
import json
import hashlib
import logging
import os
import re
import shutil
import subprocess
import time
from urllib.parse import parse_qs, quote, urlparse
from datetime import datetime, date
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

_ancillary_cache: dict[str, tuple[float, dict]] = {}
_ANCILLARY_CACHE_TTL = 1800  # 30 min
_DEBUG_PORT = 9457
_USER_DATA_DIR = os.path.join(
    os.environ.get("TEMP", os.environ.get("TMPDIR", "/tmp")), ".emirates_chrome_data"
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
    """Get or create a persistent browser context (headed — Akamai blocks headless)."""
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

        # Try existing Chrome
        pw = None
        try:
            pw = await async_playwright().start()
            _browser = await pw.chromium.connect_over_cdp(
                f"http://127.0.0.1:{_DEBUG_PORT}"
            )
            _pw_instance = pw
            logger.info("Emirates: connected to existing Chrome on port %d", _DEBUG_PORT)
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
                "--disable-http2",
                "--window-position=100,100",
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
            logger.info(
                "Emirates: Chrome launched on CDP port %d (pid %d)",
                _DEBUG_PORT, _chrome_proc.pid,
            )

        contexts = _browser.contexts
        _context = contexts[0] if contexts else await _browser.new_context()
        return _context


async def _dismiss_overlays(page) -> None:
    """Remove disruption modal, OneTrust cookie banner, and unstick header."""
    await page.evaluate("""() => {
        document.querySelectorAll(
            '#modal-wrapper, .disruption-modal-wrapper'
        ).forEach(el => el.remove());
        // Unstick header so it doesn't block form field clicks
        const hdr = document.querySelector('.header-popup__wrapper--sticky, header[data-auto="header"]');
        if (hdr) hdr.style.position = 'relative';
        // Remove header second-level menu that intercepts pointer events
        document.querySelectorAll('.second-level-menu').forEach(el => {
            el.style.pointerEvents = 'none';
        });
    }""")
    try:
        btn = page.locator("#onetrust-accept-btn-handler")
        if await btn.count() > 0:
            await btn.first.click(timeout=3000)
            await asyncio.sleep(0.5)
            return
    except Exception:
        pass
    # Force-remove cookie SDK
    try:
        await page.evaluate("""() => {
            document.querySelectorAll(
                '#onetrust-consent-sdk, .onetrust-pc-dark-filter, #onetrust-banner-sdk'
            ).forEach(el => el.remove());
        }""")
    except Exception:
        pass


async def _reset_profile():
    """Wipe Chrome profile when Akamai flags the session."""
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
    _browser = None
    _context = None
    _pw_instance = None
    _chrome_proc = None
    if os.path.isdir(_USER_DATA_DIR):
        try:
            shutil.rmtree(_USER_DATA_DIR)
            logger.info("Emirates: deleted stale Chrome profile")
        except Exception:
            pass


def _extract_api_flights(payload: dict, out: list[dict]) -> None:
    """Extract structured flight data from Emirates search-results API JSON.

    Emirates API responses vary in shape, but flight data typically appears as
    lists of 'boundOptions', 'journeys', 'flights', or 'segments' with nested
    departure/arrival times, flight numbers, and pricing.
    """
    if not isinstance(payload, dict):
        return

    # Priority: Parse Emirates bounds structure (has real segment data with times)
    if isinstance(payload.get("bounds"), list) and payload["bounds"]:
        _parse_bounds_payload(payload, out)
        return

    # If we already have flights from a successful bounds parse, don't pollute
    # with data from other API responses (e.g. currency-conversion results that
    # contain ~113 junk entries at ~EUR50 each).
    if out and any(f.get("bound_type") for f in out):
        return

    # Detect currency from top-level
    currency = ""
    for ckey in ("currency", "saleCurrency", "pricedCurrency", "currencyCode"):
        v = payload.get(ckey)
        if isinstance(v, str) and len(v) == 3:
            currency = v.upper()
            break
    if not currency:
        c_obj = payload.get("currency") or payload.get("priceSummary", {}).get("currency")
        if isinstance(c_obj, dict):
            for ck in ("sale", "code", "currencyCode"):
                v = c_obj.get(ck)
                if isinstance(v, dict):
                    v = v.get("code")
                if isinstance(v, str) and len(v) == 3:
                    currency = v.upper()
                    break

    # Look for flight option lists under common keys
    option_lists = []
    for key in ("boundOptions", "journeys", "flights", "options", "itineraries",
                "results", "searchResults", "flightOptions", "offers"):
        val = payload.get(key)
        if isinstance(val, list) and val:
            option_lists.append(val)
    # Also check nested pageProps or data
    for wrapper_key in ("data", "pageProps", "props", "searchData", "result"):
        wrapper = payload.get(wrapper_key)
        if isinstance(wrapper, dict):
            for key in ("boundOptions", "journeys", "flights", "options", "itineraries",
                        "results", "searchResults", "flightOptions", "offers"):
                val = wrapper.get(key)
                if isinstance(val, list) and val:
                    option_lists.append(val)

    for opt_list in option_lists:
        for opt in opt_list:
            if not isinstance(opt, dict):
                continue
            flight = _parse_api_option(opt, currency)
            if flight and flight.get("price", 0) > 0:
                # Skip items with no flight data (e.g. currency conversion results)
                if not flight.get("origin") and not flight.get("destination"):
                    continue
                out.append(flight)

    if out:
        logger.info("Emirates: extracted %d flights from API interception", len(out))


def _parse_bounds_payload(payload: dict, out: list[dict]) -> None:
    """Parse Emirates bounds-based search results.

    Emirates returns separate OUTBOUND/INBOUND bounds, each with options
    containing airSegments (flight details) and cabins (fare classes).
    The top-level lowestFare holds the total RT price.
    """
    bounds = payload.get("bounds")
    if not isinstance(bounds, list):
        return

    # Extract currency from currency.sale.code or currency.priced.code
    currency = ""
    cur_obj = payload.get("currency")
    if isinstance(cur_obj, dict):
        for ck in ("sale", "priced"):
            sub = cur_obj.get(ck)
            if isinstance(sub, dict):
                c = (sub.get("code") or "").upper()
                if len(c) == 3:
                    currency = c
                    break

    # Extract lowest RT fare from lowestFare.total[].amount
    price = 0.0
    lf = payload.get("lowestFare")
    if isinstance(lf, dict):
        total = lf.get("total")
        if isinstance(total, list):
            for t in total:
                if isinstance(t, dict):
                    amt = t.get("amount")
                    if isinstance(amt, (int, float)) and amt > 0:
                        price = float(amt)
                        break

    for bound in bounds:
        if not isinstance(bound, dict):
            continue

        bound_type = (bound.get("type") or "").upper()
        bound_origin = bound.get("origin", "")
        bound_dest = bound.get("destination", "")

        options = bound.get("options")
        if not isinstance(options, list):
            continue

        for opt in options:
            if not isinstance(opt, dict):
                continue

            segs = opt.get("airSegments")
            if not isinstance(segs, list) or not segs:
                continue

            first_seg = segs[0] if isinstance(segs[0], dict) else {}
            last_seg = segs[-1] if isinstance(segs[-1], dict) else {}

            # Times from segment datetimes
            dep_time = "00:00"
            arr_time = "00:00"
            dep_dt_str = first_seg.get("departureDateTime", "")
            arr_dt_str = last_seg.get("arrivalDateTime", "")
            if isinstance(dep_dt_str, str) and dep_dt_str:
                tm = re.search(r"(\d{2}):(\d{2})", dep_dt_str)
                if tm:
                    dep_time = f"{tm.group(1)}:{tm.group(2)}"
            if isinstance(arr_dt_str, str) and arr_dt_str:
                tm = re.search(r"(\d{2}):(\d{2})", arr_dt_str)
                if tm:
                    arr_time = f"{tm.group(1)}:{tm.group(2)}"

            # Flight number
            carrier = first_seg.get("carrierCode") or first_seg.get("marketingCarrierCode") or "EK"
            flt_num = str(first_seg.get("flightNumber", ""))
            flight_no = f"{carrier}{flt_num}" if flt_num else carrier

            # Duration from ondDuration (e.g. "8H45M") or segment duration
            dur_str = opt.get("ondDuration", "")
            duration = 0
            if isinstance(dur_str, str) and dur_str:
                hm = re.search(r"(\d+)H(?:(\d+)M)?", dur_str)
                if hm:
                    duration = int(hm.group(1)) * 60 + int(hm.group(2) or 0)
            if not duration:
                seg_dur = first_seg.get("duration", "")
                if isinstance(seg_dur, str):
                    hm = re.search(r"(\d+)H(?:(\d+)M)?", seg_dur)
                    if hm:
                        duration = int(hm.group(1)) * 60 + int(hm.group(2) or 0)

            stops = opt.get("numberOfConnections")
            if not isinstance(stops, int):
                stops = max(0, len(segs) - 1)

            aircraft = first_seg.get("aircraftType", "")
            origin = first_seg.get("departure") or bound_origin
            destination = last_seg.get("arrival") or bound_dest
            date_str = dep_dt_str[:10] if isinstance(dep_dt_str, str) and len(dep_dt_str) >= 10 else ""

            # Try to extract inbound flight info from combinableWith patterns
            # e.g. "JFK_MXP_EK206_1669567666_Y_FLEX" → inbound JFK→MXP EK206
            inbound_info = None
            cabs = opt.get("cabins")
            if isinstance(cabs, list):
                for cab in cabs:
                    if inbound_info:
                        break
                    if not isinstance(cab, dict):
                        continue
                    brands = cab.get("brandInformation")
                    if not isinstance(brands, list):
                        continue
                    for brand in brands:
                        if not isinstance(brand, dict):
                            continue
                        combinable = brand.get("combinableWith")
                        if isinstance(combinable, list) and combinable:
                            for cid in combinable:
                                if isinstance(cid, str) and "_" in cid:
                                    parts = cid.split("_")
                                    if len(parts) >= 3:
                                        inbound_info = (parts[0], parts[1], parts[2])
                                        break
                            break

            flight = {
                "flightNo": flight_no,
                "depTime": dep_time,
                "arrTime": arr_time,
                "dateStr": date_str,
                "duration": duration,
                "durationText": dur_str if isinstance(dur_str, str) else "",
                "nonstop": stops == 0,
                "stops": stops,
                "origin": origin,
                "originCity": "",
                "destination": destination,
                "destinationCity": "",
                "cabin": "economy",
                "price": price,
                "currency": currency or "AED",
                "aircraft": str(aircraft),
                "bound_type": bound_type,
            }

            # For outbound bounds, attach inbound info from combinableWith
            if bound_type == "OUTBOUND" and inbound_info:
                flight["inbound_origin"] = inbound_info[0]
                flight["inbound_destination"] = inbound_info[1]
                ib_flt = inbound_info[2]
                flight["inbound_flightNo"] = ib_flt if ib_flt[:2].isalpha() else f"EK{ib_flt}"
                flight["inbound_depTime"] = "00:00"
                flight["inbound_arrTime"] = "00:00"

            out.append(flight)

    if out:
        logger.info("Emirates: extracted %d flights from bounds structure", len(out))


def _parse_api_option(opt: dict, fallback_currency: str) -> Optional[dict]:
    """Parse a single flight option from various Emirates API shapes."""
    # Extract price
    price = 0.0
    for pkey in ("totalPrice", "price", "amount", "total", "fareAmount", "totalAmount"):
        v = opt.get(pkey)
        if isinstance(v, (int, float)) and v > 0:
            price = float(v)
            break
        if isinstance(v, dict):
            for sk in ("amount", "total", "value"):
                sv = v.get(sk)
                if isinstance(sv, (int, float)) and sv > 0:
                    price = float(sv)
                    break
        if price > 0:
            break
    if price <= 0:
        ps = opt.get("priceSummary") or opt.get("pricing") or {}
        if isinstance(ps, dict):
            for psk in ("total", "grandTotal", "totalAmount"):
                pv = ps.get(psk)
                if isinstance(pv, dict):
                    pv = pv.get("amount")
                if isinstance(pv, (int, float)) and pv > 0:
                    price = float(pv)
                    break
    if price <= 0 or price > 50000:
        return None

    # Extract currency
    currency = fallback_currency
    for ck in ("currency", "currencyCode", "saleCurrency", "pricedCurrency"):
        cv = opt.get(ck)
        if isinstance(cv, str) and len(cv) == 3:
            currency = cv.upper()
            break

    # Extract segments/legs
    segments = []
    for skey in ("segments", "legs", "flights", "boundDetails", "journeySegments"):
        sv = opt.get(skey)
        if isinstance(sv, list) and sv:
            segments = sv
            break
    # Check outbound wrapper
    if not segments:
        ob = opt.get("outbound") or opt.get("bound") or opt.get("journey")
        if isinstance(ob, dict):
            for skey in ("segments", "legs", "flights"):
                sv = ob.get(skey)
                if isinstance(sv, list) and sv:
                    segments = sv
                    break

    dep_time = "00:00"
    arr_time = "00:00"
    origin = ""
    destination = ""
    flight_no = "EK"
    duration = 0
    stops = 0
    aircraft = ""

    if segments:
        stops = max(0, len(segments) - 1)
        first = segments[0] if isinstance(segments[0], dict) else {}
        last = segments[-1] if isinstance(segments[-1], dict) else {}

        # Departure from first segment
        for dk in ("departureTime", "departure", "departureDateTime", "depTime", "std"):
            dv = first.get(dk)
            if isinstance(dv, dict):
                dv = dv.get("time") or dv.get("dateTime") or dv.get("local")
            if isinstance(dv, str) and len(dv) >= 5:
                # Parse HH:MM from ISO or time string
                tm = re.search(r"(\d{2}):(\d{2})", dv)
                if tm:
                    dep_time = f"{tm.group(1)}:{tm.group(2)}"
                break

        # Arrival from last segment
        for ak in ("arrivalTime", "arrival", "arrivalDateTime", "arrTime", "sta"):
            av = last.get(ak)
            if isinstance(av, dict):
                av = av.get("time") or av.get("dateTime") or av.get("local")
            if isinstance(av, str) and len(av) >= 5:
                tm = re.search(r"(\d{2}):(\d{2})", av)
                if tm:
                    arr_time = f"{tm.group(1)}:{tm.group(2)}"
                break

        # Origin/destination
        for ok in ("departure", "origin", "departureAirport", "from"):
            ov = first.get(ok)
            if isinstance(ov, dict):
                ov = ov.get("code") or ov.get("iata") or ov.get("airportCode")
            if isinstance(ov, str) and len(ov) == 3:
                origin = ov.upper()
                break
        for dk in ("arrival", "destination", "arrivalAirport", "to"):
            dv = last.get(dk)
            if isinstance(dv, dict):
                dv = dv.get("code") or dv.get("iata") or dv.get("airportCode")
            if isinstance(dv, str) and len(dv) == 3:
                destination = dv.upper()
                break

        # Flight number
        for fk in ("flightNumber", "flightNo", "flight", "marketingFlightNumber"):
            fv = first.get(fk)
            if isinstance(fv, str) and fv:
                flight_no = fv.upper()
                break
        carrier = first.get("carrier") or first.get("airline") or first.get("marketingCarrier")
        if isinstance(carrier, dict):
            carrier = carrier.get("code") or carrier.get("iata")
        if isinstance(carrier, str) and carrier and not flight_no.startswith(carrier.upper()):
            flight_no = f"{carrier.upper()}{flight_no.lstrip('EK')}"

        # Duration
        for durk in ("duration", "totalDuration", "journeyDuration", "elapsedTime"):
            durv = opt.get(durk) or first.get(durk)
            if isinstance(durv, (int, float)) and durv > 0:
                duration = int(durv)
                break
            if isinstance(durv, str):
                hm = re.search(r"(\d+)\s*[hH]\s*(\d+)", durv)
                if hm:
                    duration = int(hm.group(1)) * 60 + int(hm.group(2))
                    break

        # Aircraft
        for atk in ("aircraftType", "aircraft", "equipmentType"):
            atv = first.get(atk)
            if isinstance(atv, dict):
                atv = atv.get("code") or atv.get("name")
            if isinstance(atv, str) and atv:
                aircraft = atv
                break
    else:
        # No segments — try flat fields
        for ok in ("origin", "departure", "from"):
            ov = opt.get(ok)
            if isinstance(ov, str) and len(ov) == 3:
                origin = ov.upper()
                break
        for dk in ("destination", "arrival", "to"):
            dv = opt.get(dk)
            if isinstance(dv, str) and len(dv) == 3:
                destination = dv.upper()
                break

    return {
        "flightNo": flight_no,
        "depTime": dep_time,
        "arrTime": arr_time,
        "dateStr": "",
        "duration": duration,
        "durationText": "",
        "nonstop": stops == 0,
        "stops": stops,
        "origin": origin,
        "originCity": "",
        "destination": destination,
        "destinationCity": "",
        "cabin": "economy",
        "price": price,
        "currency": currency or "AED",
        "aircraft": aircraft,
    }


class EmiratesConnectorClient:
    """Emirates CDP Chrome connector — form fill + results page scraping."""

    def __init__(self, timeout: float = 60.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest, _retry_on_block: bool = True) -> FlightSearchResponse:
        t0 = time.monotonic()

        context = await _get_context()
        page = await context.new_page()
        await auto_block_if_proxied(page)

        # Interception state
        flexi_data: dict = {}
        api_flights: list[dict] = []  # structured flight data from API interception
        akamai_blocked = False
        simplified_option_ids: set[str] = set()
        observed_lowest_price: float = 0.0
        observed_currency: str = ""
        search_request_tokens: set[str] = set()

        async def _on_response(response):
            nonlocal observed_currency, observed_lowest_price
            nonlocal akamai_blocked
            url = response.url
            tok = self._extract_search_request_token(url)
            if tok:
                search_request_tokens.add(tok)
            if "accessrestricted" in url:
                akamai_blocked = True
                return

            if "currency-conversion-rates" in url and response.status == 200:
                try:
                    qs = parse_qs(urlparse(url).query)
                    cur = (qs.get("pricedCurrency") or [""])[0]
                    amt = (qs.get("lowestPrice") or [""])[0]
                    if cur:
                        observed_currency = cur.upper()
                    if amt:
                        observed_lowest_price = float(amt)
                except Exception as e:
                    logger.warning("Emirates: currency-conversion-rates parsing failed: %s", e)

            if "/service/search-results/" in url and response.status == 200:
                # Generic extractor across Emirates search-result service payloads.
                # Different RT flows surface prices on different endpoints.
                try:
                    text = await response.text()
                    if text:
                        if not observed_currency:
                            cur_match = re.search(r'"(?:currency|currencyCode|pricedCurrency|saleCurrency)"\s*:\s*"([A-Z]{3})"', text)
                            if cur_match:
                                observed_currency = cur_match.group(1).upper()

                        # Try to parse as structured JSON with flight data
                        try:
                            payload = json.loads(text)
                            _extract_api_flights(payload, api_flights)
                        except (json.JSONDecodeError, ValueError):
                            pass

                        amounts = []
                        for m in re.finditer(r'"(?:amount|totalAmount|totalFare|price|lowestPrice|fareAmount)"\s*:\s*([0-9]+(?:\.[0-9]+)?)', text):
                            try:
                                v = float(m.group(1))
                                if 20.0 <= v <= 50000.0:
                                    amounts.append(v)
                            except Exception:
                                continue

                        if amounts:
                            best = min(amounts)
                            if observed_lowest_price <= 0 or best < observed_lowest_price:
                                observed_lowest_price = best
                except Exception:
                    pass

            if "simplified-fare-rules" in url and response.status == 200:
                try:
                    qs = parse_qs(urlparse(url).query)
                    raw = (qs.get("optionIds") or [""])[0]
                    for item in (raw or "").split(","):
                        item = item.strip()
                        if item:
                            simplified_option_ids.add(item)

                    # Some sessions never call currency-conversion-rates.
                    # Pull best-effort price/currency from simplified rules payload.
                    text = await response.text()
                    if text:
                        if not observed_currency:
                            cur_match = re.search(r'"(?:currency|currencyCode|pricedCurrency)"\s*:\s*"([A-Z]{3})"', text)
                            if cur_match:
                                observed_currency = cur_match.group(1).upper()

                        amounts = []
                        for m in re.finditer(r'"(?:amount|totalAmount|totalFare|price|lowestPrice)"\s*:\s*([0-9]+(?:\.[0-9]+)?)', text):
                            try:
                                v = float(m.group(1))
                                if 20.0 <= v <= 50000.0:
                                    amounts.append(v)
                            except Exception:
                                continue
                        if amounts and (observed_lowest_price <= 0 or min(amounts) < observed_lowest_price):
                            observed_lowest_price = min(amounts)
                except Exception:
                    pass

            if "flexi-fares" in url and response.status == 200:
                try:
                    data = await response.json()
                    if isinstance(data, dict) and "options" in data:
                        flexi_data.update(data)
                        logger.info("Emirates: captured flexi-fares response")
                except Exception:
                    pass

        def _on_request(request):
            tok = self._extract_search_request_token(request.url)
            if tok:
                search_request_tokens.add(tok)

        page.on("response", _on_response)
        page.on("request", _on_request)

        try:
            # Step 1: Load booking page
            logger.warning("Emirates: loading /book/ for %s→%s", req.origin, req.destination)
            await page.goto(
                "https://www.emirates.com/english/book/",
                wait_until="domcontentloaded",
                timeout=45000,
            )
            await asyncio.sleep(2.0)
            await _dismiss_overlays(page)
            await asyncio.sleep(0.5)

            # Fast path: direct search-results URL often works better than
            # form automation for RT and avoids UI anti-bot edge cases.
            direct_bootstrap_ok = False
            try:
                direct_url = self._build_direct_results_url(req)
                await page.goto(direct_url, wait_until="domcontentloaded", timeout=45000)
                await asyncio.sleep(3.0)
                if "search-results" in page.url:
                    direct_bootstrap_ok = True
                    logger.warning("Emirates: direct results bootstrap succeeded")
            except Exception as e:
                logger.warning("Emirates: direct bootstrap failed: %s", e)

            if not direct_bootstrap_ok:
                # Direct bootstrap failed (likely Akamai blocked) - navigate back to
                # the booking form page before attempting form fill
                logger.warning("Emirates: direct bootstrap failed, returning to booking form")
                await page.goto(
                    "https://www.emirates.com/english/book/",
                    wait_until="domcontentloaded",
                    timeout=45000,
                )
                await asyncio.sleep(2.0)
                await _dismiss_overlays(page)
                await asyncio.sleep(0.5)

                # Step 2: Select journey type based on request
                is_rt = req.return_from is not None
                ok = await self._set_journey_type(page, is_rt)
                if not ok:
                    logger.warning("Emirates: journey type selection failed")
                    return self._empty(req)
                await asyncio.sleep(1.0)


                # Step 3: Fill airport fields
                try:
                    ok = await asyncio.wait_for(
                        self._fill_airports(page, req.origin, req.destination),
                        timeout=20.0,
                    )
                except Exception:
                    ok = False
                if not ok:
                    logger.warning("Emirates: airport fill failed, trying direct results URL fallback")
                    direct_url = self._build_direct_results_url(req)
                    await page.goto(direct_url, wait_until="domcontentloaded", timeout=45000)
                    await asyncio.sleep(2.5)
                else:
                    # Step 4: Select date(s)
                    try:
                        ok = await asyncio.wait_for(
                            self._fill_date(page, req.date_from, leg_index=0),
                            timeout=15.0,
                        )
                    except Exception:
                        ok = False
                    if not ok:
                        logger.warning("Emirates: outbound date selection failed, trying direct results URL fallback")
                        direct_url = self._build_direct_results_url(req)
                        await page.goto(direct_url, wait_until="domcontentloaded", timeout=45000)
                        await asyncio.sleep(2.5)
                    else:
                        if req.return_from is not None:
                            try:
                                ok = await asyncio.wait_for(
                                    self._fill_date(page, req.return_from, leg_index=1),
                                    timeout=15.0,
                                )
                            except Exception:
                                ok = False
                            if not ok:
                                logger.warning("Emirates: return date selection failed, trying direct results URL fallback")
                                direct_url = self._build_direct_results_url(req)
                                await page.goto(direct_url, wait_until="domcontentloaded", timeout=45000)
                                await asyncio.sleep(2.5)
                            else:
                                # Step 5: Click "Search flights"
                                await page.evaluate("""() => {
                                    const btns = document.querySelectorAll('button');
                                    for (const b of btns) {
                                        if (b.textContent.trim() === 'Search flights' && b.offsetHeight > 0) {
                                            b.click(); return;
                                        }
                                    }
                                }""")
                                logger.warning("Emirates: search clicked")
                        else:
                            # Step 5: Click "Search flights"
                            await page.evaluate("""() => {
                                const btns = document.querySelectorAll('button');
                                for (const b of btns) {
                                    if (b.textContent.trim() === 'Search flights' && b.offsetHeight > 0) {
                                        b.click(); return;
                                    }
                                }
                            }""")
                            logger.warning("Emirates: search clicked")

            # Step 6: Wait for results page
            # Give results loading its own budget. RT can take materially longer
            # than one-way, and form filling already consumed most setup time.
            base_wait = 90.0 if req.return_from is not None else 60.0
            remaining = max(base_wait, float(self.timeout), 30.0)
            deadline = time.monotonic() + remaining
            got_results = False
            logged_url = False
            attempted_direct_recovery = False
            while time.monotonic() < deadline:
                await asyncio.sleep(1.0)
                url = page.url
                if akamai_blocked:
                    logger.warning("Emirates: Akamai blocked, proceeding to fallback extraction")
                    break
                if "search-results" in url:
                    # Wait for flight data to load
                    await asyncio.sleep(3.0)
                    got_results = True
                    break
                if not logged_url and time.monotonic() > deadline - remaining + 5:
                    logger.warning("Emirates: waiting for results, current URL: %s", url[:200])
                    logged_url = True

                # If we're still on /book/ late in the wait, force direct results URL once.
                if (
                    not attempted_direct_recovery
                    and "search-results" not in url
                    and "/book/" in url
                    and time.monotonic() > (deadline - remaining + min(20.0, remaining * 0.4))
                ):
                    attempted_direct_recovery = True
                    try:
                        direct_url = self._build_direct_results_url(req)
                        logger.warning("Emirates: forcing direct results recovery")
                        await page.goto(direct_url, wait_until="domcontentloaded", timeout=45000)
                        await asyncio.sleep(3.0)
                        if "search-results" in page.url:
                            got_results = True
                            break
                    except Exception as e:
                        logger.warning("Emirates: direct recovery navigation failed: %s", e)

            if not got_results:
                logger.warning("Emirates: never reached results page (URL: %s), trying last-resort scrape", page.url[:200])

            # Step 7: Extract flight data — prefer API interception, fall back to DOM
            logger.warning("Emirates: extracting results...")

            # Priority 1: Use structured API interception data
            flights = api_flights if api_flights else []
            if flights:
                logger.warning("Emirates: using %d flights from API interception", len(flights))
            else:
                # Priority 2: DOM scraping (multi-strategy)
                flights = await self._scrape_results(page, req)

            # For RT requests with bounds-parsed outbound flights, ensure inbound info is set.
            # Also covers DOM/fallback flights that lack bound_type entirely —
            # without this, non-bounds flights would appear as one-way and get
            # dropped by the engine's RT preference filter.
            if req.return_from is not None and flights:
                for f in flights:
                    bt = (f.get("bound_type") or "").upper()
                    # Skip INBOUND legs — they're the return direction, not outbound
                    if bt == "INBOUND":
                        continue
                    # Set inbound info for OUTBOUND legs or untyped flights (DOM/fallback)
                    if not f.get("inbound_origin"):
                        f["inbound_origin"] = req.destination
                        f["inbound_destination"] = req.origin
                        f["inbound_depTime"] = "00:00"
                        f["inbound_arrTime"] = "00:00"
            
            # Track if we got proper structured flights (with real times) vs body-text fallback
            has_real_times = flights and any(
                f.get("depTime") not in ("", "00:00") for f in flights
            )

            # For round-trip body-text fallback scrapes (no real times), multiple
            # prices are extracted from the page. These are typically all RT totals
            # for different flight/fare combinations — keep the lowest (best deal).
            # DO NOT filter proper DOM-scraped results that have real departure times.
            if (
                flights 
                and req.return_from is not None 
                and len(flights) > 1 
                and not has_real_times
            ):
                # Sort by price ascending, keep only the lowest (cheapest RT fare)
                flights = sorted(flights, key=lambda f: f.get("price", 0))[:1]
                logger.warning("Emirates: RT body-text fallback filter kept lowest price=%s", 
                              flights[0].get("price") if flights else None)

            if not flights:
                # Some Emirates sessions render fares in embedded JSON only.
                # Scan full HTML source for currency + amount patterns.
                try:
                    html = await page.content()
                except Exception:
                    html = ""
                if html:
                    if not observed_currency:
                        cur_match = re.search(r'"(?:currency|currencyCode|pricedCurrency|saleCurrency)"\s*:\s*"([A-Z]{3})"', html)
                        if cur_match:
                            observed_currency = cur_match.group(1).upper()

                    amounts = []
                    for m in re.finditer(r'"(?:amount|totalAmount|totalFare|price|lowestPrice|fareAmount)"\s*:\s*([0-9]+(?:\.[0-9]+)?)', html):
                        try:
                            v = float(m.group(1))
                            if 20.0 <= v <= 50000.0:
                                amounts.append(v)
                        except Exception:
                            continue

                    # Also parse embedded JSON state (e.g. __NEXT_DATA__).
                    json_blobs = []
                    for m in re.finditer(r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, flags=re.DOTALL | re.IGNORECASE):
                        blob = (m.group(1) or "").strip()
                        if blob:
                            json_blobs.append(blob)

                    for m in re.finditer(r'<script[^>]*type="application/json"[^>]*>(.*?)</script>', html, flags=re.DOTALL | re.IGNORECASE):
                        blob = (m.group(1) or "").strip()
                        if blob and len(blob) > 20:
                            json_blobs.append(blob)

                    def _walk(node, parent_key=""):
                        nonlocal observed_currency
                        if isinstance(node, dict):
                            for k, v in node.items():
                                lk = str(k).lower()
                                if isinstance(v, str) and len(v) == 3 and v.isalpha() and "currency" in lk and not observed_currency:
                                    observed_currency = v.upper()

                                if any(t in lk for t in ["amount", "price", "fare", "total"]):
                                    cand = None
                                    if isinstance(v, (int, float)):
                                        cand = float(v)
                                    elif isinstance(v, str):
                                        txt = v.replace(",", "").strip()
                                        if re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", txt):
                                            try:
                                                cand = float(txt)
                                            except Exception:
                                                cand = None
                                    if cand is not None and 20.0 <= cand <= 50000.0:
                                        amounts.append(cand)
                                _walk(v, lk)
                        elif isinstance(node, list):
                            for it in node:
                                _walk(it, parent_key)

                    for blob in json_blobs[:6]:
                        try:
                            parsed = json.loads(blob)
                            _walk(parsed)
                        except Exception:
                            continue

                    # Only use HTML-extracted prices if API didn't confirm a price.
                    # For RT with confirmed API price, don't overwrite with regex results.
                    if amounts and observed_lowest_price <= 0:
                        observed_lowest_price = min(amounts)
                        logger.warning("Emirates: extracted observed price from HTML source")

            if not flights:
                # Fallback 1: if intercepted flexi-fares exists, use it.
                if flexi_data and flexi_data.get("options"):
                    flights = self._parse_flexi_fares(flexi_data, req)
                    logger.warning("Emirates: using intercepted flexi-fares fallback (%d offers)", len(flights))

            if not flights:
                # Fallback 2: fetch flexi-fares directly from captured searchRequest token.
                try:
                    sr_token = ""
                    if search_request_tokens:
                        sr_token = next(iter(search_request_tokens))
                    if not sr_token:
                        sr_token = self._extract_search_request_token(page.url)

                    if sr_token:
                        api_url = (
                            "https://www.emirates.com/english/book/service/search-results/flexi-fares"
                            f"?searchRequest={sr_token}"
                        )
                        resp = await page.request.get(api_url, timeout=30000)
                        if resp.ok:
                            data = await resp.json()
                            if isinstance(data, dict) and data.get("options"):
                                flights = self._parse_flexi_fares(data, req)
                                logger.warning("Emirates: using direct flexi-fares API fallback (%d offers)", len(flights))
                except Exception as e:
                    logger.warning("Emirates: direct flexi-fares API fallback failed: %s", e)

            if not flights and simplified_option_ids and observed_lowest_price > 0:
                api_fallback: list[dict] = []
                for opt in sorted(simplified_option_ids):
                    parts = opt.split("_")
                    origin = req.origin
                    destination = req.destination
                    flight_no = "EK"
                    if len(parts) >= 3:
                        origin = parts[0] or req.origin
                        destination = parts[1] or req.destination
                        flight_no = parts[2] or "EK"

                    api_fallback.append({
                        "flightNo": flight_no,
                        "depTime": "00:00",
                        "arrTime": "00:00",
                        "dateStr": "",
                        "duration": 0,
                        "durationText": "",
                        "nonstop": True,
                        "stops": 0,
                        "origin": origin,
                        "originCity": "",
                        "destination": destination,
                        "destinationCity": "",
                        "cabin": "economy",
                        "price": float(observed_lowest_price),
                        "currency": observed_currency or "EUR",
                        "aircraft": "",
                        "inbound_depTime": "00:00",
                        "inbound_arrTime": "00:00",
                        "inbound_origin": req.destination if req.return_from is not None else "",
                        "inbound_destination": req.origin if req.return_from is not None else "",
                    })

                if api_fallback:
                    flights = api_fallback
                    logger.warning("Emirates: using simplified-fare-rules fallback (%d offers)", len(flights))

            if not flights and observed_lowest_price > 0:
                logger.warning("Emirates: building fallback offer with price=%s currency=%s", observed_lowest_price, observed_currency)
                flights = [{
                    "flightNo": "EK",
                    "depTime": "00:00",
                    "arrTime": "00:00",
                    "dateStr": "",
                    "duration": 0,
                    "durationText": "",
                    "nonstop": False,
                    "stops": 0,
                    "origin": req.origin,
                    "originCity": "",
                    "destination": req.destination,
                    "destinationCity": "",
                    "cabin": "economy",
                    "price": float(observed_lowest_price),
                    "currency": observed_currency or "EUR",
                    "aircraft": "",
                    "inbound_depTime": "00:00",
                    "inbound_arrTime": "00:00",
                    "inbound_origin": req.destination if req.return_from is not None else "",
                    "inbound_destination": req.origin if req.return_from is not None else "",
                }]
                logger.warning("Emirates: using observed-price fallback")

            offers = []
            for f in flights:
                offer = self._build_offer(f, req)
                if offer:
                    offers.append(offer)

            offers.sort(key=lambda o: o.price)

            elapsed = time.monotonic() - t0
            logger.info(
                "Emirates %s→%s returned %d offers in %.1fs",
                req.origin, req.destination, len(offers), elapsed,
            )

            search_hash = hashlib.md5(
                f"emirates{req.origin}{req.destination}{req.date_from}".encode()
            ).hexdigest()[:12]

            currency = offers[0].currency if offers else "AED"
            if offers:
                segs = offers[0].outbound.segments if offers[0].outbound else []
                anc_origin = segs[0].origin if segs else req.origin
                anc_dest = segs[-1].destination if segs else req.destination
                try:
                    ancillary = await asyncio.wait_for(
                        self._fetch_ancillaries(anc_origin, anc_dest, req.date_from.isoformat(), req.adults, currency),
                        timeout=45.0,
                    )
                    if ancillary:
                        self._apply_ancillaries(offers, ancillary)
                except (asyncio.TimeoutError, TimeoutError):
                    logger.debug("Ancillary fetch timed out for %s\u2192%s", anc_origin, anc_dest)
                except Exception as _anc_err:
                    logger.debug("Ancillary fetch error for %s\u2192%s: %s", anc_origin, anc_dest, _anc_err)
            return FlightSearchResponse(
                search_id=f"fs_{search_hash}",
                origin=req.origin,
                destination=req.destination,
                currency=currency,
                offers=offers,
                total_results=len(offers),
            )

        except Exception as e:
            logger.warning("Emirates CDP error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass


    async def _fetch_ancillaries(
        self, origin: str, dest: str, date_str: str, adults: int, currency: str
    ) -> dict | None:
        # Emirates includes checked bag on all economy fares.
        return {
            "bags_note": "1 checked bag included (25–35 kg depending on route). Carry-on 7 kg included.",
            "seat_note": "Seat selection: standard seat free at online check-in. Preferred/extra-legroom from USD 25.",
            "bags_from": None,
            "currency": currency,
        }

    def _apply_ancillaries(self, offers: list, ancillary: dict) -> None:
        bags_note = ancillary.get("bags_note")
        checked_note = ancillary.get("checked_bag") or bags_note
        seat_note = ancillary.get("seat_note")
        bags_from = ancillary.get("bags_from")
        checked_from = ancillary.get("checked_bag_price")
        anc_currency = ancillary.get("currency", "EUR")
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

    # ------------------------------------------------------------------
    # Form fill helpers
    # ------------------------------------------------------------------

    def _build_direct_results_url(self, req: FlightSearchRequest) -> str:
        payload = {
            "origin": req.origin,
            "destination": req.destination,
            "originType": "AIRPORT",
            "destinationType": "AIRPORT",
            "departureDate": req.date_from.isoformat(),
            "returnDate": req.return_from.isoformat() if req.return_from else "",
            "adultCount": str(max(1, int(getattr(req, "adults", 1) or 1))),
            "childCount": "0",
            "infantCount": "0",
            "isFlexible": "false",
            "promoCode": "",
            "isStudent": "false",
            "isCash": "true",
            "isReward": "false",
            "country": "US",
            "searchType": "BOOKING",
            "class": {"M": "ECONOMY", "W": "ECONOMY", "C": "BUSINESS", "F": "FIRST"}.get(req.cabin_class or "M", "ECONOMY"),
            "flightSearchType": "ROUND_TRIP" if req.return_from else "ONE_WAY",
            "journeyType": "RETURN" if req.return_from else "OW",
        }
        encoded = quote(json.dumps(payload, separators=(",", ":")), safe="")
        # Use /booking/search-results (without /english/book) to match manual browsing flow
        # which triggers currency-conversion-rates API call with price data
        return f"https://www.emirates.com/booking/search-results/?searchRequest={encoded}"

    def _extract_search_request_token(self, url: str) -> str:
        if not url:
            return ""
        m = re.search(r"[?&]searchRequest=([^&]+)", url)
        return m.group(1) if m else ""

    async def _fill_airports(self, page, origin: str, destination: str) -> bool:
        """Fill departure and arrival auto-suggest fields."""
        inputs = page.locator("input[id^='auto-suggest_']")
        count = await inputs.count()
        if count < 2:
            logger.warning("Emirates: only %d auto-suggest inputs found", count)
            return False

        # Departure
        ok = await self._fill_auto_suggest(page, inputs.first, origin)
        if not ok:
            return False
        await asyncio.sleep(1.0)

        # Arrival
        ok = await self._fill_auto_suggest(page, inputs.nth(1), destination)
        if not ok:
            return False
        await asyncio.sleep(1.0)
        return True

    async def _set_journey_type(self, page, is_rt: bool) -> bool:
        """Switch between one-way and round-trip with broad selector fallbacks."""
        try:
            mode = "return" if is_rt else "one way"
            picked = await page.evaluate("""(isRt) => {
                const wants = isRt
                    ? [/^return$/i, /^round\\s*-?\\s*trip$/i, /^roundtrip$/i]
                    : [/^one\\s*-?\\s*way$/i];

                const pick = (nodes) => {
                    for (const n of nodes) {
                        const t = (n.textContent || '').trim();
                        if (!t || n.offsetHeight === 0) continue;
                        for (const rx of wants) {
                            if (rx.test(t)) {
                                n.click();
                                return t;
                            }
                        }
                    }
                    return null;
                };

                // Buttons/tabs first
                let p = pick(document.querySelectorAll('button, [role="tab"], [role="button"], label'));
                if (p) return p;

                // Radio fallback
                const radios = document.querySelectorAll('input[type="radio"]');
                for (const r of radios) {
                    const id = r.getAttribute('id');
                    const aria = (r.getAttribute('aria-label') || '').trim();
                    let labelText = aria;
                    if (!labelText && id) {
                        const lbl = document.querySelector(`label[for="${id}"]`);
                        labelText = (lbl?.textContent || '').trim();
                    }
                    for (const rx of wants) {
                        if (rx.test(labelText || '')) {
                            r.click();
                            return labelText || id || 'radio';
                        }
                    }
                }

                return null;
            }""", is_rt)

            await asyncio.sleep(1.5)
            if is_rt:
                has_return_input = await page.evaluate("""() => {
                    const selectors = [
                        '#date-input1', '#endDate', '#returnDate',
                        '[data-ref="return-date-input"]',
                        'input[id*="date-input1"]',
                        'input[name*="return" i]',
                    ];
                    for (const sel of selectors) {
                        const el = document.querySelector(sel);
                        if (el && el.offsetHeight > 0) return true;
                    }
                    // Generic fallback: at least 2 visible date-like inputs
                    const all = [...document.querySelectorAll('input')].filter(i => {
                        const id = (i.id || '').toLowerCase();
                        const name = (i.name || '').toLowerCase();
                        return (id.includes('date') || name.includes('date') || name.includes('return')) && i.offsetHeight > 0;
                    });
                    return all.length >= 2;
                }""")
                if not has_return_input:
                    logger.warning("Emirates: failed to confirm return mode after selecting journey type")
                    return False

            if picked:
                logger.warning("Emirates: selected journey type via '%s'", picked)
            else:
                logger.warning("Emirates: journey type control not found for mode '%s'", mode)
            return True
        except Exception as e:
            logger.warning("Emirates: journey type selection error: %s", e)
            return False

    async def _fill_auto_suggest(self, page, field, iata: str) -> bool:
        """Type into an auto-suggest field and select from dropdown."""
        try:
            # JS click to bypass label / header pointer interception
            el_handle = await field.element_handle()
            await page.evaluate("el => { el.focus(); el.click(); }", el_handle)
            await asyncio.sleep(0.5)
            # Select all existing text, then type IATA
            await page.evaluate("el => el.select()", el_handle)
            await field.type(iata, delay=100)
            await asyncio.sleep(2.5)

            # Click dropdown option matching IATA code
            selected = await page.evaluate("""(iata) => {
                const items = document.querySelectorAll(
                    '[role="option"], [role="group"] div'
                );
                for (const item of items) {
                    const text = (item.textContent || '').trim();
                    if (text.includes(iata) && item.offsetHeight > 0) {
                        item.click();
                        return text.slice(0, 80);
                    }
                }
                return null;
            }""", iata)

            if not selected:
                # Keyboard fallback
                await field.press("ArrowDown")
                await asyncio.sleep(0.2)
                await field.press("Enter")

            await asyncio.sleep(1.0)
            value = await field.input_value()
            if iata.upper() in value.upper():
                logger.info("Emirates: filled airport → %s", value)
                return True

            if value and len(value) > 2:
                logger.info("Emirates: airport filled with '%s' (expected %s)", value, iata)
                return True

            logger.warning("Emirates: airport fill failed for %s (got '%s')", iata, value)
            return False

        except Exception as e:
            logger.warning("Emirates: auto-suggest error for %s: %s", iata, e)
            return False

    async def _fill_date(self, page, target_date_value, leg_index: int = 0) -> bool:
        """Open calendar for a leg, navigate to target month, click target day."""
        try:
            dt = (
                target_date_value
                if isinstance(target_date_value, (datetime, date))
                else datetime.strptime(str(target_date_value), "%Y-%m-%d")
            )
        except (ValueError, TypeError):
            logger.warning("Emirates: invalid date value: %s", target_date_value)
            return False

        target_month = dt.strftime("%B %Y")  # e.g. "June 2026"
        target_day = str(dt.day)
        target_month_name = dt.strftime("%B")  # e.g. "June"
        target_year = str(dt.year)

        try:
            # Unstick the header so it doesn't block clicks
            await page.evaluate("""() => {
                const hdr = document.querySelector('.header-popup__wrapper--sticky');
                if (hdr) hdr.style.position = 'relative';
            }""")

            # JS-click the date input (label/header overlaps the input)
            input_selector = '#date-input0, #startDate, [data-ref="date-input"]'
            if leg_index == 1:
                input_selector = '#date-input1, #endDate, #returnDate, [data-ref="return-date-input"]'

            clicked = await page.evaluate("""(args) => {
                const [selector, legIndex] = args;
                const direct = document.querySelector(selector);
                if (direct) {
                    direct.focus();
                    direct.click();
                    return true;
                }

                // Fallback: pick Nth visible date-like input
                const all = [...document.querySelectorAll('input')].filter(i => {
                    const id = (i.id || '').toLowerCase();
                    const name = (i.name || '').toLowerCase();
                    return (id.includes('date') || name.includes('date') || name.includes('return')) && i.offsetHeight > 0;
                });
                const idx = legIndex > 0 ? 1 : 0;
                const picked = all[idx] || all[0];
                if (picked) {
                    picked.focus();
                    picked.click();
                    return true;
                }
                return false;
            }""", [input_selector, leg_index])
            if not clicked:
                logger.warning("Emirates: date input not found for leg %d", leg_index)
                return False
            await asyncio.sleep(2.0)

            # Navigate calendar to target month
            for click_idx in range(18):
                visible_months = await page.evaluate(r"""() => {
                    // Strategy 1: DayPicker .CalendarMonth_caption strong
                    let caps = [...document.querySelectorAll('.CalendarMonth_caption strong')].map(c => c.textContent);
                    if (caps.length > 0) return caps.filter(Boolean);
                    // Strategy 2: aria-label on month headings
                    caps = [...document.querySelectorAll('[class*="month"] [class*="caption"], [class*="Month"] h3, [class*="calendar"] h2')]
                        .map(c => c.textContent);
                    if (caps.length > 0) return caps.filter(Boolean);
                    // Strategy 3: anything that looks like "Month YYYY"
                    const all = document.querySelectorAll('strong, h2, h3, [class*="heading"], [class*="title"], span');
                    return [...all].map(e => e.textContent).filter(t => /^[A-Z][a-z]+ \d{4}$/.test(t?.trim()));
                }""")

                # Check if target month is visible
                found = False
                for vm in (visible_months or []):
                    if vm and target_month_name in vm and target_year in vm:
                        found = True
                        break
                if found:
                    logger.warning("Emirates: calendar reached %s (click %d)", target_month, click_idx)
                    break

                if click_idx == 0:
                    logger.warning("Emirates: calendar visible months: %s, navigating to %s",
                                   visible_months, target_month)

                # Click forward button
                clicked_fwd = await page.evaluate("""() => {
                    const selectors = [
                        'button[aria-label*="forward"]', 'button[aria-label*="next"]',
                        'button[aria-label*="Forward"]', 'button[aria-label*="Next"]',
                        '.DayPickerNavigation_button:last-of-type',
                        '[class*="navigation"] button:last-of-type',
                        '[class*="calendar"] [class*="next"]',
                        '[class*="arrow-right"]', '[class*="nav-next"]',
                        '[class*="chevron-right"]', '[class*="right-arrow"]',
                        'button[class*="next"]',
                    ];
                    for (const sel of selectors) {
                        const next = document.querySelector(sel);
                        if (next && next.offsetHeight > 0) { next.click(); return sel; }
                    }
                    return null;
                }""")
                if not clicked_fwd:
                    logger.warning("Emirates: no forward button found at click %d, months: %s", click_idx, visible_months)
                    break
                logger.warning("Emirates: fwd click %d via %s", click_idx, clicked_fwd)
                await asyncio.sleep(1.5)
            else:
                logger.warning("Emirates: exhausted calendar navigation (18 clicks)")

            # Click the target day — two-phase: mark element, then Playwright-click.
            # Using page.evaluate to click directly can hang if the click
            # triggers a page re-render or navigation.
            date_iso = dt.strftime("%Y-%m-%d")

            # Phase 1: Mark the target element with a data attribute
            strategy = await page.evaluate("""(args) => {
                const [targetMonth, targetDay, targetMonthName, targetYear, dateISO] = args;

                // Strategy 1: CalendarMonth caption match + td with day text
                const months = document.querySelectorAll('.CalendarMonth');
                for (const m of months) {
                    const cap = m.querySelector('.CalendarMonth_caption strong, .CalendarMonth_caption');
                    const capText = cap ? cap.textContent.trim() : '';
                    if (capText.includes(targetMonthName) && capText.includes(targetYear)) {
                        const cells = m.querySelectorAll('td, button, [role="gridcell"], [role="button"]');
                        for (const d of cells) {
                            if (d.textContent.trim() === targetDay) {
                                d.setAttribute('data-letsfg-target', 'day');
                                return 'month-caption-td';
                            }
                        }
                    }
                }

                // Strategy 2: data-date attribute
                const byDate = document.querySelector('td[data-date="' + dateISO + '"], [data-date="' + dateISO + '"]');
                if (byDate) { byDate.setAttribute('data-letsfg-target', 'day'); return 'data-date'; }

                // Strategy 3: aria-label
                const ariaPatterns = [
                    targetDay + ' ' + targetMonthName + ' ' + targetYear,
                    targetMonthName + ' ' + targetDay + ', ' + targetYear,
                    dateISO,
                ];
                for (const pat of ariaPatterns) {
                    for (const c of document.querySelectorAll('[aria-label]')) {
                        if ((c.getAttribute('aria-label') || '').includes(pat)) {
                            c.setAttribute('data-letsfg-target', 'day');
                            return 'aria';
                        }
                    }
                }

                // Strategy 4: brute walk calendar containers
                const calContainers = document.querySelectorAll(
                    '.DayPicker, [class*="calendar"], [class*="Calendar"], [class*="DayPicker"], table'
                );
                for (const container of calContainers) {
                    for (const c of container.querySelectorAll('td, button')) {
                        if (c.textContent.trim() === targetDay) {
                            const parentText = (c.closest('table, [class*="month"], [class*="Month"]') || container).textContent;
                            if (parentText.includes(targetMonthName)) {
                                c.setAttribute('data-letsfg-target', 'day');
                                return 'walk';
                            }
                        }
                    }
                }

                return null;
            }""", [target_month, target_day, target_month_name, target_year, date_iso])

            if not strategy:
                logger.warning("Emirates: could not find day %s in %s", target_day, target_month)
                return False

            # Phase 2: Scroll into view, force visibility, then click via Playwright
            try:
                # First scroll the marked element into view and ensure visibility
                await page.evaluate("""() => {
                    const el = document.querySelector('[data-letsfg-target="day"]');
                    if (el) {
                        el.scrollIntoView({block: 'center', behavior: 'instant'});
                        // Force visibility if the element is hidden
                        if (el.offsetHeight === 0) {
                            el.style.display = 'block';
                            el.style.visibility = 'visible';
                        }
                    }
                }""")
                await asyncio.sleep(0.3)

                target_el = page.locator('[data-letsfg-target="day"]').first
                await target_el.click(timeout=5000, force=True)
                logger.warning("Emirates: date selected via Playwright click (%s)", strategy)
            except Exception as e:
                logger.warning("Emirates: Playwright click on marked day failed: %s", e)
                # Fallback: simulate full React-compatible event sequence via JS
                await page.evaluate("""() => {
                    const el = document.querySelector('[data-letsfg-target="day"]');
                    if (el) {
                        // Scroll into view first
                        el.scrollIntoView({block: 'center'});
                        // React listens to mousedown/mouseup/click at the document level
                        const rect = el.getBoundingClientRect();
                        const cx = rect.left + rect.width / 2;
                        const cy = rect.top + rect.height / 2;
                        const opts = {bubbles: true, cancelable: true, clientX: cx, clientY: cy, view: window};
                        el.dispatchEvent(new MouseEvent('mousedown', opts));
                        el.dispatchEvent(new MouseEvent('mouseup', opts));
                        el.dispatchEvent(new MouseEvent('click', opts));
                    }
                }""")
                logger.warning("Emirates: date selected via React-compat event dispatch")

            # Clean up marker attribute and wait for UI update
            await page.evaluate("() => { const el = document.querySelector('[data-letsfg-target]'); if (el) el.removeAttribute('data-letsfg-target'); }")
            await asyncio.sleep(1.5)
            return True

        except Exception as e:
            logger.warning("Emirates: date selection error: %s", e)
            return False

    async def _fill_return_date(self, page, req: FlightSearchRequest) -> bool:
        """Fill the return date field for round-trip searches."""
        try:
            dt = req.return_from if isinstance(req.return_from, (datetime, date)) else datetime.strptime(str(req.return_from), "%Y-%m-%d")
        except (ValueError, TypeError):
            return False

        target_month_name = dt.strftime("%B")
        target_year = str(dt.year)
        target_day = str(dt.day)
        date_iso = dt.strftime("%Y-%m-%d") if hasattr(dt, 'strftime') else str(dt)

        try:
            # Click the return date input
            await page.evaluate("""() => {
                const inp = document.querySelector('#date-input1, #endDate, [data-ref="date-input-return"]');
                if (inp) { inp.focus(); inp.click(); }
            }""")
            await asyncio.sleep(2.0)

            # Navigate calendar to target month
            for _ in range(18):
                visible_months = await page.evaluate(r"""() => {
                    let caps = [...document.querySelectorAll('.CalendarMonth_caption strong')].map(c => c.textContent);
                    if (caps.length > 0) return caps.filter(Boolean);
                    const all = document.querySelectorAll('strong, h2, h3, span');
                    return [...all].map(e => e.textContent).filter(t => /^[A-Z][a-z]+ \d{4}$/.test(t?.trim()));
                }""")
                found = any(target_month_name in (vm or "") and target_year in (vm or "") for vm in (visible_months or []))
                if found:
                    break
                await page.evaluate("""() => {
                    const selectors = ['button[aria-label*="forward"]', 'button[aria-label*="next"]',
                        '.DayPickerNavigation_button:last-of-type', '[class*="nav-next"]'];
                    for (const s of selectors) {
                        const btn = document.querySelector(s);
                        if (btn && btn.offsetHeight > 0) { btn.click(); return true; }
                    }
                    return false;
                }""")
                await asyncio.sleep(0.5)

            # Click the target day
            clicked = await page.evaluate("""(args) => {
                const [targetDay, targetMonthName, dateIso] = args;
                // aria-label approach
                const labels = document.querySelectorAll('td[aria-label], button[aria-label]');
                for (const el of labels) {
                    const lbl = el.getAttribute('aria-label') || '';
                    if (lbl.includes(dateIso) || (lbl.includes(targetDay) && lbl.includes(targetMonthName))) {
                        el.click(); return true;
                    }
                }
                // Walk calendar cells
                const cells = document.querySelectorAll('td, button');
                for (const c of cells) {
                    if (c.textContent.trim() === targetDay) {
                        const parent = (c.closest('table, [class*="month"]') || c.parentElement || {}).textContent || '';
                        if (parent.includes(targetMonthName)) {
                            c.click(); return true;
                        }
                    }
                }
                return false;
            }""", [target_day, target_month_name, date_iso])

            await asyncio.sleep(1.5)
            if clicked:
                logger.info("Emirates: return date selected → %s", date_iso)
            return bool(clicked)

        except Exception as e:
            logger.warning("Emirates: return date error: %s", e)
            return False

    # ------------------------------------------------------------------
    # DOM scraping
    # ------------------------------------------------------------------

    async def _scrape_results(self, page, req: Optional[FlightSearchRequest] = None) -> list[dict]:
        """Scrape flight cards from the results page using multiple strategies.

        Strategy order (most reliable first):
        A. __NEXT_DATA__ JSON — Next.js serialized state with full flight data
        B. CSS-selector structured DOM — flight card elements with data attributes
        C. Body innerText line-walk — original fragile approach (last resort)
        """
        # ── Strategy A: __NEXT_DATA__ JSON ──
        next_data_flights = await page.evaluate(r"""() => {
            try {
                const script = document.getElementById('__NEXT_DATA__');
                if (!script) return null;
                const data = JSON.parse(script.textContent);
                const results = [];

                function walk(node, depth) {
                    if (depth > 12 || !node) return;
                    if (Array.isArray(node)) {
                        for (const item of node) walk(item, depth + 1);
                        return;
                    }
                    if (typeof node !== 'object') return;

                    // Detect flight option: has price + segments/legs/departure info
                    const hasPrice = ['totalPrice','price','amount','total','fareAmount','totalAmount']
                        .some(k => node[k] !== undefined && node[k] !== null);
                    const hasSegments = ['segments','legs','flights','boundDetails','journeySegments']
                        .some(k => Array.isArray(node[k]) && node[k].length > 0);
                    const hasDep = ['departureTime','departure','departureDateTime','depTime','std']
                        .some(k => node[k] !== undefined);

                    if (hasPrice && (hasSegments || hasDep)) {
                        let price = 0;
                        for (const pk of ['totalPrice','price','amount','total','fareAmount','totalAmount']) {
                            const v = node[pk];
                            if (typeof v === 'number' && v > 0) { price = v; break; }
                            if (typeof v === 'object' && v) {
                                for (const sk of ['amount','total','value']) {
                                    if (typeof v[sk] === 'number' && v[sk] > 0) { price = v[sk]; break; }
                                }
                            }
                            if (price > 0) break;
                        }
                        const ps = node.priceSummary || node.pricing || {};
                        if (price <= 0 && typeof ps === 'object') {
                            for (const psk of ['total','grandTotal','totalAmount']) {
                                let pv = ps[psk];
                                if (typeof pv === 'object' && pv) pv = pv.amount;
                                if (typeof pv === 'number' && pv > 0) { price = pv; break; }
                            }
                        }
                        if (price > 20 && price < 50000) {
                            let currency = '';
                            for (const ck of ['currency','currencyCode','saleCurrency','pricedCurrency']) {
                                const cv = node[ck];
                                if (typeof cv === 'string' && cv.length === 3) { currency = cv.toUpperCase(); break; }
                            }

                            // Extract first/last segment info
                            let segs = null;
                            for (const sk of ['segments','legs','flights','boundDetails']) {
                                if (Array.isArray(node[sk]) && node[sk].length) { segs = node[sk]; break; }
                            }
                            let depTime = '00:00', arrTime = '00:00', origin = '', dest = '', flightNo = 'EK', stops = 0, aircraft = '';
                            if (segs && segs.length) {
                                stops = Math.max(0, segs.length - 1);
                                const first = segs[0] || {};
                                const last = segs[segs.length - 1] || {};
                                for (const dk of ['departureTime','departure','departureDateTime','depTime','std']) {
                                    let dv = first[dk];
                                    if (typeof dv === 'object' && dv) dv = dv.time || dv.dateTime || dv.local;
                                    if (typeof dv === 'string' && dv.length >= 5) {
                                        const tm = dv.match(/(\d{2}):(\d{2})/);
                                        if (tm) { depTime = tm[1]+':'+tm[2]; break; }
                                    }
                                }
                                for (const ak of ['arrivalTime','arrival','arrivalDateTime','arrTime','sta']) {
                                    let av = last[ak];
                                    if (typeof av === 'object' && av) av = av.time || av.dateTime || av.local;
                                    if (typeof av === 'string' && av.length >= 5) {
                                        const tm = av.match(/(\d{2}):(\d{2})/);
                                        if (tm) { arrTime = tm[1]+':'+tm[2]; break; }
                                    }
                                }
                                for (const ok of ['departure','origin','departureAirport','from']) {
                                    let ov = first[ok];
                                    if (typeof ov === 'object' && ov) ov = ov.code || ov.iata || ov.airportCode;
                                    if (typeof ov === 'string' && ov.length === 3) { origin = ov.toUpperCase(); break; }
                                }
                                for (const dk2 of ['arrival','destination','arrivalAirport','to']) {
                                    let dv2 = last[dk2];
                                    if (typeof dv2 === 'object' && dv2) dv2 = dv2.code || dv2.iata || dv2.airportCode;
                                    if (typeof dv2 === 'string' && dv2.length === 3) { dest = dv2.toUpperCase(); break; }
                                }
                                for (const fk of ['flightNumber','flightNo','flight','marketingFlightNumber']) {
                                    const fv = first[fk];
                                    if (typeof fv === 'string' && fv) { flightNo = fv.toUpperCase(); break; }
                                }
                                for (const atk of ['aircraftType','aircraft','equipmentType']) {
                                    let atv = first[atk];
                                    if (typeof atv === 'object' && atv) atv = atv.code || atv.name;
                                    if (typeof atv === 'string' && atv) { aircraft = atv; break; }
                                }
                            }

                            results.push({
                                flightNo, depTime, arrTime, dateStr: '', duration: 0,
                                durationText: '', nonstop: stops === 0, stops,
                                origin, originCity: '', destination: dest, destinationCity: '',
                                cabin: 'economy', price, currency: currency || 'AED', aircraft,
                            });
                        }
                        return; // don't recurse into already-processed option
                    }

                    for (const k of Object.keys(node)) walk(node[k], depth + 1);
                }

                walk(data, 0);
                return results.length > 0 ? results : null;
            } catch (e) { return null; }
        }""")

        if next_data_flights:
            logger.info("Emirates: __NEXT_DATA__ extraction got %d flights", len(next_data_flights))
            return next_data_flights

        # ── Strategy B: CSS-selector structured DOM ──
        css_flights = await page.evaluate(r"""() => {
            const results = [];
            // Emirates flight cards use various selectors across versions
            const cardSelectors = [
                '[data-auto="flight-card"]', '[data-testid="flight-card"]',
                '.flight-card', '.bound-card', '.result-card',
                '[class*="FlightCard"]', '[class*="flight-card"]',
                '[class*="BoundCard"]', '[class*="bound-card"]',
                '[class*="ResultCard"]', '[class*="result-card"]',
                'article[class*="flight"]', 'div[class*="itinerary-card"]',
            ];
            let cards = [];
            for (const sel of cardSelectors) {
                cards = document.querySelectorAll(sel);
                if (cards.length > 0) break;
            }
            if (cards.length === 0) return null;

            for (const card of cards) {
                const text = card.innerText || '';
                // Extract times (HH:MM pattern)
                const times = text.match(/\b(\d{1,2}:\d{2})\b/g) || [];
                const depTime = times[0] || '00:00';
                const arrTime = times[1] || depTime;
                // Extract IATA codes (3-letter uppercase)
                const iatas = text.match(/\b([A-Z]{3})\b/g) || [];
                const uniqueIatas = [...new Set(iatas)].filter(c =>
                    !['THE','AND','FOR','ALL','ONE','WAY','NON','HRS','MIN','AED','USD','EUR','GBP'].includes(c)
                );
                const origin = uniqueIatas[0] || '';
                const dest = uniqueIatas[1] || '';
                // Extract price
                const priceMatch = text.match(/(AED|USD|EUR|GBP)\s*([\d,]+(?:\.\d{2})?)/i);
                if (!priceMatch) continue;
                const price = parseFloat(priceMatch[2].replace(/,/g, ''));
                if (price <= 20 || price > 50000) continue;
                // Duration
                const durMatch = text.match(/(\d+)\s*hrs?\s*(\d+)\s*mins?/);
                const duration = durMatch ? parseInt(durMatch[1]) * 60 + parseInt(durMatch[2]) : 0;
                // Stops
                const isNonstop = /non.?stop/i.test(text);
                const stopsMatch = text.match(/(\d+)\s*stop/i);
                const stops = isNonstop ? 0 : (stopsMatch ? parseInt(stopsMatch[1]) : 0);
                // Flight number
                const fnMatch = text.match(/\b(EK\s*\d{2,4})\b/i);
                const flightNo = fnMatch ? fnMatch[1].replace(/\s/g, '').toUpperCase() : 'EK';
                // Aircraft
                const acMatch = text.match(/\b(A380|A350|A340|A330|B777|B787|77W|77L|388)\b/i);
                // Cabin
                const cabin = /business/i.test(text) ? 'business' : /first/i.test(text) ? 'first' : /premium/i.test(text) ? 'premium_economy' : 'economy';

                results.push({
                    flightNo, depTime, arrTime, dateStr: '', duration,
                    durationText: durMatch ? durMatch[0] : '',
                    nonstop: isNonstop, stops,
                    origin, originCity: '', destination: dest, destinationCity: '',
                    cabin, price, currency: priceMatch[1].toUpperCase(),
                    aircraft: acMatch ? acMatch[1] : '',
                });
            }
            return results.length > 0 ? results : null;
        }""")

        if css_flights:
            logger.info("Emirates: CSS-selector extraction got %d flights", len(css_flights))
            return css_flights

        # ── Strategy C: Body innerText line-walk (legacy fallback) ──
        flights = await page.evaluate(r"""() => {
            const body = document.body?.innerText || '';
            if (body.includes('no flight options')) return [];

            const rawLines = body.split('\n').map(l => l.trim()).filter(Boolean);
            const lines = [];
            for (let k = 0; k < rawLines.length; k++) {
                if (/^(AED|USD|EUR|GBP)$/i.test(rawLines[k]) && k + 1 < rawLines.length) {
                    let next = k + 1;
                    while (next < rawLines.length && !/[\d,.]+/.test(rawLines[next]) && next < k + 4) next++;
                    if (next < rawLines.length && /^[\d,.]+$/.test(rawLines[next])) {
                        lines.push(rawLines[k] + ' ' + rawLines[next]);
                        k = next;
                        continue;
                    }
                }
                lines.push(rawLines[k]);
            }

            const results = [];
            let i = 0;
            while (i < lines.length) {
                const timeMatch = lines[i].match(/^(\d{1,2}:\d{2})$/);
                if (timeMatch) {
                    const depTime = timeMatch[1];
                    let dateStr = '';
                    if (i > 0 && /^\w{3}\s+\d{1,2}\s+\w{3}|^\w{3}\s+\w{3}\s+\d{1,2}/.test(lines[i-1])) {
                        dateStr = lines[i-1];
                    }
                    let j = i + 1;
                    while (j < lines.length && j < i + 3) {
                        if (/^\w{3}\s+\d{1,2}\s+\w{3}|^\w{3}\s+\w{3}\s+\d{1,2}/.test(lines[j])) { j++; continue; }
                        if (/^\d{1,2}:\d{2}$/.test(lines[j])) break;
                        j++;
                    }
                    if (j >= lines.length || j >= i + 3) { i++; continue; }
                    const arrTime = lines[j];
                    j++;
                    const durLine = lines[j] || '';
                    const durMatch = durLine.match(/(\d+)\s*hrs?\s*(\d+)\s*mins?/);
                    j++;
                    const stopsLine = lines[j] || '';
                    const isNonstop = stopsLine.toLowerCase().includes('non-stop');
                    j++;
                    while (j < lines.length && /opens|dialog/i.test(lines[j])) j++;
                    const originIata = lines[j] || '';
                    j++;
                    const originCity = lines[j] || '';
                    j++;
                    const destIata = lines[j] || '';
                    j++;
                    const destCity = lines[j] || '';
                    j++;
                    const cabinLine = lines[j] || '';
                    j++;
                    let priceLine = '';
                    while (j < lines.length && j < i + 25) {
                        if (/AED|USD|EUR|GBP/i.test(lines[j])) { priceLine = lines[j]; break; }
                        j++;
                    }
                    const priceMatch = priceLine.match(/(AED|USD|EUR|GBP)\s*([\d,.]+)/i);
                    let flightNo = '';
                    for (let k = j; k < Math.min(j + 8, lines.length); k++) {
                        const fnm = lines[k].match(/^(EK\d{2,4})$/i);
                        if (fnm) { flightNo = fnm[1]; break; }
                    }
                    let aircraft = '';
                    for (let k = j; k < Math.min(j + 10, lines.length); k++) {
                        if (/^(A380|A350|A340|A330|A320|B777|B787|B737|77W|77L|388)$/i.test(lines[k])) {
                            aircraft = lines[k]; break;
                        }
                    }
                    if (priceMatch) {
                        results.push({
                            flightNo: (flightNo || 'EK').toUpperCase(),
                            depTime, arrTime, dateStr,
                            duration: durMatch ? parseInt(durMatch[1]) * 60 + parseInt(durMatch[2]) : 0,
                            durationText: durLine,
                            nonstop: isNonstop, stops: isNonstop ? 0 : 1,
                            origin: originIata.length === 3 ? originIata : '',
                            originCity,
                            destination: destIata.length === 3 ? destIata : '',
                            destinationCity: destCity,
                            cabin: cabinLine.toLowerCase().includes('business') ? 'business'
                                 : cabinLine.toLowerCase().includes('first') ? 'first'
                                 : cabinLine.toLowerCase().includes('premium') ? 'premium_economy'
                                 : 'economy',
                            price: parseFloat(priceMatch[2].replace(/,/g, '')),
                            currency: priceMatch[1].toUpperCase(),
                            aircraft,
                        });
                    }
                }
                i++;
            }
            return results;
        }""")
        count = len(flights) if flights else 0
        logger.info("Emirates: body-text line-walk got %d flights", count)
        if flights:
            return flights

        # ── Strategy D: Body-text price-only regex (absolute last resort) ──
        try:
            body_text = await page.inner_text("body")
        except Exception:
            body_text = ""

        if not body_text:
            return []

        matches = re.findall(
            r"\b(AED|USD|EUR|GBP)\s*([0-9][0-9,]{2,}(?:\.[0-9]{2})?)\b",
            body_text,
            flags=re.IGNORECASE,
        )
        seen = set()
        fallback = []
        for cur, amt_txt in matches:
            try:
                amt = float(amt_txt.replace(",", ""))
            except Exception:
                continue
            if amt <= 20 or amt > 50000:
                continue
            key = (cur.upper(), round(amt, 2))
            if key in seen:
                continue
            seen.add(key)
            fallback.append({
                "flightNo": "EK",
                "depTime": "00:00",
                "arrTime": "00:00",
                "dateStr": "",
                "duration": 0,
                "durationText": "",
                "nonstop": False,
                "stops": 0,
                "origin": req.origin if req else "",
                "originCity": "",
                "destination": req.destination if req else "",
                "destinationCity": "",
                "cabin": "economy",
                "price": amt,
                "currency": cur.upper(),
                "aircraft": "",
                "inbound_depTime": "00:00",
                "inbound_arrTime": "00:00",
                "inbound_origin": req.destination if req and req.return_from is not None else "",
                "inbound_destination": req.origin if req and req.return_from is not None else "",
            })
            if len(fallback) >= 8:
                break

        if fallback:
            logger.warning("Emirates: body-text fare fallback extracted %d offers", len(fallback))
        return fallback

    # ------------------------------------------------------------------
    # Flexi-fares fallback
    # ------------------------------------------------------------------

    def _parse_flexi_fares(self, data: dict, req: FlightSearchRequest) -> list[dict]:
        """Parse flexi-fares API response as fallback when DOM is empty."""
        try:
            dt = req.date_from if isinstance(req.date_from, (datetime, date)) else datetime.strptime(str(req.date_from), "%Y-%m-%d")
        except (ValueError, TypeError):
            return []

        target_date = dt.strftime("%Y-%m-%d")
        target_return = None
        if req.return_from is not None:
            try:
                rt = req.return_from if isinstance(req.return_from, (datetime, date)) else datetime.strptime(str(req.return_from), "%Y-%m-%d")
                target_return = rt.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                target_return = None

        currency = data.get("currency", {}).get("sale", {}).get("code", "AED")
        options = data.get("options", [])
        results = []

        def _first_non_empty(*vals):
            for v in vals:
                if v not in (None, "", []):
                    return v
            return None

        def _extract_amount(option: dict) -> float:
            candidates = [
                option.get("priceSummary", {}).get("total", {}).get("amount"),
                option.get("price", {}).get("total", {}).get("amount"),
                option.get("total", {}).get("amount"),
                option.get("amount"),
            ]
            for c in candidates:
                try:
                    if c is not None:
                        return float(c)
                except Exception:
                    continue
            return 0.0

        for opt in options:
            outbound = opt.get("outbound", {}) if isinstance(opt.get("outbound"), dict) else {}
            inbound = opt.get("inbound", {}) if isinstance(opt.get("inbound"), dict) else {}

            travel_date = _first_non_empty(
                outbound.get("travelDate"),
                outbound.get("departureDate"),
                opt.get("departureDate"),
            ) or ""
            if not str(travel_date).startswith(target_date):
                continue

            if target_return is not None:
                return_date = _first_non_empty(
                    inbound.get("travelDate"),
                    inbound.get("departureDate"),
                    opt.get("returnDate"),
                ) or ""
                if return_date and not str(return_date).startswith(target_return):
                    continue

            total = _extract_amount(opt)
            if total <= 0:
                continue

            dep_time = str(travel_date)[11:16] if len(str(travel_date)) >= 16 else "00:00"
            ret_time_src = _first_non_empty(inbound.get("travelDate"), inbound.get("departureDate")) or ""
            ret_time = str(ret_time_src)[11:16] if len(str(ret_time_src)) >= 16 else "00:00"

            out_origin = _first_non_empty(outbound.get("departure"), outbound.get("origin"), req.origin) or req.origin
            out_dest = _first_non_empty(outbound.get("arrival"), outbound.get("destination"), req.destination) or req.destination
            in_origin = _first_non_empty(inbound.get("departure"), inbound.get("origin"), req.destination) or req.destination
            in_dest = _first_non_empty(inbound.get("arrival"), inbound.get("destination"), req.origin) or req.origin

            results.append({
                "flightNo": "EK",
                "depTime": dep_time,
                "arrTime": dep_time,
                "dateStr": "",
                "duration": 0,
                "durationText": "",
                "nonstop": True,
                "stops": 0,
                "origin": out_origin,
                "originCity": "",
                "destination": out_dest,
                "destinationCity": "",
                "cabin": "economy",
                "price": float(total),
                "currency": currency,
                "aircraft": "",
                "inbound_depTime": ret_time,
                "inbound_arrTime": ret_time,
                "inbound_origin": in_origin,
                "inbound_destination": in_dest,
            })

        return results

    # ------------------------------------------------------------------
    # Offer construction
    # ------------------------------------------------------------------

    def _build_offer(self, flight: dict, req: FlightSearchRequest) -> Optional[FlightOffer]:
        """Build a FlightOffer from scraped flight data."""
        try:
            dt = req.date_from if isinstance(req.date_from, (datetime, date)) else datetime.strptime(str(req.date_from), "%Y-%m-%d")
            dep_date = dt if isinstance(dt, date) and not isinstance(dt, datetime) else dt.date() if isinstance(dt, datetime) else dt
        except (ValueError, TypeError):
            dep_date = date.today()

        dep_time = flight.get("depTime", "00:00")
        arr_time = flight.get("arrTime", "00:00")

        try:
            hm_dep = dep_time.split(":")
            dep_dt = datetime(dep_date.year, dep_date.month, dep_date.day,
                              int(hm_dep[0]), int(hm_dep[1]))
        except (ValueError, IndexError):
            dep_dt = datetime(dep_date.year, dep_date.month, dep_date.day)

        try:
            hm_arr = arr_time.split(":")
            arr_dt = datetime(dep_date.year, dep_date.month, dep_date.day,
                              int(hm_arr[0]), int(hm_arr[1]))
            # Handle overnight flights (skip if both times are midnight — truly unknown)
            if arr_dt <= dep_dt and not (dep_time == "00:00" and arr_time == "00:00"):
                from datetime import timedelta
                arr_dt += timedelta(days=1)
        except (ValueError, IndexError):
            arr_dt = dep_dt

        try:
            duration_min = int(float(flight.get("duration", 0) or 0))
        except (TypeError, ValueError):
            duration_min = 0
        if duration_min <= 0 and arr_dt > dep_dt:
            duration_min = int((arr_dt - dep_dt).total_seconds() // 60)
        flight_no = flight.get("flightNo", "EK")
        origin = flight.get("origin", "") or req.origin
        destination = flight.get("destination", "") or req.destination
        price = flight.get("price", 0)
        currency = flight.get("currency", "AED")

        if price <= 0 or duration_min <= 0:
            return None

        offer_id = hashlib.md5(
            f"ek_{origin}_{destination}_{dep_date}_{flight_no}_{price}_{req.return_from or ''}".encode()
        ).hexdigest()[:12]

        segment = FlightSegment(
            airline="EK",
            airline_name="Emirates",
            flight_no=flight_no,
            origin=origin,
            destination=destination,
            origin_city=flight.get("originCity", ""),
            destination_city=flight.get("destinationCity", ""),
            departure=dep_dt,
            arrival=arr_dt,
            duration_seconds=duration_min * 60,
            cabin_class=flight.get("cabin", "economy"),
            aircraft=flight.get("aircraft", ""),
        )

        route = FlightRoute(
            segments=[segment],
            total_duration_seconds=duration_min * 60,
            stopovers=flight.get("stops", 0),
        )

        inbound_route = None
        if req.return_from is not None and flight.get("inbound_origin") and flight.get("inbound_destination"):
            try:
                ret_dt = req.return_from if isinstance(req.return_from, (datetime, date)) else datetime.strptime(str(req.return_from), "%Y-%m-%d")
                ret_date = ret_dt if isinstance(ret_dt, date) and not isinstance(ret_dt, datetime) else ret_dt.date()
            except (ValueError, TypeError):
                ret_date = dep_date

            ret_dep = flight.get("inbound_depTime", "00:00")
            ret_arr = flight.get("inbound_arrTime", "00:00")
            try:
                rh = ret_dep.split(":")
                ret_dep_dt = datetime(ret_date.year, ret_date.month, ret_date.day, int(rh[0]), int(rh[1]))
            except Exception:
                ret_dep_dt = datetime(ret_date.year, ret_date.month, ret_date.day)

            try:
                ah = ret_arr.split(":")
                ret_arr_dt = datetime(ret_date.year, ret_date.month, ret_date.day, int(ah[0]), int(ah[1]))
                if ret_arr_dt <= ret_dep_dt and not (ret_dep == "00:00" and ret_arr == "00:00"):
                    from datetime import timedelta
                    ret_arr_dt += timedelta(days=1)
            except Exception:
                ret_arr_dt = ret_dep_dt
            inbound_duration_seconds = int((ret_arr_dt - ret_dep_dt).total_seconds())
            if inbound_duration_seconds <= 0:
                inbound_duration_seconds = duration_min * 60

            inbound_seg = FlightSegment(
                airline="EK",
                airline_name="Emirates",
                flight_no=flight.get("inbound_flightNo", flight_no),
                origin=flight.get("inbound_origin", destination),
                destination=flight.get("inbound_destination", origin),
                origin_city="",
                destination_city="",
                departure=ret_dep_dt,
                arrival=ret_arr_dt,
                duration_seconds=inbound_duration_seconds,
                cabin_class=flight.get("cabin", "economy"),
                aircraft="",
            )
            inbound_route = FlightRoute(segments=[inbound_seg], total_duration_seconds=inbound_duration_seconds, stopovers=0)

        return FlightOffer(
            id=f"ek_{offer_id}",
            price=price,
            currency=currency,
            price_formatted=f"{currency} {price:,.0f}",
            outbound=route,
            inbound=inbound_route,
            airlines=["Emirates"],
            owner_airline="EK",
            booking_url=self._booking_url(req),
            is_locked=False,
            source="emirates_direct",
            source_tier="free",
        )

    @staticmethod
    def _booking_url(req: FlightSearchRequest) -> str:
        """Build Emirates booking deep-link via base64 searchRequest."""
        import base64, json as _json
        try:
            dt = req.date_from if isinstance(req.date_from, (datetime, date)) else datetime.strptime(str(req.date_from), "%Y-%m-%d")
            date_str = dt.strftime("%Y-%m-%d") if hasattr(dt, 'strftime') else str(dt)
        except (ValueError, TypeError):
            date_str = ""
        is_rt = req.return_from is not None
        search_req = {
            "journeyType": "RETURN" if is_rt else "ONEWAY",
            "bookingType": "REVENUE",
            "passengers": [{"type": "ADT", "count": req.adults or 1}],
            "segments": [
                {"departure": req.origin, "arrival": req.destination, "departureDate": date_str}
            ],
        }
        if is_rt:
            try:
                ret_dt = req.return_from if isinstance(req.return_from, (datetime, date)) else datetime.strptime(str(req.return_from), "%Y-%m-%d")
                ret_str = ret_dt.strftime("%Y-%m-%d") if hasattr(ret_dt, 'strftime') else str(ret_dt)
            except (ValueError, TypeError):
                ret_str = ""
            search_req["segments"].append(
                {"departure": req.destination, "arrival": req.origin, "departureDate": ret_str}
            )
        if req.children:
            search_req["passengers"].append({"type": "CHD", "count": req.children})
        if req.infants:
            search_req["passengers"].append({"type": "INF", "count": req.infants})
        encoded = base64.b64encode(_json.dumps(search_req).encode()).decode().rstrip("=")
        return f"https://www.emirates.com/booking/search-results/?searchRequest={encoded}"

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"emirates{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency="AED",
            offers=[],
            total_results=0,
        )
