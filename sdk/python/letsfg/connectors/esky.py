"""
eSky connector — Playwright browser (Chrome) + API interception.

eSky (esky.pl / esky.com) is a leading Polish OTA aggregator with strong
presence in CEE (Poland, Czech Republic, Romania, Hungary, Bulgaria) and LatAm.
Aggregates fares from 700+ airlines.

Strategy:
  1. Launch Playwright with Chrome channel (not Chromium) — critical for bypassing
     eSky's bot protection (reCAPTCHA + Akamai).
  2. Navigate to eSky homepage, fill and submit search form.
  3. Intercept API response from flightsapi.esky.com/gateway/v1/flights/queries/{id}
  4. Parse the `blocks` array which contains flight offers.

Verified working: April 2026.
Key findings from probing:
  - Direct URL navigation triggers "turbulencje" error page (reCAPTCHA failure).
  - Form submission on homepage creates a valid search session.
  - API endpoint: flightsapi.esky.com/gateway/v1/flights/queries/{queryId}
  - Response structure: blocks[] with legGroups[], priceDetails, dictionaries.
  - Must use real Chrome (channel='chrome'), not Chromium.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time
from datetime import date, datetime, timedelta
from typing import Any

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import auto_block_if_proxied

logger = logging.getLogger(__name__)

_ancillary_cache: dict[str, tuple[float, dict]] = {}
_ANCILLARY_CACHE_TTL = 1800  # 30 min

_CABIN_MAP = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}


class EskyConnectorClient:
    """eSky.com — Polish OTA aggregator, Playwright + API interception."""

    def __init__(self, timeout: float = 55.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        ob_result = await self._search_ow(req)
        if req.return_from and ob_result.total_results > 0:
            ib_req = req.model_copy(update={
                "origin": req.destination, "destination": req.origin,
                "date_from": req.return_from, "return_from": None,
            })
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
                    self._fetch_ancillaries(anc_origin, anc_dest, req.date_from.isoformat(), req.adults, ob_result.currency),
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
        return {
            "checked_bag": "not included – add-on from ~35 € at eDreams checkout (OTA markup; book airline direct for lower fees)",
            "bags_note": "personal item free; cabin bag add-on from ~30 € at eDreams checkout",
            "seat_note": "seat add-on from ~10 € at eDreams checkout; skip for random seat",
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
                offer.conditions["cabin_bag"] = bags_note
            if checked_note:
                offer.conditions.setdefault("checked_bag", checked_note)
            if seat_note:
                offer.conditions["seat"] = seat_note
            if bags_from is not None and offer.currency.upper() == anc_currency.upper():
                offer.bags_price["cabin_bag"] = bags_from
            if checked_from is not None and offer.currency.upper() == anc_currency.upper():
                offer.bags_price["checked_bag"] = checked_from

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        try:
            dt = (
                req.date_from
                if isinstance(req.date_from, (datetime, date))
                else datetime.strptime(str(req.date_from), "%Y-%m-%d")
            )
            if isinstance(dt, datetime):
                dt = dt.date()
        except (ValueError, TypeError):
            dt = date.today() + timedelta(days=30)

        date_str = dt.strftime("%Y-%m-%d")

        for attempt in range(2):
            try:
                offers = await self._do_search(req, date_str)
                if offers is not None:
                    offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
                    elapsed = time.monotonic() - t0
                    logger.info("eSky %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)
                    h = hashlib.md5(
                        f"esky{req.origin}{req.destination}{date_str}{req.return_from or ''}".encode()
                    ).hexdigest()[:12]
                    return FlightSearchResponse(
                        search_id=f"fs_{h}",
                        origin=req.origin,
                        destination=req.destination,
                        currency=offers[0].currency if offers else "EUR",
                        offers=offers,
                        total_results=len(offers),
                    )
            except Exception as e:
                logger.warning("eSky attempt %d failed: %s", attempt, e)

        return self._empty(req)

    async def _do_search(self, req: FlightSearchRequest, date_str: str) -> list[FlightOffer] | None:
        from playwright.async_api import async_playwright

        flight_data: dict | None = None
        dictionaries: dict = {}

        async def on_response(response):
            nonlocal flight_data, dictionaries
            url = response.url
            try:
                if response.status != 200:
                    return
                # Capture eSky flight API response
                if "flightsapi.esky" in url and "queries" in url and "smallprice" not in url:
                    ct = response.headers.get("content-type", "")
                    if "json" in ct:
                        body = await response.text()
                        data = json.loads(body)
                        if "blocks" in data:
                            flight_data = data
                            dictionaries = data.get("dictionaries", {})
                            logger.debug("eSky: captured API response with %d blocks", len(data["blocks"]))
            except Exception:
                pass

        pw = await async_playwright().start()
        try:
            from .browser import get_proxy, patchright_bandwidth_args
            proxy = get_proxy("ESKY_PROXY")
            launch_kw: dict = {
                "headless": False,
                "channel": "chrome",  # Use real Chrome, not Chromium — critical!
                "args": [
                    "--window-position=-2400,-2400",
                    "--window-size=1366,768",
                    *patchright_bandwidth_args(),
                ],
            }
            if proxy:
                launch_kw["proxy"] = proxy

            browser = await pw.chromium.launch(**launch_kw)
            ctx = await browser.new_context(
                viewport={"width": 1366, "height": 768},
                locale="pl-PL",
            )
            page = await ctx.new_page()
            await auto_block_if_proxied(page)
            page.on("response", on_response)

            # Navigate to eSky.pl homepage
            logger.info("eSky: loading homepage")
            await page.goto("https://www.esky.pl/", wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(2000)

            # Dismiss Usercentrics cookie consent overlay (uses shadow DOM)
            # Must be done BEFORE any form interaction — it blocks pointer events
            for _uc_try in range(3):
                try:
                    removed = await page.evaluate("""() => {
                        const uc = document.getElementById('usercentrics-root');
                        if (uc) { uc.remove(); return true; }
                        return false;
                    }""")
                    if removed:
                        break
                except Exception:
                    pass
                await page.wait_for_timeout(1000)
            await page.wait_for_timeout(1000)

            # Select one-way trip
            try:
                await page.click('label:has-text("W jedną stronę")', timeout=3000)
                await page.wait_for_timeout(500)
            except Exception:
                pass

            # Fill departure airport
            dep = page.locator("#qsf-departure")
            await dep.clear()
            await dep.click()
            await dep.type(req.origin, delay=100)
            await page.wait_for_timeout(2000)
            await page.click('[role="option"]:first-child', timeout=3000)
            await page.wait_for_timeout(500)

            # Fill destination airport
            arr = page.locator("#qsf-arrival")
            await arr.clear()
            await arr.click()
            await arr.type(req.destination, delay=100)
            await page.wait_for_timeout(2000)
            await page.click('[role="option"]:first-child', timeout=3000)
            await page.wait_for_timeout(500)

            # Open date picker and select date
            await page.click("#dates_from")
            await page.wait_for_timeout(1000)

            # Parse the target date
            try:
                target_date = datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                target_date = datetime.now() + timedelta(days=30)
            day = target_date.day

            # Click on the day cell
            day_cell = page.locator(f'[class*="calendarDayCell"]:not([class*="disabled"]):has-text("{day}")').first
            await day_cell.click(timeout=3000)
            await page.wait_for_timeout(500)

            # Submit search
            logger.info("eSky: submitting search %s→%s on %s", req.origin, req.destination, date_str)
            await page.click('button:has-text("Szukaj lotu")')

            # Wait for navigation to search results
            try:
                await page.wait_for_url("**/flights/search/**", timeout=20000)
            except Exception:
                pass

            # Wait for API response
            for _ in range(15):
                await page.wait_for_timeout(2000)
                if flight_data:
                    break

            offers: list[FlightOffer] = []

            # Parse the captured API response
            if flight_data:
                currency = flight_data.get("currencyCode", "PLN")
                carriers = dictionaries.get("carriers", {})
                airports = dictionaries.get("airports", {})

                for block in flight_data.get("blocks", []):
                    offer = self._parse_block(block, req, date_str, currency, carriers, airports)
                    if offer:
                        offers.append(offer)

            # Fallback: DOM extraction
            if not offers:
                offers = await self._extract_from_dom(page, req, date_str)

            await page.close()
            await ctx.close()
            await browser.close()

            return offers if offers else None

        except Exception as e:
            logger.error("eSky browser error: %s", e)
            return None
        finally:
            try:
                await pw.stop()
            except Exception:
                pass

    def _parse_block(
        self,
        block: dict,
        req: FlightSearchRequest,
        date_str: str,
        currency: str,
        carriers: dict,
        airports: dict,
    ) -> FlightOffer | None:
        """Parse an eSky block (offer) from the API response."""
        if not isinstance(block, dict):
            return None

        # Extract price
        price_details = block.get("priceDetails", {})
        price = price_details.get("amount")
        if not price:
            return None
        try:
            price_f = round(float(price), 2)
        except (ValueError, TypeError):
            return None
        if price_f <= 0:
            return None

        # Extract flight segments from legGroups
        leg_groups = block.get("legGroups", [])
        if not leg_groups:
            return None

        ob_segments: list[FlightSegment] = []
        airlines_set: list[str] = []

        for leg_group in leg_groups:
            airline_codes = leg_group.get("airlineCodes", [])
            airline_code = airline_codes[0] if airline_codes else ""
            carrier_info = carriers.get(airline_code, {})
            airline_name = carrier_info.get("name", airline_code)

            if airline_name and airline_name not in airlines_set:
                airlines_set.append(airline_name)

            for leg in leg_group.get("legs", []):
                seg = self._parse_esky_leg(leg, airline_code, airline_name, cabin=_CABIN_MAP.get(req.cabin_class or "M", "economy"))
                if seg:
                    ob_segments.append(seg)

        if not ob_segments:
            return None

        total_dur = block.get("duration", 0) * 60  # duration in minutes
        if not total_dur:
            total_dur = sum(s.duration_seconds for s in ob_segments)
        stops = max(0, len(ob_segments) - 1)
        route = FlightRoute(segments=ob_segments, total_duration_seconds=total_dur, stopovers=stops)

        offer_id = block.get("offerId", block.get("id", ""))
        fid = hashlib.md5(f"esky_{offer_id}_{price_f}".encode()).hexdigest()[:12]
        booking_url = f"https://www.esky.pl/flights/select/{req.origin}-{req.destination}/{date_str}/1-0-0"

        return FlightOffer(
            id=f"esky_{fid}",
            price=price_f,
            currency=currency,
            price_formatted=f"{price_f:.2f} {currency}",
            outbound=route,
            inbound=None,
            airlines=airlines_set or ["eSky"],
            owner_airline=airlines_set[0] if airlines_set else "eSky",
            booking_url=booking_url,
            is_locked=False,
            source="esky_ota",
            source_tier="free",
        )

    def _parse_esky_leg(self, leg: dict, airline_code: str, airline_name: str, cabin: str = "economy") -> FlightSegment | None:
        """Parse an eSky leg into a FlightSegment."""
        if not isinstance(leg, dict):
            return None

        from_data = leg.get("from", {})
        to_data = leg.get("to", {})

        origin = from_data.get("code", "")
        destination = to_data.get("code", "")
        if not origin or not destination:
            return None

        dep_str = from_data.get("time", "")
        arr_str = to_data.get("time", "")
        dep_dt = self._parse_dt(dep_str)
        arr_dt = self._parse_dt(arr_str)

        duration_min = leg.get("duration", 0)
        dur_sec = duration_min * 60 if duration_min else 0

        return FlightSegment(
            airline=airline_code,
            airline_name=airline_name,
            flight_no="",
            origin=origin,
            destination=destination,
            departure=dep_dt,
            arrival=arr_dt,
            duration_seconds=dur_sec,
            cabin_class=cabin,
        )

    async def _extract_from_dom(self, page, req: FlightSearchRequest, date_str: str) -> list[FlightOffer]:
        """Scrape flight result cards from the eSky Angular SPA."""
        offers: list[FlightOffer] = []
        seen: set[str] = set()

        try:
            flight_data = await page.evaluate("""() => {
                const results = [];
                // eSky uses Angular components: so-fsr-flight-card
                const cards = document.querySelectorAll('so-fsr-flight-card');
                
                for (const card of cards) {
                    const text = card.textContent || '';
                    
                    // Extract price (Polish format: "405 zł" or "517,50 PLN")
                    const priceMatch = text.match(/(\\d[\\d\\s,]*\\.?\\d*)\\s*(PLN|EUR|USD|GBP|zł)/i);
                    if (!priceMatch) continue;
                    
                    const curMatch = text.match(/\\b(PLN|EUR|USD|GBP)\\b/i);
                    const currency = curMatch ? curMatch[1].toUpperCase() : (text.includes('zł') ? 'PLN' : 'EUR');
                    
                    // Extract times (HH:MM format)
                    const times = text.match(/\\b(\\d{1,2}:\\d{2})\\b/g) || [];
                    
                    // Extract airline name from the card
                    const lines = text.split('\\n').filter(l => l.trim());
                    let airline = '';
                    for (const line of lines) {
                        if (line.match(/\\b(Air|Airlines?|Ways?|Jet|Express|Wizz|Ryanair|Lufthansa|Austrian|LOT|KLM|easyJet)\\b/i)) {
                            airline = line.trim();
                            break;
                        }
                    }
                    
                    // Check for direct flight
                    const isDirect = text.toLowerCase().includes('bezpośredni') || text.toLowerCase().includes('direct');
                    const stopsMatch = text.match(/(\\d+)\\s*przesiad/i);
                    const stops = isDirect ? 0 : (stopsMatch ? parseInt(stopsMatch[1]) : 0);
                    
                    // Extract duration if shown (e.g., "03h 05min")
                    const durMatch = text.match(/(\\d+)h\\s*(\\d+)min/i);
                    const durationMin = durMatch ? parseInt(durMatch[1]) * 60 + parseInt(durMatch[2]) : 0;
                    
                    // Extract airports
                    const airportMatch = text.match(/\\b([A-Z]{3})\\b.*?\\b([A-Z]{3})\\b/);
                    
                    results.push({
                        price: priceMatch[1].replace(/[\\s,]/g, '.').replace(/\\.\\./g, '.'),
                        currency: currency,
                        depTime: times[0] || '',
                        arrTime: times[1] || '',
                        airline: airline,
                        durationMin: durationMin,
                        stops: stops,
                        origin: airportMatch ? airportMatch[1] : '',
                        destination: airportMatch ? airportMatch[2] : '',
                    });
                }
                return results;
            }""")
        except Exception:
            flight_data = []

        for fd in flight_data:
            try:
                price_f = round(float(fd["price"]), 2)
            except (ValueError, TypeError):
                continue
            if price_f <= 0 or price_f > 100000:
                continue

            currency = fd.get("currency", "EUR")
            airline_name = fd.get("airline", "").strip() or "eSky"
            dep_time = fd.get("depTime", "")
            arr_time = fd.get("arrTime", "")
            dur_min = fd.get("durationMin", 0)
            stops = fd.get("stops", 0) if fd.get("stops", -1) >= 0 else 0

            dep_dt = datetime.strptime(date_str, "%Y-%m-%d")
            arr_dt = dep_dt
            if dep_time:
                try:
                    h, m = map(int, dep_time.split(":"))
                    dep_dt = dep_dt.replace(hour=h, minute=m)
                except (ValueError, IndexError):
                    pass
            if arr_time:
                try:
                    h, m = map(int, arr_time.split(":"))
                    arr_dt = arr_dt.replace(hour=h, minute=m)
                except (ValueError, IndexError):
                    pass

            dur_sec = dur_min * 60 if dur_min else 0
            if not dur_sec and arr_dt > dep_dt:
                dur_sec = int((arr_dt - dep_dt).total_seconds())

            dedup_key = f"{req.origin}_{req.destination}_{date_str}_{price_f}_{dep_time}_{airline_name}"
            if dedup_key in seen:
                continue
            seen.add(dedup_key)

            _esky_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
            seg = FlightSegment(
                airline="", airline_name=airline_name,
                flight_no="",
                origin=fd.get("origin") or req.origin,
                destination=fd.get("destination") or req.destination,
                departure=dep_dt, arrival=arr_dt,
                duration_seconds=dur_sec, cabin_class=_esky_cabin,
            )
            route = FlightRoute(segments=[seg], total_duration_seconds=dur_sec, stopovers=stops)
            fid = hashlib.md5(f"esky_{dedup_key}".encode()).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"esky_{fid}", price=price_f, currency=currency,
                price_formatted=f"{price_f:.2f} {currency}",
                outbound=route, inbound=None,
                airlines=[airline_name],
                owner_airline=airline_name,
                booking_url=f"https://www.esky.com/flights/select/{req.origin}-{req.destination}/{date_str}/1-0-0",
                is_locked=False, source="esky_ota", source_tier="free",
            ))

        return offers

    @staticmethod
    def _parse_dt(val: Any) -> datetime:
        if isinstance(val, datetime):
            return val
        if not val:
            return datetime(2000, 1, 1)
        s = str(val)
        # Try ISO format variants with correct expected lengths
        formats = [
            ("%Y-%m-%dT%H:%M:%S", 19),
            ("%Y-%m-%dT%H:%M", 16),
            ("%Y-%m-%d %H:%M:%S", 19),
            ("%Y-%m-%d %H:%M", 16),
            ("%Y-%m-%d", 10),
        ]
        for fmt, length in formats:
            if len(s) >= length:
                try:
                    return datetime.strptime(s[:length], fmt)
                except ValueError:
                    continue
        return datetime(2000, 1, 1)

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
                    id=f"rt_esky_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"esky{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="EUR",
            offers=[],
            total_results=0,
        )
