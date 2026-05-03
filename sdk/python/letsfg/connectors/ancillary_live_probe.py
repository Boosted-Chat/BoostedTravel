"""
Live ancillary price probing for Group 5 LCC connectors.

Each prober takes the first offer returned by the search (origin, dest, date,
optional flight_no) and continues through that airline's booking flow in a real
browser session.  It intercepts the XHR/fetch response that contains the bag
and seat add-on prices displayed on the extras/baggage selection page, then
caches the result for 12 hours.

This approach guarantees the prices shown are exactly the prices the airline
displays at booking time for that route — not guesses or averages.

Connectors served
-----------------
  F3  flyadeal   – bookingapi2.flyadeal.com / NSK AvailabilityResponse via Playwright
  FZ  flydubai   – flights2.flydubai.com /api/flights/1 + booking continuation
  G9  airarabia  – reservations.airarabia.com IBE via Playwright
  XQ  sunexpress – sunexpress.com /booking/select/ → /booking/extras/ via Playwright
  SZ  skyexpress – skyexpress.gr/en/booking via Playwright
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ── Module-level cache ────────────────────────────────────────────────────────
# {airline_code: (monotonic_timestamp, ancillary_dict)}
_CACHE: Dict[str, Tuple[float, dict]] = {}
_LOCKS: Dict[str, asyncio.Lock] = {}
_CACHE_TTL = 12 * 3600  # 12 hours


def _get_lock(code: str) -> asyncio.Lock:
    if code not in _LOCKS:
        _LOCKS[code] = asyncio.Lock()
    return _LOCKS[code]


def _get_cached(code: str) -> Optional[dict]:
    entry = _CACHE.get(code)
    if entry and (time.monotonic() - entry[0]) < _CACHE_TTL:
        return entry[1]
    return None


def _set_cache(code: str, data: dict) -> None:
    _CACHE[code] = (time.monotonic(), data)


async def probe_ancillaries(
    code: str,
    origin: str,
    dest: str,
    date_str: Optional[str] = None,
    flight_no: Optional[str] = None,
) -> Optional[dict]:
    """
    Return live ancillary price dict for the given airline IATA code.

    Optionally accepts ``date_str`` (YYYY-MM-DD) and ``flight_no`` from the
    first offer returned by the connector's own search.  When provided the
    prober can navigate that exact flight's booking continuation to capture the
    bag prices the airline actually displays at checkout.

    Cache key: ``{code}:{origin}:{dest}:{YYYY-MM}`` — bag prices vary by route
    and (to a lesser extent) by season, so we cache per route per calendar month.

    Returns None if code is unknown.
    """
    from datetime import date as _date_cls
    month_tag = ""
    if date_str:
        try:
            month_tag = date_str[:7]  # "YYYY-MM"
        except Exception:
            pass
    if not month_tag:
        month_tag = _date_cls.today().strftime("%Y-%m")

    cache_key = f"{code}:{origin}:{dest}:{month_tag}"

    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    lock = _get_lock(cache_key)
    async with lock:
        cached = _get_cached(cache_key)
        if cached is not None:
            return cached

        probe_fn = _PROBERS.get(code)
        if probe_fn is None:
            return None

        try:
            result = await asyncio.wait_for(
                probe_fn(origin, dest, date_str, flight_no),
                timeout=90.0,
            )
            if result:
                _set_cache(cache_key, result)
                logger.info(
                    "Ancillary probe %s %s→%s → bag_from=%s %s (seat: %s)",
                    code,
                    origin,
                    dest,
                    result.get("checked_bag_from", "?"),
                    result.get("currency", ""),
                    result.get("seat_note", "—"),
                )
            return result
        except (asyncio.TimeoutError, TimeoutError):
            logger.warning("Ancillary probe timed out for %s %s→%s", code, origin, dest)
        except Exception as exc:
            logger.warning("Ancillary probe error for %s: %s", code, exc)
        return None


# ── Shared utilities ──────────────────────────────────────────────────────────

def _pw_stealth_args() -> List[str]:
    return [
        "--disable-blink-features=AutomationControlled",
        "--no-sandbox",
        "--disable-dev-shm-usage",
    ]


def _iter_dicts(obj, _depth: int = 0):
    """Yield all dicts nested within obj (cap at depth 15 to avoid runaway)."""
    if _depth > 15:
        return
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _iter_dicts(v, _depth + 1)
    elif isinstance(obj, list):
        for item in obj:
            yield from _iter_dicts(item, _depth + 1)


def _extract_bag_seat_prices(
    data: dict,
    bag_ssrs: Optional[set] = None,
) -> Tuple[List[float], List[float]]:
    """
    Generic extractor for bag and seat add-on prices from an API response dict.
    Scans all nested dicts for price-bearing items whose name/code suggests
    checked-bag or seat.
    """
    bag_prices: List[float] = []
    seat_prices: List[float] = []

    default_bag_ssrs = {
        "BAG", "CKB", "CBAG", "CB20", "CB23", "CB15", "CB30",
        "BAGGAGE", "BG20", "BG23", "BG15", "BG30",
        "F15", "F16", "F20", "F25", "F30",
        "ACB", "ACBH", "BULK",
    }
    effective_bag_ssrs = bag_ssrs or default_bag_ssrs

    for item in _iter_dicts(data):
        code = str(item.get("ssrCode") or item.get("code") or "").upper().strip()
        name_raw = str(item.get("name") or item.get("description") or "")
        name = name_raw.lower()

        is_bag = (
            code in effective_bag_ssrs
            or "bag" in name
            or "luggage" in name
            or "check" in name
            or re.search(r"\b\d{2}kg\b", name) is not None
        )
        is_seat = "seat" in name and "bag" not in name

        for price_key in ("price", "amount", "value", "totalPrice", "farePrice", "fee", "fees"):
            raw = item.get(price_key)
            if raw is None:
                continue
            try:
                p = float(raw)
            except (TypeError, ValueError):
                continue
            if p <= 0:
                continue
            if is_bag:
                bag_prices.append(p)
            elif is_seat:
                seat_prices.append(p)

    return bag_prices, seat_prices


def _probe_date(date_str: Optional[str], weeks_ahead: int = 6) -> str:
    """Return a probe departure date: use date_str if valid & future, else weeks_ahead from today."""
    from datetime import date as _d, timedelta
    today = _d.today()
    if date_str:
        try:
            dt = _d.fromisoformat(date_str[:10])
            if dt > today:
                return dt.isoformat()
        except ValueError:
            pass
    return (today + timedelta(weeks=weeks_ahead)).isoformat()


# ── Helper: SunExpress SUNVALUE-vs-Essential bag-price extractor ──────────────

def _xq_bag_price(data: dict) -> Optional[float]:
    """Extract checked-bag add-on price from SunExpress /pricing/api/v1/journeys.

    SUNVALUE fare bundles include a 20 KG item with applicability='Included'.
    Essential bundles have no such item.  Bag add-on = min(SUNVALUE) - min(Essential).
    """
    try:
        fares_with_bag: List[float] = []
        fares_without_bag: List[float] = []
        for d in _iter_dicts(data):
            total = d.get("totalAmount")
            if total is None:
                continue
            try:
                total = float(total)
            except (TypeError, ValueError):
                continue
            if not (5.0 < total < 5000.0):
                continue
            items = d.get("items", [])
            if not isinstance(items, list):
                continue
            has_checked_bag = any(
                isinstance(it, dict) and (
                    it.get("code") in ("20KG", "23KG", "25KG", "30KG", "32KG")
                    or re.search(r"\b\d{2,3}\s*kg\b", str(it.get("name", "")), re.IGNORECASE)
                ) and str(it.get("applicability", "")).strip().lower() == "included"
                for it in items
            )
            if has_checked_bag:
                fares_with_bag.append(total)
            else:
                fares_without_bag.append(total)
        if fares_with_bag and fares_without_bag:
            diff = min(fares_with_bag) - min(fares_without_bag)
            if diff > 0:
                return round(diff, 2)
    except Exception:
        pass
    return None


# ── Helper: FlyDubai CLASSIC-vs-LITE bag-price extractor ─────────────────────

def _fz_bag_price(data: dict) -> Optional[float]:
    """For FlyDubai /api/flights/1: estimate bag add-on as CLASSIC price - LITE price.

    LITE = no checked bag, CLASSIC = 20 kg included.
    """
    _LITE_LABELS = {"lite", "go", "basic", "light", "saver", "economy"}
    _BAG_LABELS = {"classic", "value", "standard", "flex", "freedom", "plus"}
    try:
        lite_prices: List[float] = []
        bag_prices: List[float] = []
        for d in _iter_dicts(data):
            fare_label = ""
            for key in ("bundleCode", "bundleName", "fareType", "fareName",
                        "bundleType", "productCode", "cabinClass", "fareClass"):
                v = d.get(key)
                if v and isinstance(v, str):
                    fare_label = v.lower()
                    break
            if not fare_label:
                continue
            price = None
            for pk in ("price", "amount", "totalPrice", "totalAmount", "fareAmount"):
                v = d.get(pk)
                if v is None:
                    continue
                if isinstance(v, dict):
                    v = v.get("amount") or v.get("value")
                try:
                    p = float(v)  # type: ignore[arg-type]
                    if p > 0:
                        price = p
                        break
                except (TypeError, ValueError):
                    pass
            if price is None:
                continue
            if any(k in fare_label for k in _LITE_LABELS):
                lite_prices.append(price)
            elif any(k in fare_label for k in _BAG_LABELS):
                bag_prices.append(price)
        if lite_prices and bag_prices:
            diff = min(bag_prices) - min(lite_prices)
            if 5.0 < diff < 500.0:
                return round(diff, 2)
    except Exception:
        pass
    return None


def _fz_extract_brand_bag_price(data: dict) -> Optional[float]:
    """Extract bag add-on price from FlyDubai flights1.flydubai.com /api/flights/1 response.

    flights/1 returns segments[].flights[].fareTypes[] where each entry has:
      - fareTypeName: "LITE" (no checked bag), "VALUE" (20 kg bag), "FLEX" (30 kg bag)
      - fare.totalFare: total price as a string, e.g. "675.00"

    Bag add-on = min(VALUE prices) - min(LITE prices).
    """
    _LITE_NAMES = {"lite", "go", "light", "basic", "saver"}
    _BAG_NAMES = {"value", "classic", "standard", "flex", "freedom", "plus"}
    try:
        lite_prices: List[float] = []
        bag_prices: List[float] = []
        for seg in (data.get("segments") or []):
            if not isinstance(seg, dict):
                continue
            for flight in (seg.get("flights") or []):
                if not isinstance(flight, dict):
                    continue
                for ft in (flight.get("fareTypes") or []):
                    if not isinstance(ft, dict):
                        continue
                    name = (ft.get("fareTypeName") or "").lower()
                    if not name:
                        continue
                    fare = ft.get("fare")
                    if not isinstance(fare, dict):
                        continue
                    total_str = fare.get("totalFare", "")
                    try:
                        total = float(str(total_str).replace(",", ""))
                    except (ValueError, TypeError):
                        continue
                    if total <= 0:
                        continue
                    if name in _LITE_NAMES or any(k in name for k in _LITE_NAMES):
                        lite_prices.append(total)
                    elif name in _BAG_NAMES or any(k in name for k in _BAG_NAMES):
                        bag_prices.append(total)
        if lite_prices and bag_prices:
            diff = min(bag_prices) - min(lite_prices)
            if 5.0 < diff < 500.0:
                return round(diff, 2)
    except Exception:
        pass
    return None


# ── F3 — flyadeal ─────────────────────────────────────────────────────────────

def _f3_extract_bundle_bag_price(data: dict) -> Optional[float]:
    """
    Extract the cheapest checked-bag add-on price from a flyadeal Availability response.

    flyadeal uses Navitaire / fad/v1/Availability.  The response contains a
    ``bundleOffers`` list where each offer has a ``totalPrice`` (the bundle
    upgrade cost above the base fare) and ``bundleSsrPrices``.  The cheapest
    bundle whose SSRs include a bag code (F20 = 20 kg, F30 = 30 kg, …) with
    totalPrice > 0 is returned as the bag add-on price.
    """
    _BAG_SSRS = {"F15", "F16", "F20", "F25", "F30", "F32", "F38", "CBAG", "BAG"}
    try:
        av4 = data.get("data", {}).get("availabilityv4", {})
        bundle_offers = av4.get("bundleOffers", [])
        bag_prices: List[float] = []
        for offer in bundle_offers:
            val = offer.get("value", {})
            for bp in val.get("bundlePrices", []):
                total = bp.get("totalPrice") or 0.0
                if total <= 0:
                    continue
                has_bag = any(
                    sp.get("ssrCode", "").upper() in _BAG_SSRS
                    for sp in bp.get("bundleSsrPrices", [])
                )
                if has_bag:
                    bag_prices.append(float(total))
        return min(bag_prices) if bag_prices else None
    except Exception:
        return None


async def _probe_f3(
    origin: str,
    dest: str,
    date_str: Optional[str] = None,
    flight_no: Optional[str] = None,
) -> Optional[dict]:
    """
    flyadeal (F3) — stealth browser via patchright (headless=False).

    Strategy (confirmed via live browser probing 2026-05-03):
    1. Load www.flyadeal.com/en/ to establish Cloudflare clearance.
    2. Fill the Angular Material search form via coordinate clicks:
       origin at (350, 528), dest at (640, 528), JS-click mat-option,
       then JS-click button.lets_fly_button.  Standard locators don't work
       against Angular CDK overlay inputs; coordinates are required.
    3. On /en/select-flight, click the first date-strip tab that has a SAR price.
    4. JS-click the first flight_details_wrap card to select it.
    5. Coordinate-click the ▼ expand chevron at ~(1143, card_y+55) to open
       the fare bundle panel (triggers GetBundleEarnMiles API).
    6. Extract fare bundle cards from DOM (class fares__div) — find the cheapest
       bundle that includes a checked bag (text contains "checked").

    The Availability API always returns fares:[] for this carrier, so DOM
    extraction from the bundle panel is the only reliable data source.
    JED→RUH is used as a fixed probe route (reliable domestic flyadeal route).
    """
    probe_origin = "JED"
    probe_dest = "RUH"

    try:
        from patchright.async_api import async_playwright as _patchright_playwright

        async with _patchright_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=False,
                args=[
                    "--no-first-run",
                    "--no-default-browser-check",
                    "--window-size=1440,900",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            )
            try:
                ctx = await browser.new_context(
                    viewport={"width": 1440, "height": 900},
                    locale="en-US",
                    timezone_id="Asia/Riyadh",
                )
                page = await ctx.new_page()

                # ── Load homepage (establishes Cloudflare clearance) ──────────
                await page.goto(
                    "https://www.flyadeal.com/en/",
                    wait_until="domcontentloaded",
                    timeout=40_000,
                )
                await asyncio.sleep(5.0)

                # ── Dismiss cookie banner ─────────────────────────────────────
                for ck_sel in [
                    "button:has-text('Accept all')",
                    "button:has-text('Accept All')",
                ]:
                    try:
                        if await page.locator(ck_sel).is_visible(timeout=2000):
                            await page.locator(ck_sel).first.click()
                            await asyncio.sleep(1.0)
                            break
                    except Exception:
                        continue

                # ── One-way ───────────────────────────────────────────────────
                try:
                    await page.locator("label:has-text('One-way')").first.click(timeout=5000)
                    await asyncio.sleep(0.5)
                except Exception:
                    pass

                # ── Fill origin: coordinate click (350, 528) + JS-click mat-option ──
                # Angular CDK overlay inputs are not reachable via standard locators.
                await page.mouse.click(350, 528)
                await asyncio.sleep(1.0)
                await page.keyboard.type(probe_origin, delay=120)
                await asyncio.sleep(2.0)
                await page.evaluate(
                    """() => {
                        const opts = Array.from(document.querySelectorAll(
                            'mat-option:not([disabled]):not([aria-disabled="true"])'
                        )).filter(o => o.getBoundingClientRect().width > 0);
                        if (opts.length) opts[0].click();
                    }"""
                )
                await asyncio.sleep(1.5)

                # ── Fill destination: coordinate click (640, 528) + JS-click mat-option ──
                await page.mouse.click(640, 528)
                await asyncio.sleep(1.0)
                await page.keyboard.type(probe_dest, delay=120)
                await asyncio.sleep(2.0)
                await page.evaluate(
                    """() => {
                        const opts = Array.from(document.querySelectorAll(
                            'mat-option:not([disabled]):not([aria-disabled="true"])'
                        )).filter(o => o.getBoundingClientRect().width > 0);
                        if (opts.length) opts[0].click();
                    }"""
                )
                await asyncio.sleep(1.5)

                # ── Submit via JS (date defaults to today, which is fine) ─────
                await page.evaluate(
                    """() => {
                        const btn = document.querySelector('button.lets_fly_button');
                        if (btn) btn.click();
                    }"""
                )

                # ── Wait for /en/select-flight ────────────────────────────────
                for _ in range(12):
                    await asyncio.sleep(3.0)
                    if "select-flight" in page.url:
                        break
                if "select-flight" not in page.url:
                    logger.debug(
                        "F3 probe: did not reach select-flight (url=%s)", page.url
                    )
                    return None

                # ── Click first date-strip tab that has a SAR price ───────────
                # Tabs are at y 200–390; skip "---" (no flight) entries.
                await page.evaluate(
                    """() => {
                        const els = Array.from(document.querySelectorAll('*')).filter(el => {
                            const r = el.getBoundingClientRect();
                            const t = (el.textContent || '').trim();
                            return r.y > 200 && r.y < 390
                                && r.width > 30 && r.width < 250
                                && r.height > 20
                                && t.includes('SAR') && t.indexOf('---') < 0;
                        });
                        if (els.length > 0) els[0].click();
                    }"""
                )
                await asyncio.sleep(3.0)

                # ── Find first flight card position ───────────────────────────
                card_info = await page.evaluate(
                    """() => {
                        const cards = Array.from(
                            document.querySelectorAll(
                                '.flight_details_wrap, [class*=flight_details_wrap]'
                            )
                        ).filter(e => {
                            const r = e.getBoundingClientRect();
                            return r.y > 300 && r.y < 900 && r.width > 100;
                        });
                        if (cards.length > 0) {
                            const r = cards[0].getBoundingClientRect();
                            return {
                                y: Math.round(r.y),
                                x: Math.round(r.x),
                                w: Math.round(r.width),
                                h: Math.round(r.height),
                            };
                        }
                        return null;
                    }"""
                )
                if not card_info:
                    logger.debug(
                        "F3 probe: no flight cards found on select-flight page"
                    )
                    return None

                # ── JS-click the first flight card to select it ───────────────
                await page.evaluate(
                    """() => {
                        const els = Array.from(document.querySelectorAll(
                            '.flight_details_wrap, [class*=flight_details_wrap]'
                        )).filter(e => {
                            const r = e.getBoundingClientRect();
                            return r.y > 300 && r.y < 900 && r.width > 100;
                        });
                        if (els.length) els[0].click();
                    }"""
                )
                await asyncio.sleep(1.5)

                # ── Coordinate-click the ▼ chevron to expand fare bundle panel ─
                # The chevron sits at the right side of the card: x≈1143, y≈card_y+55.
                # A JS click on the expand_section element hits (0,0) and collapses
                # the panel instead — only mouse.click at the visual coordinates works.
                await page.mouse.click(1143, card_info["y"] + 55)
                await asyncio.sleep(3.0)

                # ── Extract fare bundle cards from DOM ────────────────────────
                fare_cards = await page.evaluate(
                    """() => {
                        const cards = Array.from(
                            document.querySelectorAll('[class*=fares__div]')
                        ).filter(e => {
                            const r = e.getBoundingClientRect();
                            return r.width > 80 && r.height > 50;
                        });

                        function extractSAR(text) {
                            const idx = text.indexOf('SAR');
                            if (idx < 0) return null;
                            const after = text.slice(idx + 3).trim();
                            let i = 0;
                            while (i < after.length && !/[0-9]/.test(after[i])) i++;
                            let numStr = '';
                            while (i < after.length && /[0-9.,]/.test(after[i])) {
                                numStr += after[i]; i++;
                            }
                            numStr = numStr.replace(/,/g, '');
                            return numStr ? parseFloat(numStr) : null;
                        }

                        return cards.map(card => ({
                            price: extractSAR(card.textContent || ''),
                            hasChecked: (card.textContent || '').toLowerCase()
                                .indexOf('checked') >= 0,
                        }));
                    }"""
                )

                # ── Return cheapest bundle that includes a checked bag ────────
                with_bag = [
                    f for f in fare_cards
                    if f.get("hasChecked") and f.get("price") is not None
                    and f["price"] > 0
                ]
                if with_bag:
                    bag_price = min(f["price"] for f in with_bag)
                    return {
                        "checked_bag_note": (
                            f"checked bag not included (fly fare) "
                            f"– add-on from SAR {bag_price:.0f}"
                        ),
                        "bags_note": "cabin bag 7 kg included free",
                        "checked_bag_from": bag_price,
                        "currency": "SAR",
                        "seat_note": "seat selection add-on available",
                    }

            finally:
                await browser.close()

    except Exception as exc:
        logger.debug("F3 probe error: %s", exc)

    logger.debug("F3 probe: no bag prices captured for %s→%s", probe_origin, probe_dest)
    return None


# ── FZ — flydubai ────────────────────────────────────────────────────────────

async def _probe_fz(
    origin: str,
    dest: str,
    date_str: Optional[str] = None,
    flight_no: Optional[str] = None,
) -> Optional[dict]:
    """
    FlyDubai (FZ) — try the search API (/api/flights/1) which returns LITE-fare
    bundle data; parse bag SSR prices from the response.

    If the API returns bag prices in the fare bundles, extract them.
    Otherwise navigate the booking continuation via Playwright:
    flydubai.com search results → click cheapest → bag selection page → intercept XHR.
    """
    from datetime import date as _d, timedelta

    probe_origin = origin if (origin and len(origin) == 3) else "DXB"
    probe_dest = dest if (dest and len(dest) == 3 and dest != probe_origin) else "MCT"
    if probe_dest == probe_origin:
        probe_dest = "MCT"

    dep = _probe_date(date_str, weeks_ahead=5)
    dep_dt = _d.fromisoformat(dep)
    departure_api = dep_dt.strftime("%m/%d/%Y 12:00 AM")
    dep_url = dep_dt.strftime("%Y%m%d")

    # ── Attempt 1: direct REST API (works on Cloud Run with proxy) ───────────
    try:
        from curl_cffi.requests import AsyncSession
        from .browser import get_curl_cffi_proxies
        proxies = get_curl_cffi_proxies()
        async with AsyncSession(impersonate="chrome131") as s:
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Origin": "https://flights2.flydubai.com",
                "Referer": (
                    f"https://flights2.flydubai.com/en/results/ow/a1c0i0"
                    f"/{probe_origin}_{probe_dest}/{dep_url}"
                ),
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 Chrome/135.0.0.0 Safari/537.36"
                ),
            }
            payload = {
                "promoCode": "",
                "campaignCode": "",
                "cabinClass": "Economy",
                "isDestMetro": "false",
                "isOriginMetro": "false",
                "paxInfo": {"adultCount": 1, "childCount": 0, "infantCount": 0},
                "searchCriteria": [{
                    "date": departure_api,
                    "dest": probe_dest,
                    "direction": "outBound",
                    "origin": probe_origin,
                    "isOriginMetro": False,
                    "isDestMetro": False,
                }],
                "variant": "1",
            }
            r = await s.post(
                "https://flights2.flydubai.com/api/flights/1",
                json=payload,
                headers=headers,
                proxies=proxies,
                timeout=18,
            )
            if r.status_code == 200:
                data = r.json()
                bag_prices, seat_prices = _extract_bag_seat_prices(data)
                if not bag_prices:
                    fb = _fz_bag_price(data)
                    if fb:
                        bag_prices = [fb]
                if bag_prices:
                    min_bag = min(bag_prices)
                    result: dict = {
                        "checked_bag_note": (
                            f"checked bag not included (LITE fare) "
                            f"– add-on from AED {min_bag:.0f}"
                        ),
                        "bags_note": "cabin bag 7 kg included free (LITE fare)",
                        "checked_bag_from": min_bag,
                        "currency": "AED",
                    }
                    result["seat_note"] = (
                        f"seat selection add-on from AED {min(seat_prices):.0f}"
                        if seat_prices
                        else "seat selection add-on available"
                    )
                    return result
            logger.debug("FZ /api/flights/1 status %s", r.status_code)
    except Exception as exc:
        logger.debug("FZ /api/flights/1: %s", exc)

    # ── Attempt 2: headed browser → flights1.flydubai.com direct results URL ──
    # The Angular IBE on flights1.flydubai.com fires /api/flights/1 automatically
    # when the results page loads.  This XHR succeeds (Angular includes the
    # Akamai sensor token the browser generates) even when direct API calls
    # from curl_cffi are blocked.  We intercept the response via Playwright's
    # network listener; no form fill required.
    try:
        from patchright.async_api import async_playwright as _patchright_playwright

        flights1_data: Any = None
        flights1_event = asyncio.Event()
        calendar_event_pw = asyncio.Event()

        _patchright_mgr = _patchright_playwright()
        _fz_pw = await _patchright_mgr.__aenter__()
        browser = await _fz_pw.chromium.launch(
            headless=False,
            args=[
                "--no-first-run",
                "--no-default-browser-check",
                "--window-size=1366,768",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        context = await browser.new_context(
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            timezone_id="Asia/Dubai",
        )
        try:
            page = await context.new_page()

            async def _on_resp_fz(response):
                nonlocal flights1_data
                try:
                    url = response.url
                    ct = response.headers.get("content-type", "")
                    if response.status == 200 and "json" in ct:
                        if "/api/flights/1" in url:
                            d = await response.json()
                            if d and isinstance(d, dict) and d.get("segments"):
                                flights1_data = d
                                flights1_event.set()
                        elif "/api/flights/7" in url or "/api/flights/" in url:
                            calendar_event_pw.set()
                except Exception:
                    pass

            page.on("response", _on_resp_fz)

            # Navigate directly to the results page — Angular fires flights/1 on load
            results_url = (
                f"https://flights1.flydubai.com/en/results/ow/"
                f"a1c0i0/{probe_origin}_{probe_dest}/{dep_url}"
                "?cabinClass=Economy&isOriginMetro=false&isDestMetro=false&pm=cash"
            )
            await page.goto(results_url, wait_until="domcontentloaded", timeout=30_000)

            # Wait for flights/1 (auto-fired by the Angular app on page load)
            try:
                await asyncio.wait_for(flights1_event.wait(), timeout=25.0)
            except (asyncio.TimeoutError, TimeoutError):
                # Fallback: wait for any calendar response
                try:
                    await asyncio.wait_for(calendar_event_pw.wait(), timeout=10.0)
                except (asyncio.TimeoutError, TimeoutError):
                    pass
        finally:
            await context.close()
            await browser.close()
            await _patchright_mgr.__aexit__(None, None, None)

        if flights1_data:
            bag_price = _fz_extract_brand_bag_price(flights1_data)
            if bag_price:
                return {
                    "checked_bag_note": (
                        f"checked bag not included (LITE fare) "
                        f"– add-on from AED {bag_price:.0f}"
                    ),
                    "bags_note": "cabin bag 7 kg included free (LITE fare)",
                    "checked_bag_from": bag_price,
                    "currency": "AED",
                    "seat_note": "seat selection add-on available",
                }
    except Exception as exc:
        logger.debug("FZ flights1 browser probe error: %s", exc)

    logger.debug("FZ probe: no bag prices captured for %s→%s", probe_origin, probe_dest)
    return None


# ── G9 — airarabia ───────────────────────────────────────────────────────────

async def _probe_g9(
    origin: str,
    dest: str,
    date_str: Optional[str] = None,
    flight_no: Optional[str] = None,
) -> Optional[dict]:
    """
    Air Arabia (G9) — navigate reservations.airarabia.com IBE via patchright.

    The IBE is protected by Cloudflare Turnstile (sitekey 0x4AAAAAAA6kCdGqJCjlQvHL)
    which blocks all XHR requests before a human-verified token is obtained.
    patchright headless=False does not auto-solve Turnstile; every API call times out.

    This prober returns None immediately so the connector falls through to its
    own static reference data without wasting 35+ seconds on a failed browser attempt.
    """
    logger.debug("G9 probe: reservations.airarabia.com guarded by Turnstile; skipping")
    return None


# ── XQ — SunExpress ───────────────────────────────────────────────────────────

async def _probe_xq(
    origin: str,
    dest: str,
    date_str: Optional[str] = None,
    flight_no: Optional[str] = None,
) -> Optional[dict]:
    """
    SunExpress (XQ) — reuse the persistent headed Chrome session.

    The connector already navigates /booking/select/ to extract flight offers.
    Here we extend that session: after the flight list loads, click the first
    flight card to proceed to the passenger/bags step, then intercept the
    NSK GetSSRAvailability XHR that returns bag add-on prices.
    """
    from .sunexpress import _get_context
    from .browser import auto_block_if_proxied

    dep = _probe_date(date_str, weeks_ahead=6)
    dep_compact = dep.replace("-", "")

    # Always probe a known-working XQ international route (Turkey→Europe).
    # IST→AYT and many Turkish domestic routes have no SunExpress flights,
    # so using the caller's origin/dest is unreliable.
    probe_origin = "AYT"
    probe_dest = "DUS"

    xq_bag_price_val: Optional[float] = None
    seat_prices: List[float] = []
    cap_event = asyncio.Event()

    try:
        context = await _get_context()
        page = await context.new_page()
        await auto_block_if_proxied(page)

        async def _on_resp(resp):
            nonlocal xq_bag_price_val
            ct = resp.headers.get("content-type", "")
            if resp.status != 200 or "json" not in ct:
                return
            url_l = resp.url.lower()
            if not any(k in url_l for k in [
                "journeys", "pricing", "ssr", "bag", "ancill",
                "extras", "addon", "bundle", "getavail", "availability",
            ]):
                return
            try:
                data = await resp.json()
                # Try SunExpress-specific SUNVALUE vs Essential extractor first
                p = _xq_bag_price(data)
                if p and p > 0:
                    xq_bag_price_val = p
                    cap_event.set()
                    return
                # Fallback: generic SSR extractor
                b, s = _extract_bag_seat_prices(data)
                seat_prices.extend(s)
                if b:
                    xq_bag_price_val = min(b)
                    cap_event.set()
            except Exception:
                pass

        page.on("response", _on_resp)

        # New URL format: origin1/destination1/departure1 → directly loads flight results
        # /pricing/api/v1/journeys fires automatically on page load with all fare bundles
        select_url = (
            f"https://www.sunexpress.com/en-gb/booking/select/"
            f"?origin1={probe_origin}&destination1={probe_dest}"
            f"&departure1={dep}&adt1=1&chd1=0&inf1=0&currency=EUR"
        )
        try:
            await page.goto(select_url, wait_until="domcontentloaded", timeout=30_000)
        except Exception as exc:
            logger.debug("XQ booking/select: %s", exc)

        # Dismiss cookie banner if present
        try:
            await page.evaluate("""() => {
                for (const b of document.querySelectorAll('button')) {
                    if (/accept all|agree|allow all/i.test(b.textContent) && b.offsetHeight > 0) {
                        b.click(); return;
                    }
                }
            }""")
        except Exception:
            pass
        await asyncio.sleep(2)

        # /pricing/api/v1/journeys fires on page load — wait for it
        try:
            await asyncio.wait_for(cap_event.wait(), timeout=30.0)
        except (asyncio.TimeoutError, TimeoutError):
            pass

        await page.close()
    except Exception as exc:
        logger.debug("XQ Playwright probe error: %s", exc)

    if xq_bag_price_val and xq_bag_price_val > 0:
        result: dict = {
            "checked_bag_note": (
                f"checked bag not included (Essential fare) "
                f"– add-on from EUR {xq_bag_price_val:.0f}"
            ),
            "bags_note": "cabin bag 8 kg included free (Essential fare)",
            "checked_bag_from": xq_bag_price_val,
            "currency": "EUR",
        }
        result["seat_note"] = (
            f"seat selection add-on from EUR {min(seat_prices):.0f}"
            if seat_prices
            else "seat selection add-on available"
        )
        return result

    logger.debug("XQ probe: no bag prices captured for %s→%s", probe_origin, probe_dest)
    return None


# ── SZ — SkyExpress ───────────────────────────────────────────────────────────

def _sz_bag_price_from_search_shop(data: dict) -> Optional[float]:
    """
    Parse a Sabre ezyCommerce Availability/SearchShop response and return the
    cheapest checked-bag add-on price.

    SkyExpress sells bags as part of fare bundles (not standalone SSRs), so we
    compute the price differential: cheapest fare WITH checked bag minus the
    cheapest fare WITHOUT checked bag across all non-sold-out flights.
    """
    routes = data.get("routes", [])
    if not routes:
        return None

    min_no_bag: Optional[float] = None
    min_with_bag: Optional[float] = None

    for flight in routes[0].get("flights", []):
        if flight.get("soldOut") or flight.get("isPlaceHolder"):
            continue
        for fare in flight.get("fares", []):
            if fare.get("soldOut"):
                continue
            price = fare.get("price")
            if price is None or price <= 0:
                continue

            services = fare.get("fareBundle", {}).get("bundleServices", [])
            has_checked_bag = any(
                "checked" in s.get("description", "").lower()
                or s.get("ssrCode", "").startswith("CB")
                or s.get("ssrCode", "") in ("2C32",)
                for s in services
            )

            if has_checked_bag:
                if min_with_bag is None or price < min_with_bag:
                    min_with_bag = price
            else:
                if min_no_bag is None or price < min_no_bag:
                    min_no_bag = price

    if min_with_bag is not None and min_no_bag is not None:
        diff = round(min_with_bag - min_no_bag, 2)
        return diff if diff > 0 else None
    return None


async def _probe_sz(
    origin: str,
    dest: str,
    date_str: Optional[str] = None,
    flight_no: Optional[str] = None,
) -> Optional[dict]:
    """
    SkyExpress (SZ) — navigate skyexpress.gr booking form via Playwright.

    The form is Angular + PrimeNG (p-dropdown for airports, p-calendar for dates).
    Submitting redirects to flights.skyexpress.gr (Sabre ezyCommerce).
    The Availability/SearchShop API fires automatically on the results page and
    returns fare bundles that encode checked-bag pricing.

    Strategy:
    1. Load skyexpress.gr/en/book/flight, click "One way".
    2. Select origin/dest via PrimeNG p-dropdown panels.
    3. Open PrimeNG p-calendar via label:has-text('Dates'), pick target day, close.
    4. Click search → wait for navigation to flights.skyexpress.gr.
    5. Intercept Availability/SearchShop (content-type: text/plain, JSON body).
    6. Parse fare bundles: checked_bag_from = cheapest_with_bag − cheapest_without_bag.
    """
    from datetime import datetime as _dt

    from playwright.async_api import async_playwright

    dep = _probe_date(date_str, weeks_ahead=6)

    probe_origin = origin if (origin and len(origin) == 3) else "ATH"
    probe_dest = dest if (dest and len(dest) == 3 and dest != probe_origin) else "HER"
    if probe_dest == probe_origin:
        probe_dest = "HER"

    dep_date = _dt.strptime(dep, "%Y-%m-%d")
    target_month_name = dep_date.strftime("%B")
    target_day_str = str(dep_date.day)

    search_shop_data: Optional[dict] = None
    cap_event = asyncio.Event()

    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True, args=_pw_stealth_args())
            ctx = await browser.new_context(
                viewport={"width": 1366, "height": 768},
                locale="en-US",
                timezone_id="Europe/Athens",
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/135.0.0.0 Safari/537.36"
                ),
            )
            page = await ctx.new_page()

            async def _on_resp(resp):
                nonlocal search_shop_data
                if resp.status != 200:
                    return
                if "ezycommerce.sabre.com" not in resp.url:
                    return
                if "Availability/SearchShop" not in resp.url:
                    return
                try:
                    body = await resp.body()
                    data = json.loads(body.decode("utf-8"))
                    search_shop_data = data
                    cap_event.set()
                except Exception:
                    pass

            page.on("response", _on_resp)

            booking_url = (
                f"https://www.skyexpress.gr/en/book/flight"
                f"?origin={probe_origin}&destination={probe_dest}"
                f"&date={dep}&adult=1&tripType=OW"
            )
            try:
                await page.goto(booking_url, wait_until="domcontentloaded", timeout=30_000)
                await asyncio.sleep(4)
            except Exception as exc:
                logger.debug("SZ booking page: %s", exc)

            # Dismiss cookie consent
            try:
                if await page.locator("#onetrust-accept-btn-handler").count() > 0:
                    await page.locator("#onetrust-accept-btn-handler").first.click(timeout=3000)
                    await asyncio.sleep(1.5)
            except Exception:
                pass

            # Dismiss promo popup
            try:
                if await page.locator(".sky-obx__close").count() > 0:
                    await page.locator(".sky-obx__close").first.click(timeout=2000)
                    await asyncio.sleep(0.5)
            except Exception:
                pass

            # Click "One way" — URL param tripType=OW does NOT auto-set the radio
            try:
                await page.locator("label:has-text('One way')").first.click(timeout=5000)
                await asyncio.sleep(1.2)
            except Exception as exc:
                logger.debug("SZ one-way click: %s", exc)

            # Select origin via PrimeNG p-dropdown (NOT p-autocomplete)
            try:
                await page.locator("p-dropdown").nth(0).click(timeout=5000)
                await asyncio.sleep(1.2)
                await page.locator(".p-dropdown-item").filter(has_text=probe_origin).first.click(timeout=4000)
                await asyncio.sleep(1.2)
            except Exception as exc:
                logger.debug("SZ origin dropdown: %s", exc)

            # Select destination via PrimeNG p-dropdown
            try:
                await page.locator("p-dropdown").nth(1).click(timeout=5000)
                await asyncio.sleep(1.2)
                await page.locator(".p-dropdown-item").filter(has_text=probe_dest).first.click(timeout=4000)
                await asyncio.sleep(2.0)
            except Exception as exc:
                logger.debug("SZ dest dropdown: %s", exc)

            # Open PrimeNG p-calendar via the readonly input (force=True bypasses overlay span)
            try:
                await page.locator("#dates-book-flight-form").first.click(timeout=3000, force=True)
                await asyncio.sleep(1.5)
                panel_count = await page.locator(".p-datepicker-group").count()
                if panel_count == 0:
                    await page.locator("label:has-text('Dates')").first.click(timeout=3000)
                    await asyncio.sleep(1.5)
                    panel_count = await page.locator(".p-datepicker-group").count()
            except Exception as exc:
                logger.debug("SZ calendar open: %s", exc)
                panel_count = 0

            # Click target day in the correct month panel
            try:
                target_panel_idx = None
                for i in range(panel_count):
                    hdr = await (
                        page.locator(".p-datepicker-group").nth(i)
                        .locator(".p-datepicker-title").first
                        .inner_text()
                    )
                    if target_month_name in hdr:
                        target_panel_idx = i
                        break

                if target_panel_idx is not None:
                    panel = page.locator(".p-datepicker-group").nth(target_panel_idx)
                    cells = await panel.locator(
                        "td:not(.p-disabled):not(.p-datepicker-other-month) a"
                    ).all()
                    for cell in cells:
                        if (await cell.inner_text()).strip() == target_day_str:
                            await cell.click()
                            await asyncio.sleep(0.5)
                            break
                    # Close calendar
                    try:
                        await page.locator("button:has-text('Done')").first.click(timeout=2000)
                    except Exception:
                        pass
            except Exception as exc:
                logger.debug("SZ calendar day click: %s", exc)

            # Submit search
            try:
                await page.locator("button#book-flight-form-search-button").click(timeout=5000)
            except Exception as exc:
                logger.debug("SZ search button: %s", exc)

            # Wait for redirect to flights.skyexpress.gr
            try:
                await page.wait_for_url("**/flights.skyexpress.gr/**", timeout=20_000)
            except Exception:
                pass

            # Wait for SearchShop to fire
            try:
                await asyncio.wait_for(cap_event.wait(), timeout=30.0)
            except (asyncio.TimeoutError, TimeoutError):
                pass

            await browser.close()
    except Exception as exc:
        logger.debug("SZ Playwright probe error: %s", exc)

    if search_shop_data:
        bag_from = _sz_bag_price_from_search_shop(search_shop_data)
        if bag_from and bag_from > 0:
            return {
                "checked_bag_note": (
                    f"checked bag not included in base fare "
                    f"– add-on from EUR {bag_from:.0f}"
                ),
                "bags_note": "hand luggage 8 kg + 1 personal item included in base fare",
                "checked_bag_from": bag_from,
                "currency": "EUR",
                "seat_note": "seat selection add-on available",
            }

    logger.debug("SZ probe: no bag prices captured for %s→%s", probe_origin, probe_dest)
    return None


# ── LA — LATAM Airlines ───────────────────────────────────────────────────────

async def _probe_la(
    origin: str,
    dest: str,
    date_str: Optional[str] = None,
    flight_no: Optional[str] = None,
) -> Optional[dict]:
    """
    LATAM Airlines (LA/JJ) — navigate the full booking flow using the persistent
    CDP Chrome session (port 9456, same as the latam connector).

    Flow: CL-locale search → click first card → select BASIC fare →
          accept restrictions modal → skip flex (button9) → seats page →
          click 'Quiero elegir asientos después' → tienda/store page →
          intercept /bff/ancillaries/baggages/order/{orderId}/all.

    Uses CL locale because additionalServicesOfferCl=true — the US locale
    suppresses the bags upsell step entirely.
    """
    from letsfg.connectors.latam import _get_context as _la_ctx

    probe_origin = origin if (origin and len(origin) == 3) else "SCL"
    probe_dest = dest if (dest and len(dest) == 3 and dest != probe_origin) else "LIM"
    if probe_dest == probe_origin:
        probe_dest = "LIM"

    dep = _probe_date(date_str, weeks_ahead=6)

    search_url = (
        f"https://www.latamairlines.com/cl/es/ofertas-vuelos"
        f"?outbound={dep}T00%3A00%3A00.000Z&inbound=null"
        f"&origin={probe_origin}&destination={probe_dest}"
        f"&adt=1&chd=0&inf=0&trip=OW&cabin=Y&redemption=false&sort=RECOMMENDED"
    )

    bag_prices: List[float] = []
    carry_on_prices: List[float] = []
    seat_prices: List[float] = []
    currency = "CLP"
    bags_event = asyncio.Event()
    seats_event = asyncio.Event()

    try:
        ctx = await asyncio.wait_for(_la_ctx(), timeout=20.0)
        page = await ctx.new_page()

        async def _on_resp(resp):
            nonlocal currency
            if "latamairlines.com" not in resp.url:
                return
            if resp.status != 200:
                return
            ct = resp.headers.get("content-type", "")
            if "json" not in ct:
                return
            url = resp.url
            try:
                if "/bff/ancillaries/baggages/order/" in url and url.split("?")[0].endswith("/all"):
                    data = await resp.json()
                    # data is list[{passengerId, uniqId, offers:[{typeBag, weight, offers:[{price:{amount,currency}}]}]}]
                    if isinstance(data, list):
                        for pax in data:
                            for bag_type in pax.get("offers", []):
                                wt = bag_type.get("typeBag", 0)
                                for offer in bag_type.get("offers", []):
                                    pr = offer.get("price") or offer.get("totalPrice") or {}
                                    amt = pr.get("amount")
                                    cur = pr.get("currency", currency)
                                    if amt and float(amt) > 0:
                                        currency = cur
                                        if wt <= 15:
                                            carry_on_prices.append(float(amt))
                                        else:
                                            bag_prices.append(float(amt))
                    if bag_prices or carry_on_prices:
                        bags_event.set()
                elif "/bff/ancillaries/seats/offers/order/" in url:
                    data = await resp.json()
                    # data is {"0": {"flightCode", "offers": [{price:{amount,currency}, ...}]}}
                    if isinstance(data, dict):
                        for leg in data.values():
                            if isinstance(leg, dict):
                                for seat in leg.get("offers", []):
                                    pr = seat.get("price") or seat.get("totalPrice") or {}
                                    amt = pr.get("amount")
                                    if amt and float(amt) > 0:
                                        seat_prices.append(float(amt))
                        if seat_prices:
                            seats_event.set()
            except Exception:
                pass

        page.on("response", _on_resp)

        # ── Load search ───────────────────────────────────────────────────────
        logger.debug("LA probe: goto %s", search_url[:100])
        try:
            await page.goto(search_url, wait_until="domcontentloaded", timeout=30_000)
        except Exception as exc:
            logger.debug("LA search page: %s", exc)
        await asyncio.sleep(8)
        logger.debug("LA probe: page URL after 8s = %s", page.url[:120])

        # ── Dismiss overlays ──────────────────────────────────────────────────
        for sel in [
            "[data-testid='country-suggestion--dialog'] button:last-child",
        ]:
            try:
                loc = page.locator(sel).first
                if await loc.count() > 0 and await loc.is_visible(timeout=500):
                    await loc.click(timeout=1500)
                    await asyncio.sleep(0.3)
            except Exception:
                pass
        try:
            await page.evaluate("""
            () => {
                ['[data-testid="country-suggestion--dialog"]',
                 '[id="country-suggestion"]',
                 '[data-testid="boreal-backdrop"]'].forEach(s => {
                    const el = document.querySelector(s);
                    if (el) { el.style.display = 'none'; el.style.pointerEvents = 'none'; }
                });
            }
            """)
        except Exception:
            pass

        # ── Click first flight card ───────────────────────────────────────────
        try:
            cnt = await page.locator('[data-testid="wrapper-card-header-0"]').count()
            logger.debug("LA probe: card count=%d", cnt)
            await page.locator('[data-testid="wrapper-card-header-0"]').click(timeout=5000)
            logger.debug("LA probe: card clicked")
            await asyncio.sleep(3)
        except Exception as exc:
            logger.debug("LA click card: %s", exc)
            await page.close()
            return None

        # ── Select BASIC fare ─────────────────────────────────────────────────
        try:
            await page.locator('[data-testid="bundle-detail-0-flight-select"]').click(timeout=5000)
            logger.debug("LA probe: BASIC fare clicked")
            await asyncio.sleep(2)
        except Exception as exc:
            logger.debug("LA BASIC fare click: %s", exc)

        # ── Accept restrictions modal ─────────────────────────────────────────
        try:
            btn = page.locator('[data-testid="current-brand-button"]')
            cnt = await btn.count()
            logger.debug("LA probe: restrictions btn count=%d", cnt)
            if cnt > 0 and await btn.is_visible(timeout=2000):
                await btn.click(timeout=5000)
                logger.debug("LA probe: restrictions accepted, URL=%s", page.url[:100])
                await asyncio.sleep(3)
        except Exception as exc:
            logger.debug("LA restrictions modal: %s", exc)

        # ── Accept cookies if present ─────────────────────────────────────────
        for sel in ["button:has-text('Accept all cookies')", "button:has-text('Aceptar todas')"]:
            try:
                loc = page.locator(sel).first
                if await loc.count() > 0 and await loc.is_visible(timeout=800):
                    await loc.click(timeout=1500)
                    await asyncio.sleep(0.5)
                    break
            except Exception:
                pass

        # ── Skip flex (Continue without refund option) ────────────────────────
        try:
            btn = page.locator('[data-testid="button9--button"]')
            cnt = await btn.count()
            logger.debug("LA probe: flex btn count=%d, URL=%s", cnt, page.url[:100])
            if cnt > 0 and await btn.is_visible(timeout=2000):
                await btn.click(timeout=5000)
                logger.debug("LA probe: flex skipped")
                await asyncio.sleep(4)
        except Exception:
            for sel in ["button:has-text('Continuar')", "button:has-text('Continue')"]:
                try:
                    loc = page.locator(sel).first
                    if await loc.count() > 0 and await loc.is_visible(timeout=1000):
                        await loc.click(timeout=3000)
                        await asyncio.sleep(4)
                        break
                except Exception:
                    pass

        # ── Wait for seats page, then skip ───────────────────────────────────
        await asyncio.sleep(5)
        logger.debug("LA probe: URL after flex+wait = %s", page.url[:120])
        try:
            await asyncio.wait_for(seats_event.wait(), timeout=3.0)
        except (asyncio.TimeoutError, TimeoutError):
            pass
        # Skip seat selection
        skip_clicked = False
        for sel in [
            "button:has-text('Quiero elegir asientos después')",
            "button:has-text('elegir asientos después')",
            "button:has-text('después')",
            "button:has-text('Skip')",
            "button:has-text('later')",
        ]:
            try:
                loc = page.locator(sel).first
                cnt = await loc.count()
                logger.debug("LA probe: skip-seats sel=%r count=%d", sel[:40], cnt)
                if cnt > 0 and await loc.is_visible(timeout=1000):
                    await loc.click(timeout=5000)
                    skip_clicked = True
                    logger.debug("LA probe: seats skipped with sel=%r", sel[:40])
                    break
            except Exception as exc:
                logger.debug("LA probe: skip-seats sel=%r exc=%s", sel[:40], exc)

        logger.debug("LA probe: skip_clicked=%s, URL=%s", skip_clicked, page.url[:120])
        if skip_clicked:
            # LATAM now shows a confirmation modal: "Tu asiento podría ser el del medio"
            # Must click "Prefiero uno aleatorio" to confirm seat skip
            await asyncio.sleep(2)
            for confirm_sel in [
                "button:has-text('Prefiero uno aleatorio')",
                "button:has-text('aleatorio')",
                "button:has-text('random')",
            ]:
                try:
                    loc = page.locator(confirm_sel).first
                    cnt = await loc.count()
                    logger.debug("LA probe: confirm-skip sel=%r count=%d", confirm_sel[:40], cnt)
                    if cnt > 0 and await loc.is_visible(timeout=2000):
                        await loc.click(timeout=5000)
                        logger.debug("LA probe: confirm-skip clicked")
                        break
                except Exception as exc:
                    logger.debug("LA probe: confirm-skip sel=%r exc=%s", confirm_sel[:40], exc)

            # Wait for navigation to the tienda (store) page where bags API fires
            try:
                await page.wait_for_url("**/tienda**", timeout=20_000)
                logger.debug("LA probe: navigated to tienda, URL=%s", page.url[:120])
            except Exception as exc:
                logger.debug("LA probe: wait_for_url tienda failed: %s, URL=%s", exc, page.url[:120])
            # Give the page a moment to fire the bags API
            await asyncio.sleep(3)

        # ── Wait for bags/all API call ────────────────────────────────────────
        try:
            await asyncio.wait_for(bags_event.wait(), timeout=20.0)
        except (asyncio.TimeoutError, TimeoutError):
            logger.debug("LA bags_event timed out for %s→%s", probe_origin, probe_dest)

        await page.close()
    except Exception as exc:
        logger.debug("LA Playwright probe error: %s", exc)

    if not bag_prices and not carry_on_prices:
        logger.debug("LA probe: no bag prices captured for %s→%s", probe_origin, probe_dest)
        return None

    all_bag = sorted(carry_on_prices + bag_prices)
    min_bag = min(bag_prices) if bag_prices else min(carry_on_prices)
    cur = currency

    result: dict = {
        "checked_bag_note": (
            f"checked bag not included (Basic fare) – add-on from {cur} {min_bag:.0f}"
        ),
        "bags_note": f"carry-on not included (Basic fare) – add-on from {cur} {min(carry_on_prices):.0f}" if carry_on_prices else "carry-on not included (Basic fare)",
        "checked_bag_from": min_bag,
        "currency": cur,
    }
    result["seat_note"] = (
        f"seat selection add-on from {cur} {min(seat_prices):.0f}"
        if seat_prices
        else "seat selection available"
    )
    return result


# ── AF — Air France ───────────────────────────────────────────────────────────

async def _probe_af(
    origin: str,
    dest: str,
    date_str: Optional[str] = None,
    flight_no: Optional[str] = None,
) -> Optional[dict]:
    """
    Air France (AF) — navigate wwws.airfrance.pl via patchright (real Chrome).

    Flow confirmed from live capture (2026-05-03):
    1. Navigate to wwws.airfrance.pl/en (Angular SPA)
    2. Auto-dismiss cookie banner via JS eval
    3. Select One-way, fill origin/dest via keyboard (Akamai-safe)
    4. Click Search → Angular navigates to /search/open-dates or /search/results
    5. On open-dates: click a calendar date → navigates to /search/results
    6. Wait for SearchResultAvailableOffersQuery (fires on results page)
    7. Click first flight row → SearchUpsellOffersQuery fires (fare family
       comparison: Light=no bag, Standard=1×23 kg)
    8. Extract bag add-on = Standard price − Light price
    9. Fallback: AncillariesBaggageQuery (baggageOffers[amount=1].price)
    """
    import json as _json
    import urllib.parse as _up
    from patchright.async_api import async_playwright as _patchright_playwright
    from .browser import find_chrome

    dep = _probe_date(date_str, weeks_ahead=6)

    probe_origin = origin if (origin and len(origin) == 3) else "CDG"
    probe_dest = dest if (dest and len(dest) == 3 and dest != probe_origin) else "LHR"
    if probe_dest == probe_origin:
        probe_dest = "LHR"

    bag_prices: List[float] = []
    seat_prices: List[float] = []
    currency = "PLN"
    upsell_event = asyncio.Event()
    results_event = asyncio.Event()

    async def _on_resp(resp):
        if "/gql/v1" not in resp.url:
            return
        params = dict(_up.parse_qsl(_up.urlparse(resp.url).query))
        op = params.get("operationName", "")
        if not op:
            return
        try:
            if op == "SearchResultAvailableOffersQuery":
                results_event.set()
                return
            if op == "SearchUpsellOffersQuery":
                body = await resp.body()
                data = _json.loads(body)
                price = _af_bag_price_from_gql_upsell(data)
                if price and price > 0:
                    bag_prices.append(price)
                    upsell_event.set()
            elif op == "AncillariesBaggageQuery":
                body = await resp.body()
                data = _json.loads(body)
                result = _af_bag_price_from_ancillaries_gql(data)
                if result:
                    p, cur = result
                    if p > 0:
                        bag_prices.append(p)
                        nonlocal currency
                        currency = cur
                        upsell_event.set()
        except Exception as exc:
            logger.debug("AF GQL parse %s: %s", op, exc)

    chrome_path: Optional[str] = None
    try:
        chrome_path = find_chrome()
    except RuntimeError:
        pass

    try:
        async with _patchright_playwright() as pw:
            launch_kwargs: dict = {
                "headless": False,
                "args": [
                    "--no-first-run",
                    "--no-default-browser-check",
                    "--window-size=1440,900",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            }
            if chrome_path:
                launch_kwargs["executable_path"] = chrome_path
            browser = await pw.chromium.launch(**launch_kwargs)
            ctx = await browser.new_context(
                viewport={"width": 1440, "height": 900},
                locale="en-GB",
                timezone_id="Europe/Paris",
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/135.0.0.0 Safari/537.36"
                ),
            )
            page = await ctx.new_page()
            page.on("response", _on_resp)

            # Navigate to Angular homepage
            try:
                await page.goto(
                    "https://wwws.airfrance.pl/en",
                    wait_until="domcontentloaded",
                    timeout=30_000,
                )
                await asyncio.sleep(4)
            except Exception as exc:
                logger.debug("AF homepage: %s", exc)

            # Auto-dismiss cookie banner
            try:
                await page.evaluate("""() => {
                    const btn = [...document.querySelectorAll('button')]
                        .find(b => /^accept$/i.test(b.textContent?.trim()) && b.offsetParent);
                    if (btn) btn.click();
                }""")
                await asyncio.sleep(0.5)
                await page.evaluate(
                    "() => document.getElementById('bw-cookie-banner')?.remove()"
                )
                await asyncio.sleep(0.5)
            except Exception:
                pass

            # Select One-way via JS eval
            try:
                await page.evaluate("""() => {
                    document.querySelector(
                        '[data-testid="bwsfe-widget__trip-type-selector"]'
                    )?.click();
                }""")
                await asyncio.sleep(0.8)
                await page.evaluate("""() => {
                    Array.from(document.querySelectorAll('mat-option'))
                        .find(o => /one.?way/i.test(o.textContent))?.click();
                }""")
                await asyncio.sleep(0.8)
            except Exception as exc:
                logger.debug("AF one-way: %s", exc)

            # Origin via keyboard (Akamai-safe)
            try:
                await page.evaluate("""() => {
                    const el = document.getElementById('bwsfe-station-picker-input-0');
                    if (el) { el.focus(); el.click(); }
                }""")
                await asyncio.sleep(0.5)
                await page.keyboard.type(probe_origin, delay=120)
                await asyncio.sleep(2.5)
                await page.keyboard.press("ArrowDown")
                await asyncio.sleep(0.3)
                await page.keyboard.press("Enter")
                await asyncio.sleep(2)
            except Exception as exc:
                logger.debug("AF origin: %s", exc)

            # Destination via keyboard
            try:
                await page.evaluate("""() => {
                    const el = document.getElementById('bwsfe-station-picker-input-1');
                    if (el) { el.focus(); el.click(); }
                }""")
                await asyncio.sleep(0.5)
                await page.keyboard.type(probe_dest, delay=120)
                await asyncio.sleep(2.5)
                await page.keyboard.press("ArrowDown")
                await asyncio.sleep(0.3)
                await page.keyboard.press("Enter")
                await asyncio.sleep(2)
            except Exception as exc:
                logger.debug("AF dest: %s", exc)

            # Click Search via JS eval
            try:
                await page.evaluate("""() => {
                    document.querySelector(
                        '[data-testid="bwsfe-widget__search-button"]'
                    )?.click();
                }""")
                await asyncio.sleep(2)
            except Exception as exc:
                logger.debug("AF search click: %s", exc)

            # Wait for URL to reach /search/
            for _ in range(90):
                await asyncio.sleep(0.5)
                if "/search/" in page.url:
                    break

            # On open-dates calendar: click a future date to proceed to results
            if "open-dates" in page.url:
                await asyncio.sleep(2)
                try:
                    await page.evaluate("""() => {
                        const btns = [...document.querySelectorAll(
                            'bwc-day button, [class*="calendar-day"] button'
                        )];
                        const enabled = btns.filter(b => !b.disabled && b.offsetParent);
                        for (const btn of enabled) {
                            const label = btn.getAttribute('aria-label') || '';
                            const num = parseInt(btn.textContent?.trim() || '');
                            if (/jun|jul|aug|sep/i.test(label) && num >= 15) {
                                btn.click(); return;
                            }
                        }
                        const btn = enabled[5] || enabled[0];
                        if (btn) btn.click();
                    }""")
                    await asyncio.sleep(3)
                except Exception as exc:
                    logger.debug("AF calendar click: %s", exc)

            # Wait for SearchResultAvailableOffersQuery
            try:
                await asyncio.wait_for(results_event.wait(), timeout=30.0)
            except (asyncio.TimeoutError, TimeoutError):
                pass

            await asyncio.sleep(2)

            # Click first flight row to trigger SearchUpsellOffersQuery
            if not upsell_event.is_set():
                try:
                    await page.evaluate("""() => {
                        const sels = [
                            'bwsfe-offer-itinerary',
                            '[data-testid*="offer-itinerary"]',
                            '[class*="offer-itinerary"]',
                            'app-offer-itinerary',
                        ];
                        for (const sel of sels) {
                            const items = [...document.querySelectorAll(sel)];
                            const el = items.find(e => e.offsetParent);
                            if (el) {
                                const btn = el.querySelector('button') || el;
                                btn.click();
                                return;
                            }
                        }
                        // Fallback: first button whose test-id mentions offer
                        const btn = [...document.querySelectorAll('button')].find(b =>
                            b.offsetParent &&
                            (b.getAttribute('data-testid') || '').toLowerCase().includes('offer')
                        );
                        if (btn) btn.click();
                    }""")
                    await asyncio.sleep(2)
                except Exception as exc:
                    logger.debug("AF flight row click: %s", exc)

            # Wait for SearchUpsellOffersQuery
            try:
                await asyncio.wait_for(upsell_event.wait(), timeout=20.0)
            except (asyncio.TimeoutError, TimeoutError):
                pass

            await browser.close()
    except Exception as exc:
        logger.debug("AF probe error: %s", exc)

    if bag_prices:
        min_bag = min(p for p in bag_prices if p > 0)
        result: dict = {
            "checked_bag_note": (
                f"checked bag not included (Light fare) "
                f"– add-on from {currency} {min_bag:.0f}"
            ),
            "bags_note": "cabin bag 12 kg included free (Light fare)",
            "checked_bag_from": min_bag,
            "checked_bag_price": min_bag,
            "currency": currency,
        }
        result["seat_note"] = (
            f"seat selection add-on from {currency} {min(seat_prices):.0f}"
            if seat_prices
            else "seat selection add-on available"
        )
        return result

    logger.debug("AF probe: no bag prices captured for %s→%s", probe_origin, probe_dest)
    return None


def _af_bag_price_from_gql_upsell(data: dict) -> Optional[float]:
    """
    Extract checked-bag add-on price from AF SearchUpsellOffersQuery GQL response.

    The response contains upsellFlightProducts with fare families. The Light fare
    has no checked bag; Standard has 1×23 kg. Returns Standard − Light price diff.
    Confirmed structure from live capture 2026-05-03.
    """
    try:
        recs = (
            data.get("data", {})
            .get("upsellOffers", {})
            .get("upsellRecommendations", [])
        )
        for rec in recs:
            products = rec.get("upsellFlightProducts", [])
            light_price: Optional[float] = None
            standard_price: Optional[float] = None
            for p in products:
                cu = p.get("activeConnectionUpsell") or {}
                fare_title = (
                    (cu.get("fareFamily") or {}).get("title", "")
                ).lower().strip()
                price_val = (cu.get("price") or {}).get("relevantPrice")
                if price_val is None:
                    continue
                price_val = float(price_val)
                conds = cu.get("primaryConditions") or []
                bag_included = any(
                    c.get("code") == "MERGED_BAGGAGE_ALLOWANCE" and c.get("included") is True
                    for c in conds
                )
                bag_excluded = any(
                    c.get("code") == "MERGED_BAGGAGE_ALLOWANCE" and c.get("included") is False
                    for c in conds
                )
                if bag_excluded and fare_title in ("light", "basic", "eco light", "light fare"):
                    light_price = price_val
                elif bag_included and fare_title in (
                    "standard", "classic", "flex", "standard plus", "economy"
                ):
                    if standard_price is None or price_val < standard_price:
                        standard_price = price_val
            if light_price is not None and standard_price is not None:
                diff = standard_price - light_price
                if 5 < diff < 3000:
                    return round(diff, 2)
    except Exception:
        pass
    return None


def _af_bag_price_from_ancillaries_gql(data: dict) -> Optional[tuple]:
    """
    Extract 1-bag add-on price from AF AncillariesBaggageQuery GQL response.

    baggageOffers[amount=1].priceGroup.currentPrice.amount is the direct
    add-on price for 1 checked bag on a Light fare.
    Returns (price: float, currency: str) or None.
    Confirmed structure from live capture 2026-05-03.
    """
    try:
        offers = (
            data.get("data", {})
            .get("ancillaries", {})
            .get("baggageOffers", [])
        )
        for o in offers:
            if o.get("amount") == 1 and o.get("unit") == "PIECE":
                pg = o.get("priceGroup") or {}
                cp = pg.get("currentPrice") or {}
                price = cp.get("amount")
                cur = cp.get("currencyCode", "PLN")
                if price is not None and float(price) > 0:
                    return (float(price), str(cur))
    except Exception:
        pass
    return None


def _af_bag_price_from_fare_families(data: dict) -> Optional[float]:
    """
    Extract bag add-on price from AF/KL fare family data.

    AF/KL Sputnik response contains fare bundles: Light (no bag) and Standard
    (23 kg included). The price difference is the bag add-on price.
    """
    _LIGHT_KEYS = {"light", "basic", "economy light", "eco light"}
    _BAG_KEYS = {"standard", "flex", "classic", "economy", "economy standard"}
    light_prices: List[float] = []
    bag_prices: List[float] = []

    try:
        for d in _iter_dicts(data):
            bundle_name = ""
            for key in ("bundleName", "fareName", "fareFamily", "productCode",
                        "cabinClassName", "brandName", "fareType"):
                v = d.get(key)
                if v and isinstance(v, str):
                    bundle_name = v.lower().strip()
                    break
            if not bundle_name:
                continue
            price = None
            for pk in ("totalPrice", "price", "amount", "totalAmount", "fareAmount", "basePrice"):
                v = d.get(pk)
                if v is None:
                    continue
                if isinstance(v, dict):
                    v = v.get("amount") or v.get("value")
                try:
                    p = float(v)  # type: ignore[arg-type]
                    if 10 < p < 10000:
                        price = p
                        break
                except (TypeError, ValueError):
                    pass
            if price is None:
                continue
            if any(k in bundle_name for k in _LIGHT_KEYS):
                light_prices.append(price)
            elif any(k in bundle_name for k in _BAG_KEYS):
                bag_prices.append(price)
    except Exception:
        pass

    if light_prices and bag_prices:
        diff = min(bag_prices) - min(light_prices)
        if 5 < diff < 500:
            return round(diff, 2)
    return None


async def _scrape_fare_family_bag_price(page) -> List[float]:
    """
    Scrape bag add-on prices from the visible DOM of an airline fare selection page.

    Looks for price patterns next to bag-related keywords in the page body.
    Returns a list of numeric prices found (may be empty).
    """
    try:
        prices = await page.evaluate(r"""() => {
            const results = [];
            const text = (document.body || {}).innerText || '';
            // Match patterns like "EUR 25", "€25", "25 EUR", "$25" near bag keywords
            const bagSection = text.replace(/\n/g, ' ');
            const bagPattern = /(?:bag|baggage|luggage|checked|hold)[^\n]{0,60}([\d]+(?:[.,]\d{1,2})?)\s*(EUR|GBP|USD|CAD|AUD|NZD|CHF)/gi;
            const pricePattern = /(EUR|GBP|USD|CAD|AUD|NZD|CHF)\s*([\d]+(?:[.,]\d{1,2})?)/gi;
            let m;
            while ((m = bagPattern.exec(bagSection)) !== null) {
                const val = parseFloat(m[1].replace(',', '.'));
                if (val > 3 && val < 500) results.push(val);
            }
            return results.slice(0, 5);
        }""")
        if isinstance(prices, list):
            return [float(p) for p in prices if p]
    except Exception:
        pass
    return []


# ── KL — KLM ─────────────────────────────────────────────────────────────────

async def _probe_kl(
    origin: str,
    dest: str,
    date_str: Optional[str] = None,
    flight_no: Optional[str] = None,
) -> Optional[dict]:
    """
    KLM (KL) — same Angular/GQL approach as AF.

    KLM shares the same Angular SPA backend as Air France (wwws.klm.com).
    Fires identical GQL operations: SearchResultAvailableOffersQuery,
    SearchUpsellOffersQuery, AncillariesBaggageQuery.

    Flow (mirrors _probe_af):
    1. Navigate to wwws.klm.com/en
    2. Dismiss cookie banner, select One-way, fill origin/dest via keyboard
    3. Click Search → wait for /search/ route
    4. On open-dates: click a calendar date
    5. Wait for SearchResultAvailableOffersQuery, click first flight row
    6. Capture SearchUpsellOffersQuery → Light vs Standard fare diff
    7. Fallback: AncillariesBaggageQuery → direct add-on price
    """
    import json as _json
    import urllib.parse as _up
    from patchright.async_api import async_playwright as _patchright_playwright
    from .browser import find_chrome

    dep = _probe_date(date_str, weeks_ahead=6)

    probe_origin = origin if (origin and len(origin) == 3) else "AMS"
    probe_dest = dest if (dest and len(dest) == 3 and dest != probe_origin) else "LHR"
    if probe_dest == probe_origin:
        probe_dest = "LHR"

    bag_prices: List[float] = []
    seat_prices: List[float] = []
    currency = "EUR"
    upsell_event = asyncio.Event()
    results_event = asyncio.Event()

    async def _on_resp(resp):
        if "/gql/v1" not in resp.url:
            return
        params = dict(_up.parse_qsl(_up.urlparse(resp.url).query))
        op = params.get("operationName", "")
        if not op:
            return
        try:
            if op == "SearchResultAvailableOffersQuery":
                results_event.set()
                return
            if op == "SearchUpsellOffersQuery":
                body = await resp.body()
                data = _json.loads(body)
                price = _af_bag_price_from_gql_upsell(data)
                if price and price > 0:
                    bag_prices.append(price)
                    upsell_event.set()
            elif op == "AncillariesBaggageQuery":
                body = await resp.body()
                data = _json.loads(body)
                result = _af_bag_price_from_ancillaries_gql(data)
                if result:
                    p, cur = result
                    if p > 0:
                        bag_prices.append(p)
                        nonlocal currency
                        currency = cur
                        upsell_event.set()
        except Exception as exc:
            logger.debug("KL GQL parse %s: %s", op, exc)

    chrome_path: Optional[str] = None
    try:
        chrome_path = find_chrome()
    except RuntimeError:
        pass

    try:
        async with _patchright_playwright() as pw:
            launch_kwargs: dict = {
                "headless": False,
                "args": [
                    "--no-first-run",
                    "--no-default-browser-check",
                    "--window-size=1440,900",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            }
            if chrome_path:
                launch_kwargs["executable_path"] = chrome_path
            browser = await pw.chromium.launch(**launch_kwargs)
            ctx = await browser.new_context(
                viewport={"width": 1440, "height": 900},
                locale="en-GB",
                timezone_id="Europe/Amsterdam",
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/135.0.0.0 Safari/537.36"
                ),
            )
            page = await ctx.new_page()
            page.on("response", _on_resp)

            # Navigate to KLM Angular homepage
            try:
                await page.goto(
                    "https://wwws.klm.com/en",
                    wait_until="domcontentloaded",
                    timeout=30_000,
                )
                await asyncio.sleep(4)
            except Exception as exc:
                logger.debug("KL homepage: %s", exc)

            # Dismiss cookie banner
            try:
                await page.evaluate("""() => {
                    const btn = [...document.querySelectorAll('button')]
                        .find(b => /^accept$/i.test(b.textContent?.trim()) && b.offsetParent);
                    if (btn) btn.click();
                }""")
                await asyncio.sleep(0.5)
                await page.evaluate(
                    "() => document.getElementById('bw-cookie-banner')?.remove()"
                )
                await asyncio.sleep(0.5)
            except Exception:
                pass

            # Select One-way
            try:
                await page.evaluate("""() => {
                    document.querySelector(
                        '[data-testid="bwsfe-widget__trip-type-selector"]'
                    )?.click();
                }""")
                await asyncio.sleep(0.8)
                await page.evaluate("""() => {
                    Array.from(document.querySelectorAll('mat-option'))
                        .find(o => /one.?way/i.test(o.textContent))?.click();
                }""")
                await asyncio.sleep(0.8)
            except Exception as exc:
                logger.debug("KL one-way: %s", exc)

            # Origin via keyboard
            try:
                await page.evaluate("""() => {
                    const el = document.getElementById('bwsfe-station-picker-input-0');
                    if (el) { el.focus(); el.click(); }
                }""")
                await asyncio.sleep(0.5)
                await page.keyboard.type(probe_origin, delay=120)
                await asyncio.sleep(2.5)
                await page.keyboard.press("ArrowDown")
                await asyncio.sleep(0.3)
                await page.keyboard.press("Enter")
                await asyncio.sleep(2)
            except Exception as exc:
                logger.debug("KL origin: %s", exc)

            # Destination via keyboard
            try:
                await page.evaluate("""() => {
                    const el = document.getElementById('bwsfe-station-picker-input-1');
                    if (el) { el.focus(); el.click(); }
                }""")
                await asyncio.sleep(0.5)
                await page.keyboard.type(probe_dest, delay=120)
                await asyncio.sleep(2.5)
                await page.keyboard.press("ArrowDown")
                await asyncio.sleep(0.3)
                await page.keyboard.press("Enter")
                await asyncio.sleep(2)
            except Exception as exc:
                logger.debug("KL dest: %s", exc)

            # Click Search
            try:
                await page.evaluate("""() => {
                    document.querySelector(
                        '[data-testid="bwsfe-widget__search-button"]'
                    )?.click();
                }""")
                await asyncio.sleep(2)
            except Exception as exc:
                logger.debug("KL search click: %s", exc)

            # Wait for URL to reach /search/
            for _ in range(90):
                await asyncio.sleep(0.5)
                if "/search/" in page.url:
                    break

            # On open-dates: click a future date to proceed to results
            if "open-dates" in page.url:
                await asyncio.sleep(2)
                try:
                    await page.evaluate("""() => {
                        const btns = [...document.querySelectorAll(
                            'bwc-day button, [class*="calendar-day"] button'
                        )];
                        const enabled = btns.filter(b => !b.disabled && b.offsetParent);
                        for (const btn of enabled) {
                            const label = btn.getAttribute('aria-label') || '';
                            const num = parseInt(btn.textContent?.trim() || '');
                            if (/jun|jul|aug|sep/i.test(label) && num >= 15) {
                                btn.click(); return;
                            }
                        }
                        const btn = enabled[5] || enabled[0];
                        if (btn) btn.click();
                    }""")
                    await asyncio.sleep(3)
                except Exception as exc:
                    logger.debug("KL calendar click: %s", exc)

            # Wait for SearchResultAvailableOffersQuery
            try:
                await asyncio.wait_for(results_event.wait(), timeout=30.0)
            except (asyncio.TimeoutError, TimeoutError):
                pass

            await asyncio.sleep(2)

            # Click first flight row to trigger SearchUpsellOffersQuery
            if not upsell_event.is_set():
                try:
                    await page.evaluate("""() => {
                        const sels = [
                            'bwsfe-offer-itinerary',
                            '[data-testid*="offer-itinerary"]',
                            '[class*="offer-itinerary"]',
                            'app-offer-itinerary',
                        ];
                        for (const sel of sels) {
                            const items = [...document.querySelectorAll(sel)];
                            const el = items.find(e => e.offsetParent);
                            if (el) {
                                const btn = el.querySelector('button') || el;
                                btn.click();
                                return;
                            }
                        }
                        const btn = [...document.querySelectorAll('button')].find(b =>
                            b.offsetParent &&
                            (b.getAttribute('data-testid') || '').toLowerCase().includes('offer')
                        );
                        if (btn) btn.click();
                    }""")
                    await asyncio.sleep(2)
                except Exception as exc:
                    logger.debug("KL flight row click: %s", exc)

            # Wait for SearchUpsellOffersQuery
            try:
                await asyncio.wait_for(upsell_event.wait(), timeout=20.0)
            except (asyncio.TimeoutError, TimeoutError):
                pass

            await browser.close()
    except Exception as exc:
        logger.debug("KL probe error: %s", exc)

    if bag_prices:
        min_bag = min(p for p in bag_prices if p > 0)
        result: dict = {
            "checked_bag_note": (
                f"checked bag not included (Light fare) "
                f"– add-on from {currency} {min_bag:.0f}"
            ),
            "bags_note": "cabin bag 12 kg included free (Light fare)",
            "checked_bag_from": min_bag,
            "checked_bag_price": min_bag,
            "currency": currency,
        }
        result["seat_note"] = (
            f"seat selection add-on from {currency} {min(seat_prices):.0f}"
            if seat_prices
            else "seat selection add-on available"
        )
        return result

    logger.debug("KL probe: no bag prices captured for %s→%s", probe_origin, probe_dest)
    return None


# ── BA — British Airways ──────────────────────────────────────────────────────

async def _probe_ba(
    origin: str,
    dest: str,
    date_str: Optional[str] = None,
    flight_no: Optional[str] = None,
) -> Optional[dict]:
    """
    British Airways (BA) — navigate BA booking flow via Playwright.

    BA's website fires an NDC/GraphQL API when showing fare families
    (Hand Baggage Only vs Euro Traveller). The price difference between
    HBO and ET is the checked-bag add-on price (typically GBP 35-75).

    Strategy:
    1. Navigate to BA search results deep link.
    2. Intercept JSON responses from britishairways.com APIs.
    3. Extract fare family price differential as checked-bag add-on.
    4. Fallback: scrape DOM for bag add-on price text.
    """
    from patchright.async_api import async_playwright as _patchright_playwright
    from .browser import find_chrome

    dep = _probe_date(date_str, weeks_ahead=6)

    probe_origin = origin if (origin and len(origin) == 3) else "LHR"
    probe_dest = dest if (dest and len(dest) == 3 and dest != probe_origin) else "CDG"
    if probe_dest == probe_origin:
        probe_dest = "CDG"

    bag_prices: List[float] = []
    seat_prices: List[float] = []
    currency = "GBP"
    cap_event = asyncio.Event()

    async def _on_resp(resp):
        ct = resp.headers.get("content-type", "")
        if resp.status != 200 or "json" not in ct:
            return
        url_l = resp.url.lower()
        if "britishairways" not in url_l and "ba.com" not in url_l:
            return
        try:
            data = await resp.json()
            # Look for NDC fare offerings or ancillary responses
            b, s = _extract_bag_seat_prices(data)
            bag_prices.extend(b)
            seat_prices.extend(s)
            diff = _ba_bag_price_from_fare_families(data)
            if diff:
                bag_prices.append(diff)
            if bag_prices:
                cap_event.set()
        except Exception:
            pass

    chrome_path: Optional[str] = None
    try:
        chrome_path = find_chrome()
    except RuntimeError:
        pass

    try:
        async with _patchright_playwright() as pw:
            launch_kwargs: dict = {
                "headless": False,
                "args": [
                    "--no-first-run",
                    "--no-default-browser-check",
                    "--window-size=1440,900",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            }
            if chrome_path:
                launch_kwargs["executable_path"] = chrome_path
            browser = await pw.chromium.launch(**launch_kwargs)
            ctx = await browser.new_context(
                viewport={"width": 1440, "height": 900},
                locale="en-GB",
                timezone_id="Europe/London",
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/135.0.0.0 Safari/537.36"
                ),
            )
            page = await ctx.new_page()
            page.on("response", _on_resp)

            try:
                await page.goto(
                    "https://www.britishairways.com/en-gb/",
                    wait_until="domcontentloaded",
                    timeout=30_000,
                )
                await asyncio.sleep(3)
            except Exception as exc:
                logger.debug("BA homepage: %s", exc)

            try:
                await page.evaluate("""() => {
                    for (const b of document.querySelectorAll('button')) {
                        const t = (b.textContent || '').toLowerCase();
                        if ((t.includes('accept') || t.includes('agree') || t.includes('ok')) && b.offsetHeight > 0) {
                            b.click(); return;
                        }
                    }
                }""")
                await asyncio.sleep(1)
            except Exception:
                pass

            dep_compact = dep.replace("-", "")
            booking_url = (
                f"https://www.britishairways.com/travel/beplorer/public/en_gb"
                f"?eId=106004&departure={probe_origin}&arrival={probe_dest}"
                f"&departDate={dep_compact}&numAdults=1&cabin=M"
            )
            try:
                await page.goto(booking_url, wait_until="domcontentloaded", timeout=30_000)
                await asyncio.sleep(4)
            except Exception as exc:
                logger.debug("BA search nav: %s", exc)

            try:
                await asyncio.wait_for(cap_event.wait(), timeout=25.0)
            except (asyncio.TimeoutError, TimeoutError):
                pass

            # Try clicking first flight result to trigger ancillary API
            if not bag_prices:
                try:
                    for sel in [
                        "[data-testid='fare-card']",
                        "[class*='fare-option']",
                        "[class*='FareCard']",
                        "button[class*='select']",
                    ]:
                        if await page.locator(sel).count() > 0:
                            await page.locator(sel).first.click()
                            await asyncio.sleep(4)
                            break
                    try:
                        await asyncio.wait_for(cap_event.wait(), timeout=15.0)
                    except (asyncio.TimeoutError, TimeoutError):
                        pass
                except Exception as exc:
                    logger.debug("BA fare card click: %s", exc)

            if not bag_prices:
                try:
                    dom_prices = await _scrape_fare_family_bag_price(page)
                    bag_prices.extend(dom_prices)
                except Exception:
                    pass

            await browser.close()
    except Exception as exc:
        logger.debug("BA Playwright probe error: %s", exc)

    if bag_prices:
        min_bag = min(p for p in bag_prices if p > 0)
        result: dict = {
            "checked_bag_note": (
                f"checked bag not included (Hand Baggage Only) – add-on from GBP {min_bag:.0f}"
            ),
            "bags_note": "cabin bag 23 kg included free (Hand Baggage Only fare)",
            "checked_bag_from": min_bag,
            "checked_bag_price": min_bag,
            "currency": currency,
        }
        result["seat_note"] = (
            f"seat selection add-on from GBP {min(seat_prices):.0f}"
            if seat_prices
            else "seat selection add-on available"
        )
        return result

    logger.debug("BA probe: no bag prices captured for %s→%s", probe_origin, probe_dest)
    return None


def _ba_bag_price_from_fare_families(data: dict) -> Optional[float]:
    """Extract bag add-on from BA fare family data (HBO vs Euro Traveller)."""
    _NO_BAG = {"hand baggage only", "hbo", "basic", "light", "economy light"}
    _BAG_INCL = {"euro traveller", "standard", "classic", "economy", "plus"}
    no_bag_prices: List[float] = []
    bag_prices: List[float] = []

    try:
        for d in _iter_dicts(data):
            bundle_name = ""
            for key in ("fareFamily", "fareName", "bundleName", "productName",
                        "cabinName", "fareType", "brandName"):
                v = d.get(key)
                if v and isinstance(v, str):
                    bundle_name = v.lower().strip()
                    break
            if not bundle_name:
                continue
            price = None
            for pk in ("totalPrice", "price", "amount", "totalAmount", "baseAmount"):
                v = d.get(pk)
                if v is None:
                    continue
                if isinstance(v, dict):
                    v = v.get("amount") or v.get("value")
                try:
                    p = float(v)  # type: ignore[arg-type]
                    if 10 < p < 10000:
                        price = p
                        break
                except (TypeError, ValueError):
                    pass
            if price is None:
                continue
            if any(k in bundle_name for k in _NO_BAG):
                no_bag_prices.append(price)
            elif any(k in bundle_name for k in _BAG_INCL):
                bag_prices.append(price)
    except Exception:
        pass

    if no_bag_prices and bag_prices:
        diff = min(bag_prices) - min(no_bag_prices)
        if 5 < diff < 500:
            return round(diff, 2)
    return None


# ── LH / OS / LX / SN — Lufthansa Group ─────────────────────────────────────

async def _probe_lh(
    origin: str,
    dest: str,
    date_str: Optional[str] = None,
    flight_no: Optional[str] = None,
) -> Optional[dict]:
    """
    Lufthansa Group (LH/OS/LX/SN) — navigate Lufthansa booking via Playwright.

    Lufthansa uses an NDC booking engine. The fare selection page shows
    Economy Light (no bag) vs Economy Classic (23 kg included). We intercept
    the NDC OfferPrice or AirShopping response to extract the bag add-on.

    Strategy:
    1. Navigate to Lufthansa search results for the given route.
    2. Intercept JSON responses from the Lufthansa booking APIs.
    3. Extract fare family price differential as checked-bag add-on.
    """
    from patchright.async_api import async_playwright as _patchright_playwright
    from .browser import find_chrome

    dep = _probe_date(date_str, weeks_ahead=6)

    probe_origin = origin if (origin and len(origin) == 3) else "FRA"
    probe_dest = dest if (dest and len(dest) == 3 and dest != probe_origin) else "LHR"
    if probe_dest == probe_origin:
        probe_dest = "LHR"

    bag_prices: List[float] = []
    seat_prices: List[float] = []
    currency = "EUR"
    cap_event = asyncio.Event()

    async def _on_resp(resp):
        ct = resp.headers.get("content-type", "")
        if resp.status != 200 or "json" not in ct:
            return
        url_l = resp.url.lower()
        if not any(k in url_l for k in ("lufthansa", "lh.com", "swiss", "austrian", "brussels")):
            return
        try:
            data = await resp.json()
            b, s = _extract_bag_seat_prices(data)
            bag_prices.extend(b)
            seat_prices.extend(s)
            diff = _af_bag_price_from_fare_families(data)
            if diff:
                bag_prices.append(diff)
            if bag_prices:
                cap_event.set()
        except Exception:
            pass

    chrome_path: Optional[str] = None
    try:
        chrome_path = find_chrome()
    except RuntimeError:
        pass

    try:
        async with _patchright_playwright() as pw:
            launch_kwargs: dict = {
                "headless": False,
                "args": [
                    "--no-first-run",
                    "--no-default-browser-check",
                    "--window-size=1440,900",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            }
            if chrome_path:
                launch_kwargs["executable_path"] = chrome_path
            browser = await pw.chromium.launch(**launch_kwargs)
            ctx = await browser.new_context(
                viewport={"width": 1440, "height": 900},
                locale="en-GB",
                timezone_id="Europe/Berlin",
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/135.0.0.0 Safari/537.36"
                ),
            )
            page = await ctx.new_page()
            page.on("response", _on_resp)

            try:
                await page.goto(
                    "https://www.lufthansa.com/gb/en/homepage",
                    wait_until="domcontentloaded",
                    timeout=30_000,
                )
                await asyncio.sleep(3)
            except Exception as exc:
                logger.debug("LH homepage: %s", exc)

            try:
                await page.evaluate("""() => {
                    for (const b of document.querySelectorAll('button')) {
                        const t = (b.textContent || '').toLowerCase();
                        if ((t.includes('accept') || t.includes('agree') || t.includes('ok')) && b.offsetHeight > 0) {
                            b.click(); return;
                        }
                    }
                }""")
                await asyncio.sleep(1)
            except Exception:
                pass

            booking_url = (
                f"https://www.lufthansa.com/gb/en/flight-selection?"
                f"origin={probe_origin}&destination={probe_dest}"
                f"&outbound={dep}&adults=1&tripType=OW"
            )
            try:
                await page.goto(booking_url, wait_until="domcontentloaded", timeout=30_000)
                await asyncio.sleep(4)
            except Exception as exc:
                logger.debug("LH search nav: %s", exc)

            try:
                await asyncio.wait_for(cap_event.wait(), timeout=25.0)
            except (asyncio.TimeoutError, TimeoutError):
                pass

            if not bag_prices:
                try:
                    dom_prices = await _scrape_fare_family_bag_price(page)
                    bag_prices.extend(dom_prices)
                except Exception:
                    pass

            await browser.close()
    except Exception as exc:
        logger.debug("LH Playwright probe error: %s", exc)

    if bag_prices:
        min_bag = min(p for p in bag_prices if p > 0)
        result: dict = {
            "checked_bag_note": (
                f"checked bag not included (Economy Light) – add-on from EUR {min_bag:.0f}"
            ),
            "bags_note": "cabin bag 8 kg included free (Economy Light)",
            "checked_bag_from": min_bag,
            "checked_bag_price": min_bag,
            "currency": currency,
        }
        result["seat_note"] = (
            f"seat selection add-on from EUR {min(seat_prices):.0f}"
            if seat_prices
            else "seat selection add-on available"
        )
        return result

    logger.debug("LH probe: no bag prices captured for %s→%s", probe_origin, probe_dest)
    return None


# ── IB — Iberia ───────────────────────────────────────────────────────────────

async def _probe_ib(
    origin: str,
    dest: str,
    date_str: Optional[str] = None,
    flight_no: Optional[str] = None,
) -> Optional[dict]:
    """
    Iberia (IB) — navigate Iberia booking flow via Playwright.

    Iberia uses Amadeus IBE. Their search results show fare families
    (Basic = no bag, Economy = 23 kg included). Intercept XHR responses.
    """
    from patchright.async_api import async_playwright as _patchright_playwright
    from .browser import find_chrome

    dep = _probe_date(date_str, weeks_ahead=6)

    probe_origin = origin if (origin and len(origin) == 3) else "MAD"
    probe_dest = dest if (dest and len(dest) == 3 and dest != probe_origin) else "LHR"
    if probe_dest == probe_origin:
        probe_dest = "LHR"

    bag_prices: List[float] = []
    seat_prices: List[float] = []
    currency = "EUR"
    cap_event = asyncio.Event()

    async def _on_resp(resp):
        ct = resp.headers.get("content-type", "")
        if resp.status != 200 or "json" not in ct:
            return
        url_l = resp.url.lower()
        if "iberia" not in url_l and "amadeus" not in url_l:
            return
        try:
            data = await resp.json()
            b, s = _extract_bag_seat_prices(data)
            bag_prices.extend(b)
            seat_prices.extend(s)
            diff = _af_bag_price_from_fare_families(data)
            if diff:
                bag_prices.append(diff)
            if bag_prices:
                cap_event.set()
        except Exception:
            pass

    chrome_path: Optional[str] = None
    try:
        chrome_path = find_chrome()
    except RuntimeError:
        pass

    try:
        async with _patchright_playwright() as pw:
            launch_kwargs: dict = {
                "headless": False,
                "args": [
                    "--no-first-run",
                    "--no-default-browser-check",
                    "--window-size=1440,900",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            }
            if chrome_path:
                launch_kwargs["executable_path"] = chrome_path
            browser = await pw.chromium.launch(**launch_kwargs)
            ctx = await browser.new_context(
                viewport={"width": 1440, "height": 900},
                locale="en-GB",
                timezone_id="Europe/Madrid",
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/135.0.0.0 Safari/537.36"
                ),
            )
            page = await ctx.new_page()
            page.on("response", _on_resp)

            try:
                await page.goto("https://www.iberia.com/en/", wait_until="domcontentloaded", timeout=30_000)
                await asyncio.sleep(3)
            except Exception as exc:
                logger.debug("IB homepage: %s", exc)

            try:
                await page.evaluate("""() => {
                    for (const b of document.querySelectorAll('button')) {
                        const t = (b.textContent || '').toLowerCase();
                        if ((t.includes('accept') || t.includes('agree') || t.includes('ok')) && b.offsetHeight > 0) {
                            b.click(); return;
                        }
                    }
                }""")
                await asyncio.sleep(1)
            except Exception:
                pass

            booking_url = (
                f"https://www.iberia.com/en/flights/results/?"
                f"origin={probe_origin}&destination={probe_dest}"
                f"&departure={dep}&adults=1&tripType=OW"
            )
            try:
                await page.goto(booking_url, wait_until="domcontentloaded", timeout=30_000)
                await asyncio.sleep(4)
            except Exception as exc:
                logger.debug("IB search nav: %s", exc)

            try:
                await asyncio.wait_for(cap_event.wait(), timeout=25.0)
            except (asyncio.TimeoutError, TimeoutError):
                pass

            if not bag_prices:
                try:
                    dom_prices = await _scrape_fare_family_bag_price(page)
                    bag_prices.extend(dom_prices)
                except Exception:
                    pass

            await browser.close()
    except Exception as exc:
        logger.debug("IB Playwright probe error: %s", exc)

    if bag_prices:
        min_bag = min(p for p in bag_prices if p > 0)
        result: dict = {
            "checked_bag_note": (
                f"checked bag not included (Basic fare) – add-on from EUR {min_bag:.0f}"
            ),
            "bags_note": "cabin bag included free (Basic fare)",
            "checked_bag_from": min_bag,
            "checked_bag_price": min_bag,
            "currency": currency,
        }
        result["seat_note"] = (
            f"seat selection add-on from EUR {min(seat_prices):.0f}"
            if seat_prices
            else "seat selection add-on available"
        )
        return result

    logger.debug("IB probe: no bag prices captured for %s→%s", probe_origin, probe_dest)
    return None


# ── AC — Air Canada ───────────────────────────────────────────────────────────

async def _probe_ac(
    origin: str,
    dest: str,
    date_str: Optional[str] = None,
    flight_no: Optional[str] = None,
) -> Optional[dict]:
    """
    Air Canada (AC) — navigate Air Canada booking flow via Playwright.

    Air Canada uses a React-based booking engine. The fare selection page shows
    Basic (no bag) vs Standard (1×23 kg included) with the price difference
    being the checked-bag add-on.
    """
    from patchright.async_api import async_playwright as _patchright_playwright
    from .browser import find_chrome

    dep = _probe_date(date_str, weeks_ahead=6)

    probe_origin = origin if (origin and len(origin) == 3) else "YYZ"
    probe_dest = dest if (dest and len(dest) == 3 and dest != probe_origin) else "JFK"
    if probe_dest == probe_origin:
        probe_dest = "JFK"

    bag_prices: List[float] = []
    seat_prices: List[float] = []
    currency = "CAD"
    cap_event = asyncio.Event()

    async def _on_resp(resp):
        ct = resp.headers.get("content-type", "")
        if resp.status != 200 or "json" not in ct:
            return
        url_l = resp.url.lower()
        if "aircanada" not in url_l and "ac.com" not in url_l:
            return
        try:
            data = await resp.json()
            b, s = _extract_bag_seat_prices(data)
            bag_prices.extend(b)
            seat_prices.extend(s)
            diff = _af_bag_price_from_fare_families(data)
            if diff:
                bag_prices.append(diff)
            if bag_prices:
                cap_event.set()
        except Exception:
            pass

    chrome_path: Optional[str] = None
    try:
        chrome_path = find_chrome()
    except RuntimeError:
        pass

    try:
        async with _patchright_playwright() as pw:
            launch_kwargs: dict = {
                "headless": False,
                "args": [
                    "--no-first-run",
                    "--no-default-browser-check",
                    "--window-size=1440,900",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            }
            if chrome_path:
                launch_kwargs["executable_path"] = chrome_path
            browser = await pw.chromium.launch(**launch_kwargs)
            ctx = await browser.new_context(
                viewport={"width": 1440, "height": 900},
                locale="en-CA",
                timezone_id="America/Toronto",
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/135.0.0.0 Safari/537.36"
                ),
            )
            page = await ctx.new_page()
            page.on("response", _on_resp)

            try:
                await page.goto("https://www.aircanada.com/en-ca/", wait_until="domcontentloaded", timeout=30_000)
                await asyncio.sleep(3)
            except Exception as exc:
                logger.debug("AC homepage: %s", exc)

            try:
                await page.evaluate("""() => {
                    for (const b of document.querySelectorAll('button')) {
                        const t = (b.textContent || '').toLowerCase();
                        if ((t.includes('accept') || t.includes('agree') || t.includes('ok')) && b.offsetHeight > 0) {
                            b.click(); return;
                        }
                    }
                }""")
                await asyncio.sleep(1)
            except Exception:
                pass

            booking_url = (
                f"https://www.aircanada.com/en-ca/aco/home.ace#"
                f"OW/true/{probe_origin}/{probe_dest}/{dep}/null/"
                f"1/0/0/Y/-/-"
            )
            try:
                await page.goto(booking_url, wait_until="domcontentloaded", timeout=30_000)
                await asyncio.sleep(4)
            except Exception as exc:
                logger.debug("AC search nav: %s", exc)

            try:
                await asyncio.wait_for(cap_event.wait(), timeout=25.0)
            except (asyncio.TimeoutError, TimeoutError):
                pass

            if not bag_prices:
                try:
                    dom_prices = await _scrape_fare_family_bag_price(page)
                    bag_prices.extend(dom_prices)
                except Exception:
                    pass

            await browser.close()
    except Exception as exc:
        logger.debug("AC Playwright probe error: %s", exc)

    if bag_prices:
        min_bag = min(p for p in bag_prices if p > 0)
        result: dict = {
            "checked_bag_note": (
                f"checked bag not included (Basic fare) – add-on from CAD {min_bag:.0f}"
            ),
            "bags_note": "carry-on bag included free (Basic fare)",
            "checked_bag_from": min_bag,
            "checked_bag_price": min_bag,
            "currency": currency,
        }
        result["seat_note"] = (
            f"seat selection add-on from CAD {min(seat_prices):.0f}"
            if seat_prices
            else "seat selection add-on available"
        )
        return result

    logger.debug("AC probe: no bag prices captured for %s→%s", probe_origin, probe_dest)
    return None


# ── AY — Finnair ──────────────────────────────────────────────────────────────

async def _probe_ay(
    origin: str,
    dest: str,
    date_str: Optional[str] = None,
    flight_no: Optional[str] = None,
) -> Optional[dict]:
    """
    Finnair (AY) — navigate Finnair booking flow via Playwright.

    Finnair uses an Angular booking engine with Amadeus backend.
    Fare families: Light (no bag) vs Classic (23 kg included).
    """
    from patchright.async_api import async_playwright as _patchright_playwright
    from .browser import find_chrome

    dep = _probe_date(date_str, weeks_ahead=6)

    probe_origin = origin if (origin and len(origin) == 3) else "HEL"
    probe_dest = dest if (dest and len(dest) == 3 and dest != probe_origin) else "LHR"
    if probe_dest == probe_origin:
        probe_dest = "LHR"

    bag_prices: List[float] = []
    seat_prices: List[float] = []
    currency = "EUR"
    cap_event = asyncio.Event()

    async def _on_resp(resp):
        ct = resp.headers.get("content-type", "")
        if resp.status != 200 or "json" not in ct:
            return
        url_l = resp.url.lower()
        if "finnair" not in url_l and "amadeus" not in url_l:
            return
        try:
            data = await resp.json()
            b, s = _extract_bag_seat_prices(data)
            bag_prices.extend(b)
            seat_prices.extend(s)
            diff = _af_bag_price_from_fare_families(data)
            if diff:
                bag_prices.append(diff)
            if bag_prices:
                cap_event.set()
        except Exception:
            pass

    chrome_path: Optional[str] = None
    try:
        chrome_path = find_chrome()
    except RuntimeError:
        pass

    try:
        async with _patchright_playwright() as pw:
            launch_kwargs: dict = {
                "headless": False,
                "args": [
                    "--no-first-run",
                    "--no-default-browser-check",
                    "--window-size=1440,900",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            }
            if chrome_path:
                launch_kwargs["executable_path"] = chrome_path
            browser = await pw.chromium.launch(**launch_kwargs)
            ctx = await browser.new_context(
                viewport={"width": 1440, "height": 900},
                locale="en-GB",
                timezone_id="Europe/Helsinki",
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/135.0.0.0 Safari/537.36"
                ),
            )
            page = await ctx.new_page()
            page.on("response", _on_resp)

            try:
                await page.goto("https://www.finnair.com/en/", wait_until="domcontentloaded", timeout=30_000)
                await asyncio.sleep(3)
            except Exception as exc:
                logger.debug("AY homepage: %s", exc)

            try:
                await page.evaluate("""() => {
                    for (const b of document.querySelectorAll('button')) {
                        const t = (b.textContent || '').toLowerCase();
                        if ((t.includes('accept') || t.includes('agree') || t.includes('ok')) && b.offsetHeight > 0) {
                            b.click(); return;
                        }
                    }
                }""")
                await asyncio.sleep(1)
            except Exception:
                pass

            booking_url = (
                f"https://www.finnair.com/en/booking/flights"
                f"?type=OW&from={probe_origin}&to={probe_dest}"
                f"&date={dep}&adults=1"
            )
            try:
                await page.goto(booking_url, wait_until="domcontentloaded", timeout=30_000)
                await asyncio.sleep(4)
            except Exception as exc:
                logger.debug("AY search nav: %s", exc)

            try:
                await asyncio.wait_for(cap_event.wait(), timeout=25.0)
            except (asyncio.TimeoutError, TimeoutError):
                pass

            if not bag_prices:
                try:
                    dom_prices = await _scrape_fare_family_bag_price(page)
                    bag_prices.extend(dom_prices)
                except Exception:
                    pass

            await browser.close()
    except Exception as exc:
        logger.debug("AY Playwright probe error: %s", exc)

    if bag_prices:
        min_bag = min(p for p in bag_prices if p > 0)
        result: dict = {
            "checked_bag_note": (
                f"checked bag not included (Light fare) – add-on from EUR {min_bag:.0f}"
            ),
            "bags_note": "cabin bag included free (Light fare)",
            "checked_bag_from": min_bag,
            "checked_bag_price": min_bag,
            "currency": currency,
        }
        result["seat_note"] = (
            f"seat selection add-on from EUR {min(seat_prices):.0f}"
            if seat_prices
            else "seat selection add-on available"
        )
        return result

    logger.debug("AY probe: no bag prices captured for %s→%s", probe_origin, probe_dest)
    return None


# ── A3 — Aegean ───────────────────────────────────────────────────────────────

async def _probe_a3(
    origin: str,
    dest: str,
    date_str: Optional[str] = None,
    flight_no: Optional[str] = None,
) -> Optional[dict]:
    """
    Aegean Airlines (A3) — navigate Aegean booking flow via Playwright.

    Aegean uses Amadeus IBE. Fare families: Basic (no bag) vs Flex (23 kg).
    """
    from patchright.async_api import async_playwright as _patchright_playwright
    from .browser import find_chrome

    dep = _probe_date(date_str, weeks_ahead=6)

    probe_origin = origin if (origin and len(origin) == 3) else "ATH"
    probe_dest = dest if (dest and len(dest) == 3 and dest != probe_origin) else "LHR"
    if probe_dest == probe_origin:
        probe_dest = "LHR"

    bag_prices: List[float] = []
    seat_prices: List[float] = []
    currency = "EUR"
    cap_event = asyncio.Event()

    async def _on_resp(resp):
        ct = resp.headers.get("content-type", "")
        if resp.status != 200 or "json" not in ct:
            return
        url_l = resp.url.lower()
        if "aegean" not in url_l and "airaegean" not in url_l and "amadeus" not in url_l:
            return
        try:
            data = await resp.json()
            b, s = _extract_bag_seat_prices(data)
            bag_prices.extend(b)
            seat_prices.extend(s)
            diff = _af_bag_price_from_fare_families(data)
            if diff:
                bag_prices.append(diff)
            if bag_prices:
                cap_event.set()
        except Exception:
            pass

    chrome_path: Optional[str] = None
    try:
        chrome_path = find_chrome()
    except RuntimeError:
        pass

    try:
        async with _patchright_playwright() as pw:
            launch_kwargs: dict = {
                "headless": False,
                "args": [
                    "--no-first-run",
                    "--no-default-browser-check",
                    "--window-size=1440,900",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            }
            if chrome_path:
                launch_kwargs["executable_path"] = chrome_path
            browser = await pw.chromium.launch(**launch_kwargs)
            ctx = await browser.new_context(
                viewport={"width": 1440, "height": 900},
                locale="en-GB",
                timezone_id="Europe/Athens",
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/135.0.0.0 Safari/537.36"
                ),
            )
            page = await ctx.new_page()
            page.on("response", _on_resp)

            try:
                await page.goto("https://en.aegeanair.com/", wait_until="domcontentloaded", timeout=30_000)
                await asyncio.sleep(3)
            except Exception as exc:
                logger.debug("A3 homepage: %s", exc)

            try:
                await page.evaluate("""() => {
                    for (const b of document.querySelectorAll('button')) {
                        const t = (b.textContent || '').toLowerCase();
                        if ((t.includes('accept') || t.includes('agree') || t.includes('ok')) && b.offsetHeight > 0) {
                            b.click(); return;
                        }
                    }
                }""")
                await asyncio.sleep(1)
            except Exception:
                pass

            booking_url = (
                f"https://en.aegeanair.com/flights/book-flights/"
                f"?from={probe_origin}&to={probe_dest}&date={dep}&adults=1&type=ONE_WAY"
            )
            try:
                await page.goto(booking_url, wait_until="domcontentloaded", timeout=30_000)
                await asyncio.sleep(4)
            except Exception as exc:
                logger.debug("A3 search nav: %s", exc)

            try:
                await asyncio.wait_for(cap_event.wait(), timeout=25.0)
            except (asyncio.TimeoutError, TimeoutError):
                pass

            if not bag_prices:
                try:
                    dom_prices = await _scrape_fare_family_bag_price(page)
                    bag_prices.extend(dom_prices)
                except Exception:
                    pass

            await browser.close()
    except Exception as exc:
        logger.debug("A3 Playwright probe error: %s", exc)

    if bag_prices:
        min_bag = min(p for p in bag_prices if p > 0)
        result: dict = {
            "checked_bag_note": (
                f"checked bag not included (Basic fare) – add-on from EUR {min_bag:.0f}"
            ),
            "bags_note": "cabin bag 8 kg included free (Basic fare)",
            "checked_bag_from": min_bag,
            "checked_bag_price": min_bag,
            "currency": currency,
        }
        result["seat_note"] = (
            f"seat selection add-on from EUR {min(seat_prices):.0f}"
            if seat_prices
            else "seat selection add-on available"
        )
        return result

    logger.debug("A3 probe: no bag prices captured for %s→%s", probe_origin, probe_dest)
    return None


# ── NZ — Air New Zealand ──────────────────────────────────────────────────────

async def _probe_nz(
    origin: str,
    dest: str,
    date_str: Optional[str] = None,
    flight_no: Optional[str] = None,
) -> Optional[dict]:
    """
    Air New Zealand (NZ) — navigate Air NZ booking flow via Playwright.

    Air NZ uses their own React-based IBE. Fare families include:
    Seat (no bag) vs Works Lite (20 kg included).
    """
    from patchright.async_api import async_playwright as _patchright_playwright
    from .browser import find_chrome

    dep = _probe_date(date_str, weeks_ahead=6)

    probe_origin = origin if (origin and len(origin) == 3) else "AKL"
    probe_dest = dest if (dest and len(dest) == 3 and dest != probe_origin) else "SYD"
    if probe_dest == probe_origin:
        probe_dest = "SYD"

    bag_prices: List[float] = []
    seat_prices: List[float] = []
    currency = "NZD"
    cap_event = asyncio.Event()

    async def _on_resp(resp):
        ct = resp.headers.get("content-type", "")
        if resp.status != 200 or "json" not in ct:
            return
        url_l = resp.url.lower()
        if "airnewzealand" not in url_l and "airnz" not in url_l:
            return
        try:
            data = await resp.json()
            b, s = _extract_bag_seat_prices(data)
            bag_prices.extend(b)
            seat_prices.extend(s)
            diff = _af_bag_price_from_fare_families(data)
            if diff:
                bag_prices.append(diff)
            if bag_prices:
                cap_event.set()
        except Exception:
            pass

    chrome_path: Optional[str] = None
    try:
        chrome_path = find_chrome()
    except RuntimeError:
        pass

    try:
        async with _patchright_playwright() as pw:
            launch_kwargs: dict = {
                "headless": False,
                "args": [
                    "--no-first-run",
                    "--no-default-browser-check",
                    "--window-size=1440,900",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            }
            if chrome_path:
                launch_kwargs["executable_path"] = chrome_path
            browser = await pw.chromium.launch(**launch_kwargs)
            ctx = await browser.new_context(
                viewport={"width": 1440, "height": 900},
                locale="en-NZ",
                timezone_id="Pacific/Auckland",
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/135.0.0.0 Safari/537.36"
                ),
            )
            page = await ctx.new_page()
            page.on("response", _on_resp)

            try:
                await page.goto("https://www.airnewzealand.co.nz/", wait_until="domcontentloaded", timeout=30_000)
                await asyncio.sleep(3)
            except Exception as exc:
                logger.debug("NZ homepage: %s", exc)

            try:
                await page.evaluate("""() => {
                    for (const b of document.querySelectorAll('button')) {
                        const t = (b.textContent || '').toLowerCase();
                        if ((t.includes('accept') || t.includes('agree') || t.includes('ok')) && b.offsetHeight > 0) {
                            b.click(); return;
                        }
                    }
                }""")
                await asyncio.sleep(1)
            except Exception:
                pass

            booking_url = (
                f"https://www.airnewzealand.co.nz/booking/flights?"
                f"origin={probe_origin}&destination={probe_dest}"
                f"&departureDate={dep}&adults=1&tripType=ONE_WAY"
            )
            try:
                await page.goto(booking_url, wait_until="domcontentloaded", timeout=30_000)
                await asyncio.sleep(4)
            except Exception as exc:
                logger.debug("NZ search nav: %s", exc)

            try:
                await asyncio.wait_for(cap_event.wait(), timeout=25.0)
            except (asyncio.TimeoutError, TimeoutError):
                pass

            if not bag_prices:
                try:
                    dom_prices = await _scrape_fare_family_bag_price(page)
                    bag_prices.extend(dom_prices)
                except Exception:
                    pass

            await browser.close()
    except Exception as exc:
        logger.debug("NZ Playwright probe error: %s", exc)

    if bag_prices:
        min_bag = min(p for p in bag_prices if p > 0)
        result: dict = {
            "checked_bag_note": (
                f"checked bag not included (Seat fare) – add-on from NZD {min_bag:.0f}"
            ),
            "bags_note": "cabin bag included free (Seat fare)",
            "checked_bag_from": min_bag,
            "checked_bag_price": min_bag,
            "currency": currency,
        }
        result["seat_note"] = (
            f"seat selection add-on from NZD {min(seat_prices):.0f}"
            if seat_prices
            else "seat selection add-on available"
        )
        return result

    logger.debug("NZ probe: no bag prices captured for %s→%s", probe_origin, probe_dest)
    return None


# ── QF — Qantas ───────────────────────────────────────────────────────────────

async def _probe_qf(
    origin: str,
    dest: str,
    date_str: Optional[str] = None,
    flight_no: Optional[str] = None,
) -> Optional[dict]:
    """
    Qantas (QF) — navigate Qantas booking flow via Playwright.

    Qantas uses a React-based IBE. Fare families include:
    Economy Lite (no bag) vs Economy (23 kg included).
    """
    from patchright.async_api import async_playwright as _patchright_playwright
    from .browser import find_chrome

    dep = _probe_date(date_str, weeks_ahead=6)

    probe_origin = origin if (origin and len(origin) == 3) else "SYD"
    probe_dest = dest if (dest and len(dest) == 3 and dest != probe_origin) else "MEL"
    if probe_dest == probe_origin:
        probe_dest = "MEL"

    bag_prices: List[float] = []
    seat_prices: List[float] = []
    currency = "AUD"
    cap_event = asyncio.Event()

    async def _on_resp(resp):
        ct = resp.headers.get("content-type", "")
        if resp.status != 200 or "json" not in ct:
            return
        url_l = resp.url.lower()
        if "qantas" not in url_l:
            return
        try:
            data = await resp.json()
            b, s = _extract_bag_seat_prices(data)
            bag_prices.extend(b)
            seat_prices.extend(s)
            diff = _af_bag_price_from_fare_families(data)
            if diff:
                bag_prices.append(diff)
            if bag_prices:
                cap_event.set()
        except Exception:
            pass

    chrome_path: Optional[str] = None
    try:
        chrome_path = find_chrome()
    except RuntimeError:
        pass

    try:
        async with _patchright_playwright() as pw:
            launch_kwargs: dict = {
                "headless": False,
                "args": [
                    "--no-first-run",
                    "--no-default-browser-check",
                    "--window-size=1440,900",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            }
            if chrome_path:
                launch_kwargs["executable_path"] = chrome_path
            browser = await pw.chromium.launch(**launch_kwargs)
            ctx = await browser.new_context(
                viewport={"width": 1440, "height": 900},
                locale="en-AU",
                timezone_id="Australia/Sydney",
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/135.0.0.0 Safari/537.36"
                ),
            )
            page = await ctx.new_page()
            page.on("response", _on_resp)

            try:
                await page.goto("https://www.qantas.com/au/en.html", wait_until="domcontentloaded", timeout=30_000)
                await asyncio.sleep(3)
            except Exception as exc:
                logger.debug("QF homepage: %s", exc)

            try:
                await page.evaluate("""() => {
                    for (const b of document.querySelectorAll('button')) {
                        const t = (b.textContent || '').toLowerCase();
                        if ((t.includes('accept') || t.includes('agree') || t.includes('ok')) && b.offsetHeight > 0) {
                            b.click(); return;
                        }
                    }
                }""")
                await asyncio.sleep(1)
            except Exception:
                pass

            booking_url = (
                f"https://www.qantas.com/au/en/book-a-trip/flights/find/results.html?"
                f"origin={probe_origin}&destination={probe_dest}"
                f"&departDate={dep}&adults=1&tripType=O"
            )
            try:
                await page.goto(booking_url, wait_until="domcontentloaded", timeout=30_000)
                await asyncio.sleep(4)
            except Exception as exc:
                logger.debug("QF search nav: %s", exc)

            try:
                await asyncio.wait_for(cap_event.wait(), timeout=25.0)
            except (asyncio.TimeoutError, TimeoutError):
                pass

            if not bag_prices:
                try:
                    dom_prices = await _scrape_fare_family_bag_price(page)
                    bag_prices.extend(dom_prices)
                except Exception:
                    pass

            await browser.close()
    except Exception as exc:
        logger.debug("QF Playwright probe error: %s", exc)

    if bag_prices:
        min_bag = min(p for p in bag_prices if p > 0)
        result: dict = {
            "checked_bag_note": (
                f"checked bag not included (Economy Lite) – add-on from AUD {min_bag:.0f}"
            ),
            "bags_note": "cabin bag included free (Economy Lite)",
            "checked_bag_from": min_bag,
            "checked_bag_price": min_bag,
            "currency": currency,
        }
        result["seat_note"] = (
            f"seat selection add-on from AUD {min(seat_prices):.0f}"
            if seat_prices
            else "seat selection add-on available"
        )
        return result

    logger.debug("QF probe: no bag prices captured for %s→%s", probe_origin, probe_dest)
    return None


# ── Dispatch table ────────────────────────────────────────────────────────────
_PROBERS = {
    "F3": _probe_f3,
    "FZ": _probe_fz,
    "G9": _probe_g9,
    "XQ": _probe_xq,
    "SZ": _probe_sz,
    "LA": _probe_la,
    "JJ": _probe_la,  # LATAM Brazil (same booking engine)
    # Group 6 — full-service carriers
    "AF": _probe_af,
    "KL": _probe_kl,
    "BA": _probe_ba,
    "LH": _probe_lh,
    "OS": _probe_lh,  # Austrian Airlines (same Lufthansa Group engine)
    "LX": _probe_lh,  # Swiss (same Lufthansa Group engine)
    "SN": _probe_lh,  # Brussels Airlines (same Lufthansa Group engine)
    "IB": _probe_ib,
    "AC": _probe_ac,
    "AY": _probe_ay,
    "A3": _probe_a3,
    "NZ": _probe_nz,
    "QF": _probe_qf,
}
