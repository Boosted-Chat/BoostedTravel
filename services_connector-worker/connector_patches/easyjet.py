"""
easyJet patchright connector -- Cloud Run patch.

Replaces CDP Chrome with Patchright headed browser (via Xvfb) to bypass
Akamai WAF that detects CDP automation indicators. Form fill logic and
API response interception are unchanged from the SDK.

Strategy:
1. Launch Patchright headed Chrome (binary-level anti-detection).
2. Navigate to easyJet homepage, fill search form.
3. Intercept POST /funnel/api/query response.
4. Parse journeyPairs -> FlightOffers.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
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
from .airline_routes import get_city_airports

logger = logging.getLogger(__name__)

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1920, "height": 1080},
]


async def _launch_browser(proxy_url: str | None = None):
    """Launch a Patchright browser using the system Chrome binary.

    Returns (pw, browser, context, page).
    """
    from patchright.async_api import async_playwright
    from .browser import find_chrome, inject_stealth_js, auto_block_if_proxied

    proxy = None
    _BYPASS = ".google.com,.googletagmanager.com,.gstatic.com,.googleapis.com,.google-analytics.com,.googlesyndication.com,.doubleclick.net"
    letsfg_proxy = proxy_url or os.environ.get("LETSFG_PROXY", "").strip()
    if letsfg_proxy:
        import socket as _sock
        try:
            _s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
            _s.connect(("127.0.0.1", 8899))
            _s.close()
            proxy = {"server": "http://127.0.0.1:8899", "bypass": _BYPASS}
            logger.info("easyJet: using proxy relay on port 8899")
        except OSError:
            from urllib.parse import urlparse
            p = urlparse(letsfg_proxy)
            proxy = {"server": f"{p.scheme}://{p.hostname}:{p.port}", "bypass": _BYPASS}
            if p.username:
                proxy["username"] = p.username
                proxy["password"] = p.password or ""
            logger.info("easyJet: using direct proxy %s:%s", p.hostname, p.port)
    else:
        logger.info("easyJet: no proxy, direct connection")

    try:
        chrome_path = find_chrome()
        logger.info("easyJet: using system Chrome at %s", chrome_path)
    except RuntimeError:
        chrome_path = None

    pw = await async_playwright().start()
    vp = random.choice(_VIEWPORTS)
    launch_args = [
        "--disable-blink-features=AutomationControlled",
        "--disable-dev-shm-usage",
        "--no-first-run",
        "--no-default-browser-check",
        f"--window-size={vp['width']},{vp['height']}",
    ]
    launch_kwargs = dict(
        headless=False,
        args=launch_args,
        proxy=proxy,
    )
    if chrome_path:
        launch_kwargs["executable_path"] = chrome_path
    browser = await pw.chromium.launch(**launch_kwargs)
    context = await browser.new_context(
        viewport=vp,
        locale="en-US",
        timezone_id="Europe/London",
        color_scheme="light",
    )
    page = await context.new_page()

    await inject_stealth_js(page)
    await auto_block_if_proxied(page)

    return pw, browser, context, page


async def _dismiss_cookies(page) -> None:
    """Remove cookie banners and overlays."""
    for label in ["Accept", "Accept all", "Accept All Cookies", "I agree", "Got it", "OK"]:
        try:
            btn = page.get_by_role("button", name=label)
            if await btn.count() > 0:
                await btn.first.click(timeout=2000)
                logger.info("easyJet: clicked cookie accept button '%s'", label)
                await asyncio.sleep(0.5)
                break
        except Exception:
            continue
    try:
        await page.evaluate("""() => {
            const ids = ['ensBannerBG', 'ensNotifyBanner', 'onetrust-consent-sdk',
                          'ensCloseBanner', 'ens-banner-overlay'];
            ids.forEach(id => { const el = document.getElementById(id); if (el) el.remove(); });
            document.querySelectorAll(
                '.ens-banner, [class*="cookie-banner"], [class*="consent"], ' +
                '[class*="CookieBanner"], [id*="cookie"], [id*="consent"], ' +
                '[class*="overlay"][style*="z-index"]'
            ).forEach(el => { if (el.offsetHeight > 0) el.remove(); });
            document.querySelectorAll(
                '.modal-lightbox-wrapper, .account-modal, .modal__dialog-wrapper'
            ).forEach(el => el.remove());
        }""")
    except Exception:
        pass


class EasyjetConnectorClient:
    """easyJet Patchright headed Chrome -- form fill + API response interception."""

    def __init__(self, timeout: float = 30.0):
        self.timeout = timeout
        self._pw = None
        self._browser = None

    async def close(self):
        if self._browser:
            try:
                await self._browser.close()
            except Exception:
                pass
            self._browser = None
        if self._pw:
            try:
                await self._pw.stop()
            except Exception:
                pass
            self._pw = None

    _EASYJET_AIRPORTS: frozenset[str] = frozenset({
        "LGW", "LTN", "STN", "BRS", "MAN", "EDI", "GLA", "LPL",
        "BHX", "NCL", "BFS", "ABZ", "INV", "EMA", "SOU",
        "CDG", "ORY", "LYS", "NCE", "TLS", "BOD", "NTE", "MRS",
        "AMS", "BER", "MXP", "FCO", "NAP", "VCE", "PSA", "BRI",
        "BCN", "MAD", "AGP", "ALC", "PMI", "IBZ", "SVQ", "FUE",
        "ACE", "LPA", "TFS", "FAO", "LIS", "OPO", "GVA", "BSL",
        "CPH", "PRG", "BUD", "KRK", "WRO", "VIE",
        "ATH", "HER", "CFU", "RHO", "JTR", "ZTH", "SKG",
        "SPU", "DBV", "ZAG", "LJU", "TLV", "IST", "SAW",
        "ESB", "ADB", "AYT", "DLM", "BJV", "HRG", "SSH",
        "MRK", "RAK",
    })

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        ob_offers, ob_currency = await self._search_fanout(req)

        if req.return_from and ob_offers:
            ib_req = req.model_copy(update={
                "origin": req.destination, "destination": req.origin,
                "date_from": req.return_from, "return_from": None,
            })
            ib_offers, _ = await self._search_fanout(ib_req)
            if ib_offers:
                ob_offers = self._combine_rt(ob_offers, ib_offers, req)

        search_hash = hashlib.md5(
            f"easyjet{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]

        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=ob_currency,
            offers=ob_offers,
            total_results=len(ob_offers),
        )

    async def _search_fanout(self, req: FlightSearchRequest) -> tuple[list[FlightOffer], str]:
        origins = get_city_airports(req.origin)
        dests = get_city_airports(req.destination)
        origins = [a for a in origins if a in self._EASYJET_AIRPORTS] or origins
        dests = [a for a in dests if a in self._EASYJET_AIRPORTS] or dests

        all_offers: list[FlightOffer] = []
        seen_hashes: set[str] = set()
        currency = "GBP"
        pairs = [(o, d) for o in origins for d in dests if o != d]
        if not pairs:
            return [], currency

        for o, d in pairs:
            sub_req = req.model_copy(update={"origin": o, "destination": d})
            try:
                resp = await self._search_single(sub_req)
                if resp and resp.offers:
                    currency = resp.currency or currency
                    for offer in resp.offers:
                        segs = offer.outbound.segments if offer.outbound else []
                        route = "-".join(s.origin for s in segs) if segs else ""
                        h = f"{offer.price}-{route}"
                        if h not in seen_hashes:
                            seen_hashes.add(h)
                            all_offers.append(offer)
            except Exception as e:
                logger.warning("easyJet: %s->%s failed: %s", o, d, e)

        all_offers.sort(key=lambda o: o.price)
        return all_offers, currency

    async def _search_single(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """Search easyJet for a single pair using Patchright headed Chrome."""
        t0 = time.monotonic()
        pw = browser = context = page = results_page = None

        try:
            pw, browser, context, page = await _launch_browser()
            self._pw = pw
            self._browser = browser

            search_data: dict = {}
            akamai_blocked = False

            async def _on_response(response):
                nonlocal akamai_blocked
                url = response.url
                if (
                    "/funnel/api/query" in url
                    and "auth-status" not in url
                    and "search/airports" not in url
                    and "/stats" not in url
                ):
                    status = response.status
                    if status == 403:
                        akamai_blocked = True
                        logger.warning("easyJet: Akamai 403 on /funnel/api/query")
                        return
                    if status == 200:
                        try:
                            data = await response.json()
                            if isinstance(data, dict) and "journeyPairs" in data:
                                search_data.update(data)
                                logger.info("easyJet: captured search API response")
                        except Exception as e:
                            logger.warning("easyJet: failed to parse API response: %s", e)

            page.on("response", _on_response)

            logger.info("easyJet: loading homepage for %s->%s", req.origin, req.destination)
            await page.goto(
                "https://www.easyjet.com/en/",
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            await asyncio.sleep(random.uniform(2.5, 4.0))

            # Let Akamai sensors settle — wait for network idle + add human-like behavior
            try:
                await page.wait_for_load_state("networkidle", timeout=10000)
            except Exception:
                pass

            await _dismiss_cookies(page)
            await asyncio.sleep(random.uniform(0.5, 1.0))
            await _dismiss_cookies(page)

            # Human-like: random scroll + mouse movement to pass behavioral analysis
            try:
                await page.mouse.move(
                    random.randint(200, 800), random.randint(200, 400)
                )
                await asyncio.sleep(random.uniform(0.3, 0.7))
                await page.evaluate("window.scrollBy(0, %d)" % random.randint(50, 200))
                await asyncio.sleep(random.uniform(0.5, 1.0))
                await page.evaluate("window.scrollTo(0, 0)")
                await asyncio.sleep(random.uniform(0.3, 0.6))
            except Exception:
                pass

            ok = await self._fill_search_form(page, req)
            if not ok:
                logger.warning("easyJet: form fill failed, aborting")
                return self._empty(req)

            new_page_event = asyncio.Event()
            new_page_ref: list = [None]

            def _on_new_page(p):
                new_page_ref[0] = p
                new_page_event.set()

            context.on("page", _on_new_page)
            await asyncio.sleep(random.uniform(0.5, 1.5))

            try:
                await page.get_by_role("button", name="Show flights").click(timeout=5000)
                logger.info("easyJet: clicked 'Show flights', waiting for results")
            except Exception as e:
                logger.warning("easyJet: could not click 'Show flights': %s", e)
                return self._empty(req)

            try:
                await asyncio.wait_for(new_page_event.wait(), timeout=10.0)
                results_page = new_page_ref[0]
                if results_page:
                    results_page.on("response", _on_response)
                    logger.info("easyJet: results tab opened: %s", results_page.url)
                    try:
                        await results_page.wait_for_load_state("domcontentloaded", timeout=15000)
                    except Exception:
                        pass
            except (asyncio.TimeoutError, Exception):
                logger.info("easyJet: no new tab, checking current page: %s", page.url)
                try:
                    await page.wait_for_url("**/buy/flights**", timeout=5000)
                except Exception:
                    pass

            remaining = max(self.timeout - (time.monotonic() - t0), 10)
            deadline = time.monotonic() + remaining
            while not search_data and not akamai_blocked and time.monotonic() < deadline:
                await asyncio.sleep(0.5)

            if akamai_blocked:
                logger.warning("easyJet: Akamai flagged session, trying DOM scrape fallback")
                # Try DOM scraping on whatever page shows results
                active_page = results_page or page
                dom_offers = await self._scrape_dom_fallback(active_page, req)
                if dom_offers:
                    elapsed = time.monotonic() - t0
                    search_hash = hashlib.md5(
                        f"easyjet{req.origin}{req.destination}{req.date_from}".encode()
                    ).hexdigest()[:12]
                    return FlightSearchResponse(
                        search_id=f"fs_{search_hash}",
                        origin=req.origin, destination=req.destination,
                        currency="GBP", offers=dom_offers,
                        total_results=len(dom_offers),
                    )
                return self._empty(req)

            if not search_data or not search_data.get("journeyPairs"):
                logger.warning("easyJet: no journeyPairs in intercepted response")
                return self._empty(req)

            currency = search_data.get("metaData", {}).get("currencyCode", "GBP")
            offers = self._parse_journey_pairs(search_data["journeyPairs"], req, currency)
            offers.sort(key=lambda o: o.price)

            elapsed = time.monotonic() - t0
            logger.info(
                "easyJet %s->%s returned %d offers in %.1fs (patchright)",
                req.origin, req.destination, len(offers), elapsed,
            )

            search_hash = hashlib.md5(
                f"easyjet{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
            ).hexdigest()[:12]
            return FlightSearchResponse(
                search_id=f"fs_{search_hash}",
                origin=req.origin,
                destination=req.destination,
                currency=currency,
                offers=offers,
                total_results=len(offers),
            )
        except Exception as e:
            logger.error("easyJet patchright error: %s", e)
            return self._empty(req)
        finally:
            for p in [results_page, page]:
                try:
                    if p:
                        await p.close()
                except Exception:
                    pass
            try:
                if context:
                    await context.close()
            except Exception:
                pass
            try:
                if browser:
                    await browser.close()
            except Exception:
                pass
            try:
                if pw:
                    await pw.stop()
            except Exception:
                pass
            self._pw = None
            self._browser = None

    # -- Form interaction --------------------------------------------------

    async def _fill_search_form(self, page, req: FlightSearchRequest) -> bool:
        ok = await self._fill_airport_field(page, "From", req.origin)
        if not ok:
            return False
        await asyncio.sleep(random.uniform(0.7, 1.3))
        ok = await self._fill_airport_field(page, "To", req.destination)
        if not ok:
            return False
        await asyncio.sleep(random.uniform(0.7, 1.3))
        ok = await self._fill_date(page, req)
        if not ok:
            return False
        return True

    _IATA_NAMES: dict[str, str] = {
        "LGW": "London Gatwick", "LTN": "London Luton", "STN": "London Stansted",
        "BRS": "Bristol", "MAN": "Manchester", "EDI": "Edinburgh",
        "GLA": "Glasgow", "LPL": "Liverpool", "BHX": "Birmingham",
        "NCL": "Newcastle", "BFS": "Belfast", "ABZ": "Aberdeen",
        "INV": "Inverness", "EMA": "East Midlands", "SOU": "Southampton",
        "CDG": "Paris Charles de Gaulle", "ORY": "Paris Orly",
        "AMS": "Amsterdam", "BER": "Berlin", "BCN": "Barcelona",
        "MAD": "Madrid", "LIS": "Lisbon", "FCO": "Rome Fiumicino",
        "MXP": "Milan Malpensa", "NAP": "Naples", "VCE": "Venice",
        "GVA": "Geneva", "BSL": "Basel", "CPH": "Copenhagen",
        "PRG": "Prague", "BUD": "Budapest", "KRK": "Krakow",
        "WRO": "Wroclaw", "VIE": "Vienna",
        "ATH": "Athens", "IST": "Istanbul", "AGP": "Malaga",
        "ALC": "Alicante", "PMI": "Palma de Mallorca", "FAO": "Faro",
        "NCE": "Nice", "LYS": "Lyon", "TLS": "Toulouse",
        "BOD": "Bordeaux", "NTE": "Nantes", "MRS": "Marseille",
    }

    async def _fill_airport_field(self, page, label: str, iata: str) -> bool:
        result = await self._try_fill_airport(page, label, iata)
        if result:
            return True
        name = self._IATA_NAMES.get(iata)
        if name:
            logger.info("easyJet: retrying %s with name '%s'", label, name)
            result = await self._try_fill_airport(page, label, name, match_hint=iata)
            if result:
                return True
        logger.warning("easyJet: %s field -- no match for %s", label, iata)
        return False

    async def _try_fill_airport(self, page, label: str, query: str, match_hint: str = "") -> bool:
        hint = match_hint or query
        try:
            await page.evaluate("""() => {
                document.querySelectorAll(
                    '.modal-lightbox-wrapper, .account-modal, .modal__dialog-wrapper, ' +
                    '[class*="overlay"][style*="z-index"]'
                ).forEach(el => el.remove());
            }""")

            field = page.get_by_role("textbox", name=label)
            clear_name = "Clear selected departure airport" if label == "From" else "Clear selected destination airport"
            try:
                clear_btn = page.get_by_role("button", name=clear_name)
                if await clear_btn.count() > 0:
                    await clear_btn.click(timeout=2000)
                    await asyncio.sleep(random.uniform(0.3, 0.6))
            except Exception:
                pass

            await field.click(timeout=3000)
            await asyncio.sleep(random.uniform(0.3, 0.6))
            await field.type(query, delay=random.randint(80, 150))
            logger.info("easyJet: typed '%s' in %s field", query, label)
            await asyncio.sleep(random.uniform(3.0, 4.5))

            for role in ("option", "radio", "listitem"):
                try:
                    option = page.get_by_role(role, name=re.compile(
                        rf"{re.escape(hint)}", re.IGNORECASE
                    )).first
                    if await option.count() > 0:
                        await option.click(timeout=3000)
                        logger.info("easyJet: selected %s airport via %s role", hint, role)
                        await asyncio.sleep(0.3)
                        await page.keyboard.press("Escape")
                        await asyncio.sleep(0.3)
                        return True
                except Exception:
                    continue

            for sel in (
                f'[data-testid*="airport"] >> text=/{re.escape(hint)}/i',
                f'li:has-text("{hint}")',
                f'[role="listbox"] >> text=/{re.escape(hint)}/i',
                f'ul li >> text=/{re.escape(hint)}/i',
                f'button:has-text("{hint}")',
                f'a:has-text("{hint}")',
            ):
                try:
                    el = page.locator(sel).first
                    if await el.count() > 0:
                        await el.click(timeout=3000)
                        logger.info("easyJet: selected %s airport via locator", hint)
                        await asyncio.sleep(0.3)
                        await page.keyboard.press("Escape")
                        await asyncio.sleep(0.3)
                        return True
                except Exception:
                    continue

            try:
                clicked = await page.evaluate("""(term) => {
                    const walker = document.createTreeWalker(
                        document.body, NodeFilter.SHOW_ELEMENT, null
                    );
                    const regex = new RegExp('\\\\b' + term + '\\\\b', 'i');
                    const candidates = [];
                    while (walker.nextNode()) {
                        const el = walker.currentNode;
                        const rect = el.getBoundingClientRect();
                        if (rect.width === 0 || rect.height === 0) continue;
                        const style = getComputedStyle(el);
                        if (style.display === 'none' || style.visibility === 'hidden') continue;
                        const fullText = el.textContent || '';
                        if (regex.test(fullText) && (el.tagName === 'LI' || el.tagName === 'BUTTON'
                            || el.tagName === 'A' || el.getAttribute('role') === 'option'
                            || el.getAttribute('role') === 'listitem'
                            || el.classList.toString().match(/airport|result|suggest|item/i))) {
                            candidates.push(el);
                        }
                    }
                    candidates.sort((a, b) => a.textContent.length - b.textContent.length);
                    if (candidates.length > 0) {
                        candidates[0].click();
                        return true;
                    }
                    return false;
                }""", hint)
                if clicked:
                    logger.info("easyJet: selected %s airport via JS tree-walk", hint)
                    await asyncio.sleep(0.3)
                    await page.keyboard.press("Escape")
                    await asyncio.sleep(0.3)
                    return True
            except Exception:
                pass

            for sel in (
                '[role="listbox"] [role="option"]',
                '[class*="airport"] li',
                '[class*="dropdown"] li',
                '[class*="suggestion"] li',
                '[class*="result"] li',
            ):
                try:
                    item = page.locator(sel).first
                    if await item.count() > 0:
                        await item.click(timeout=3000)
                        logger.info("easyJet: selected first dropdown item for %s", hint)
                        await asyncio.sleep(0.3)
                        await page.keyboard.press("Escape")
                        await asyncio.sleep(0.3)
                        return True
                except Exception:
                    continue

            return False
        except Exception as e:
            logger.warning("easyJet: %s field error: %s", label, e)
            return False

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        target = req.date_from
        target_month_name = target.strftime("%B").upper()
        target_year = target.year
        target_month_year = f"{target_month_name} {target_year}"

        try:
            try:
                date_field = page.get_by_role("textbox", name="Clear selected travel date")
                if await date_field.count() == 0:
                    date_field = page.get_by_placeholder("Choose your dates")
                await date_field.click(timeout=3000)
            except Exception:
                when_section = page.locator("text=When").first
                await when_section.click(timeout=3000)
            await asyncio.sleep(0.5)

            try:
                await page.wait_for_selector('[data-testid="month-title"]', timeout=10000)
            except Exception:
                logger.warning("easyJet: calendar grid didn't load in time")
                return False
            await asyncio.sleep(0.5)

            testid = f"{target.day}-{target.month}-{target.year}"
            day_btn = page.locator(f'[data-testid="{testid}"]')
            aria_label = f"{target.strftime('%B')} {target.day}, {target.year}"
            day_btn_fallback = page.get_by_role("button", name=aria_label)

            import calendar
            month_order = {m.upper(): i for i, m in enumerate(calendar.month_name) if m}

            def parse_month_year(text: str) -> tuple[int, int]:
                parts = text.strip().split()
                if len(parts) >= 2:
                    return int(parts[1]), month_order.get(parts[0], 0)
                return (9999, 99)

            target_key = (target_year, target.month)

            for attempt in range(24):
                if await day_btn.count() > 0 or await day_btn_fallback.count() > 0:
                    break
                try:
                    visible_months = await page.evaluate("""() => {
                        const titles = document.querySelectorAll('[data-testid="month-title"]');
                        return Array.from(titles).map(t => t.textContent.trim().toUpperCase());
                    }""")

                    if any(target_month_year in m for m in visible_months):
                        await asyncio.sleep(0.5)
                        if await day_btn.count() > 0 or await day_btn_fallback.count() > 0:
                            break
                        break

                    if visible_months:
                        first_key = parse_month_year(visible_months[0])
                        if first_key > target_key:
                            try:
                                prev_btn = page.get_by_role("button", name="Previous month")
                                if await prev_btn.count() > 0:
                                    await prev_btn.click(timeout=2000)
                                    await asyncio.sleep(0.5)
                                    continue
                            except Exception:
                                pass
                            break
                        else:
                            try:
                                await page.get_by_role("button", name="Next month").click(timeout=2000)
                                await asyncio.sleep(0.5)
                            except Exception:
                                break
                    else:
                        try:
                            await page.get_by_role("button", name="Next month").click(timeout=2000)
                            await asyncio.sleep(0.5)
                        except Exception:
                            break
                except Exception:
                    try:
                        await page.get_by_role("button", name="Next month").click(timeout=2000)
                        await asyncio.sleep(0.5)
                    except Exception:
                        break

            if await day_btn.count() > 0:
                await day_btn.click(timeout=5000)
                logger.info("easyJet: clicked date %s (testid: %s)", target, testid)
            elif await day_btn_fallback.count() > 0:
                await day_btn_fallback.click(timeout=5000)
                logger.info("easyJet: clicked date %s (aria-label)", target)
            else:
                clicked = await page.evaluate("""(args) => {
                    const [testid, ariaLabel] = args;
                    let btn = document.querySelector(`[data-testid="${testid}"]`);
                    if (!btn) {
                        btn = Array.from(document.querySelectorAll('button'))
                            .find(b => b.getAttribute('aria-label') === ariaLabel);
                    }
                    if (btn) { btn.click(); return true; }
                    return false;
                }""", [testid, aria_label])
                if clicked:
                    logger.info("easyJet: clicked date %s (JS fallback)", target)
                else:
                    logger.warning("easyJet: could not find date button for %s", target)
                    return False

            await asyncio.sleep(0.5)
            await page.keyboard.press("Escape")
            await asyncio.sleep(0.3)
            return True
        except Exception as e:
            logger.warning("easyJet: date error: %s", e)
            return False

    # -- DOM scraping fallback (Akamai 403) --------------------------------

    async def _scrape_dom_fallback(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Attempt to extract flight data from the rendered page when API is blocked."""
        try:
            await asyncio.sleep(5.0)  # Wait for page to render whatever it can
            flights = await page.evaluate(r"""(params) => {
                const [origin, dest] = params;
                const results = [];
                // Look for flight cards in the DOM
                const cards = document.querySelectorAll(
                    '[class*="flight-card"], [class*="journey"], [class*="FlightCard"], ' +
                    '[data-testid*="flight"], [class*="flight-row"], [class*="flightInfo"]'
                );
                for (const card of cards) {
                    const text = card.innerText || '';
                    if (text.length < 20) continue;
                    const times = text.match(/\b(\d{1,2}:\d{2})\b/g) || [];
                    if (times.length < 2) continue;
                    const priceMatch = text.match(/[£€$]\s*([\d,.]+)/);
                    if (!priceMatch) continue;
                    const price = parseFloat(priceMatch[1].replace(',', ''));
                    if (isNaN(price) || price <= 0) continue;
                    results.push({
                        dep_time: times[0],
                        arr_time: times[1],
                        price: price,
                        text: text.replace(/\s+/g, ' ').slice(0, 200)
                    });
                }
                return results;
            }""", [req.origin, req.destination])

            if not flights:
                logger.info("easyJet: DOM scrape found no flight cards")
                return []

            logger.info("easyJet: DOM scrape found %d flight cards", len(flights))
            offers: list[FlightOffer] = []
            dt = req.date_from if hasattr(req.date_from, 'strftime') else datetime.strptime(str(req.date_from), "%Y-%m-%d")
            date_str = dt.strftime("%Y-%m-%d")

            for i, f in enumerate(flights):
                dep_time = f.get("dep_time", "00:00")
                arr_time = f.get("arr_time", "00:00")
                price = f.get("price", 0)
                offer_id = hashlib.md5(f"ej-dom-{req.origin}{req.destination}{date_str}{dep_time}{price}".encode()).hexdigest()[:12]
                offers.append(FlightOffer(
                    id=offer_id,
                    source="easyjet_direct",
                    price=price,
                    currency="GBP",
                    deep_link=f"https://www.easyjet.com/en/buy/flights?origin={req.origin}&destination={req.destination}&date={date_str}",
                    outbound=FlightRoute(
                        departure=f"{date_str}T{dep_time}:00",
                        arrival=f"{date_str}T{arr_time}:00",
                        duration_minutes=0,
                        stops=0,
                        segments=[FlightSegment(
                            origin=req.origin,
                            destination=req.destination,
                            departure=f"{date_str}T{dep_time}:00",
                            arrival=f"{date_str}T{arr_time}:00",
                            carrier="U2",
                            flight_number="",
                            duration_minutes=0,
                        )],
                    ),
                ))
            return offers
        except Exception as e:
            logger.warning("easyJet: DOM scrape failed: %s", e)
            return []

    # -- Parsing -----------------------------------------------------------

    def _parse_journey_pairs(
        self, journey_pairs: list, req: FlightSearchRequest, currency: str
    ) -> list[FlightOffer]:
        offers: list[FlightOffer] = []
        target_date = req.date_from.strftime("%Y-%m-%d")
        booking_url = self._build_booking_url(req)

        for pair in journey_pairs:
            outbound = pair.get("outbound", {})
            flights_by_date = outbound.get("flights", {})
            matched_dates = [dk for dk in flights_by_date if dk == target_date]
            if not matched_dates:
                matched_dates = list(flights_by_date.keys())
            for date_key in matched_dates:
                for flight in flights_by_date[date_key]:
                    offer = self._parse_single_flight(flight, currency, booking_url, req)
                    if offer:
                        offers.append(offer)
        return offers

    def _parse_single_flight(
        self, flight: dict, currency: str, booking_url: str, req: FlightSearchRequest
    ) -> Optional[FlightOffer]:
        if flight.get("soldOut") or flight.get("saleableStatus") != "AVAILABLE":
            return None

        fares = flight.get("fares", {})
        adt_fares = fares.get("ADT", {})
        price = None
        for fare_family in ["STANDARD", "FLEXI"]:
            fare = adt_fares.get(fare_family)
            if fare:
                unit_price = fare.get("unitPrice", {})
                gross = unit_price.get("grossPrice")
                if gross is not None:
                    if price is None or gross < price:
                        price = gross
                    break

        if price is None or price <= 0:
            return None

        flight_no = flight.get("flightNumber", "")
        carrier = flight.get("iataCarrierCode", "U2")
        if flight_no and not flight_no.startswith(carrier):
            flight_no = f"{carrier}{flight_no}"

        dep_str = flight.get("localDepartureDateTime", "")
        arr_str = flight.get("localArrivalDateTime", "")

        segment = FlightSegment(
            airline=carrier,
            airline_name="easyJet",
            flight_no=flight_no,
            origin=flight.get("departureAirportCode", ""),
            destination=flight.get("arrivalAirportCode", ""),
            departure=self._parse_dt(dep_str),
            arrival=self._parse_dt(arr_str),
            cabin_class={"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy"),
        )

        total_dur = int((segment.arrival - segment.departure).total_seconds())
        route = FlightRoute(
            segments=[segment],
            total_duration_seconds=max(total_dur, 0),
            stopovers=0,
        )
        key = f"{flight_no}_{dep_str}_{price}"

        return FlightOffer(
            id=f"ej_{hashlib.md5(key.encode()).hexdigest()[:12]}",
            price=round(price, 2),
            currency=currency,
            price_formatted=f"{price:.2f} {currency}",
            outbound=route,
            inbound=None,
            airlines=["easyJet"],
            owner_airline="U2",
            booking_url=booking_url,
            is_locked=False,
            source="easyjet_direct",
            source_tier="free",
        )

    def _build_booking_url(self, req: FlightSearchRequest) -> str:
        date_out = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://www.easyjet.com/en/buy/flights"
            f"?dep={req.origin}&dest={req.destination}"
            f"&dd={date_out}&isOneWay=on"
            f"&apax={req.adults}&cpax={req.children or 0}"
            f"&ipax={req.infants or 0}"
        )

    def _parse_dt(self, s: str) -> datetime:
        if not s:
            return datetime(2000, 1, 1)
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            try:
                return datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
            except Exception:
                return datetime(2000, 1, 1)

    @staticmethod
    def _combine_rt(ob: list, ib: list, req) -> list:
        combos = []
        for o in sorted(ob, key=lambda x: x.price)[:15]:
            for i in sorted(ib, key=lambda x: x.price)[:10]:
                combos.append(FlightOffer(
                    id=f"u2_rt_{o.id}_{i.id}",
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
        search_hash = hashlib.md5(
            f"easyjet{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=[],
            total_results=0,
        )