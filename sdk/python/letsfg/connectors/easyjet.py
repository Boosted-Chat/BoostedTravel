"""
easyJet CDP Chrome hybrid scraper — persistent browser + form navigation
+ API response interception.

easyJet's API (/funnel/api/query) is behind Akamai WAF — requires browser-level
session. Direct deep-link URLs redirect to homepage without a BFF session.
The search must be initiated via the homepage form to trigger the BFF.

Strategy (CDP Chrome + response interception):
1. Launch REAL system Chrome (--remote-debugging-port, --user-data-dir).
2. Connect via Playwright CDP. Browser context persists across searches.
3. Each search: new page → intercept → homepage → fill form → click search.
4. Capture POST /funnel/api/query response via page.on("response").
5. Parse journeyPairs → FlightOffers.

If Akamai flags the session (403), delete user-data-dir and restart Chrome
with a clean profile. Real Chrome bypasses fingerprinting where bundled
Chromium fails.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
import re
import shutil
import subprocess
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
from .browser import find_chrome, stealth_popen_kwargs, proxy_chrome_args, auto_block_if_proxied, inject_stealth_js, disable_background_networking_args, get_default_proxy
from .airline_routes import get_city_airports

logger = logging.getLogger(__name__)

_ancillary_cache: dict[str, tuple[float, dict]] = {}
_ANCILLARY_CACHE_TTL = 1800  # 30 min


def _extract_hhmm(value: object) -> str:
    if isinstance(value, datetime):
        return value.strftime("%H:%M")
    text = str(value or "").strip()
    if len(text) >= 16:
        return text[11:16]
    if len(text) >= 5 and text[2] == ":" and text[:2].isdigit() and text[3:5].isdigit():
        return text[:5]
    return ""

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]

_DEBUG_PORT = 9450
_USER_DATA_DIR = os.path.join(
    os.environ.get("TEMP", os.environ.get("TMPDIR", "/tmp")), ".easyjet_chrome_data"
)

_pw_instance = None
_browser = None
_chrome_proc = None
_browser_lock: Optional[asyncio.Lock] = None
_context = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_context():
    """Get or create a persistent browser context (shares cookies across searches)."""
    global _context
    browser = await _get_browser()
    if _context:
        try:
            # Check context is still alive
            if _context.pages:
                return _context
        except Exception:
            pass
    contexts = browser.contexts
    if contexts:
        _context = contexts[0]
    else:
        _context = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
        )
    return _context


async def _get_browser():
    """Launch real Chrome via CDP (headed — Akamai blocks headless)."""
    global _pw_instance, _browser, _chrome_proc
    lock = _get_lock()
    async with lock:
        if _browser:
            try:
                if _browser.is_connected():
                    return _browser
            except Exception:
                pass

        from playwright.async_api import async_playwright
        from .browser import find_chrome, stealth_popen_kwargs, _launched_procs

        # Try connecting to existing Chrome on the port first
        pw = None
        try:
            pw = await async_playwright().start()
            _browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{_DEBUG_PORT}")
            _pw_instance = pw
            logger.info("easyJet: connected to existing Chrome on port %d", _DEBUG_PORT)
            return _browser
        except Exception:
            if pw:
                try:
                    await pw.stop()
                except Exception:
                    pass

        # Launch Chrome HEADED (no --headless) — Akamai 403s headless Chrome.
        # Off-screen + minimised so it doesn't disturb the user.
        chrome = find_chrome()
        os.makedirs(_USER_DATA_DIR, exist_ok=True)
        args = [
            chrome,
            f"--remote-debugging-port={_DEBUG_PORT}",
            f"--user-data-dir={_USER_DATA_DIR}",
            "--no-first-run",
            *proxy_chrome_args(),
            *disable_background_networking_args(),
            "--no-default-browser-check",
            "--disable-blink-features=AutomationControlled",
            "--disable-http2",
            "--window-position=-2400,-2400",
            "--window-size=1366,768",
            "about:blank",
        ]
        _chrome_proc = subprocess.Popen(args, **stealth_popen_kwargs())
        _launched_procs.append(_chrome_proc)
        await asyncio.sleep(2.0)

        pw = await async_playwright().start()
        _pw_instance = pw
        _browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{_DEBUG_PORT}")
        logger.info("easyJet: Chrome launched headed on CDP port %d (pid %d)", _DEBUG_PORT, _chrome_proc.pid)
        return _browser


async def _dismiss_cookies(page) -> None:
    """Remove ensighten / cookie banners and account modals that block interaction."""
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
            // Remove cookie banners
            const ids = ['ensBannerBG', 'ensNotifyBanner', 'onetrust-consent-sdk',
                          'ensCloseBanner', 'ens-banner-overlay'];
            ids.forEach(id => { const el = document.getElementById(id); if (el) el.remove(); });
            document.querySelectorAll(
                '.ens-banner, [class*="cookie-banner"], [class*="consent"], ' +
                '[class*="CookieBanner"], [id*="cookie"], [id*="consent"], ' +
                '[class*="overlay"][style*="z-index"]'
            ).forEach(el => { if (el.offsetHeight > 0) el.remove(); });
            // Remove account modals that intercept pointer events
            document.querySelectorAll(
                '.modal-lightbox-wrapper, .account-modal, .modal__dialog-wrapper'
            ).forEach(el => el.remove());
        }""")
    except Exception:
        pass


async def _reset_chrome_profile():
    """Kill Chrome and wipe user-data-dir to clear Akamai-flagged sessions."""
    global _browser, _chrome_proc, _context
    try:
        if _browser:
            await _browser.close()
    except Exception:
        pass
    _browser = None
    _context = None
    if _chrome_proc:
        try:
            _chrome_proc.terminate()
        except Exception:
            pass
        _chrome_proc = None
    if os.path.isdir(_USER_DATA_DIR):
        try:
            shutil.rmtree(_USER_DATA_DIR)
            logger.info("easyJet: deleted stale Chrome profile %s", _USER_DATA_DIR)
        except Exception as e:
            logger.warning("easyJet: failed to delete Chrome profile: %s", e)


async def _launch_patchright_browser():
    """Launch a Patchright browser using the local Chrome binary when available."""
    from patchright.async_api import async_playwright

    proxy = get_default_proxy()
    try:
        chrome_path = find_chrome()
    except RuntimeError:
        chrome_path = None

    pw = await async_playwright().start()
    viewport = random.choice(_VIEWPORTS)
    launch_kwargs = {
        "headless": False,
        "args": [
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--no-first-run",
            "--no-default-browser-check",
            f"--window-size={viewport['width']},{viewport['height']}",
        ],
        "proxy": proxy,
    }
    if chrome_path:
        launch_kwargs["executable_path"] = chrome_path

    browser = await pw.chromium.launch(**launch_kwargs)
    context = await browser.new_context(
        viewport=viewport,
        locale="en-US",
        timezone_id="Europe/London",
        color_scheme="light",
    )
    page = await context.new_page()
    await inject_stealth_js(page)
    await auto_block_if_proxied(page)
    return pw, browser, context, page


class EasyjetConnectorClient:
    """easyJet CDP Chrome scraper — persistent browser + form + API response interception."""

    def __init__(self, timeout: float = 30.0):
        self.timeout = timeout

    async def close(self):
        pass  # Browser is shared singleton

    # easyJet airports — used to skip airports easyJet doesn't serve
    # (e.g. LHR, LCY, SEN) so the fan-out only tries relevant ones.
    _EASYJET_AIRPORTS: frozenset[str] = frozenset({
        # UK
        "LGW", "LTN", "STN", "BRS", "MAN", "EDI", "GLA", "LPL",
        "BHX", "NCL", "BFS", "ABZ", "INV", "EMA", "SOU",
        # Europe hubs
        "CDG", "ORY", "LYS", "NCE", "TLS", "BOD", "NTE", "MRS",
        "AMS", "BER", "MXP", "FCO", "NAP", "VCE", "PSA", "BRI",
        "BCN", "MAD", "AGP", "ALC", "PMI", "IBZ", "SVQ", "FUE",
        "ACE", "LPA", "TFS", "FAO", "LIS", "OPO", "GVA", "BSL",
        "CPH", "PRG", "BUD", "KRK", "WRO", "VIE",  # GDN dropped by easyJet (Mar 2026)
        "ATH", "HER", "CFU", "RHO", "JTR", "ZTH", "SKG",
        "SPU", "DBV", "ZAG", "LJU", "TLV", "IST", "SAW",
        "ESB", "ADB", "AYT", "DLM", "BJV", "HRG", "SSH",
        "MRK", "RAK",
    })

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """
        Search easyJet with city-code fan-out (parallel).

        If origin/dest is a city code (e.g. LON), expand to individual airports
        and search each one in parallel, merging results.  easyJet does NOT fly
        from LHR so city expansion is critical for London searches.
        """
        ob_offers, ob_currency = await self._search_fanout(req)

        if req.return_from and ob_offers:
            ib_req = req.model_copy(update={"origin": req.destination, "destination": req.origin, "date_from": req.return_from, "return_from": None})
            ib_offers, _ = await self._search_fanout(ib_req)
            if ib_offers:
                ob_offers = self._combine_rt(ob_offers, ib_offers, req)

        search_hash = hashlib.md5(
            f"easyjet{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]

        resp = FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=ob_currency,
            offers=ob_offers,
            total_results=len(ob_offers),
        )
        if resp.offers:
            segs = resp.offers[0].outbound.segments if resp.offers[0].outbound else []
            anc_origin = segs[0].origin if segs else req.origin
            anc_dest = segs[-1].destination if segs else req.destination
            try:
                ancillary = await asyncio.wait_for(
                    self._fetch_ancillaries(anc_origin, anc_dest, req.date_from.isoformat(), req.adults, resp.currency),
                    timeout=45.0,
                )
                if ancillary:
                    self._apply_ancillaries(resp.offers, ancillary)
            except (asyncio.TimeoutError, TimeoutError):
                logger.debug("Ancillary fetch timed out for %s\u2192%s", anc_origin, anc_dest)
            except Exception as _anc_err:
                logger.debug("Ancillary fetch error for %s\u2192%s: %s", anc_origin, anc_dest, _anc_err)
        return resp

    async def _fetch_ancillaries(
        self, origin: str, dest: str, date_str: str, adults: int, currency: str
    ) -> dict | None:
        """EasyJet STANDARD fare always includes 1 cabin bag — enrich note only."""
        return {
            "bags_from": 0.0,
            "bags_note": "1 cabin bag (56\u00d745\u00d725 cm, up to 15 kg) included in base fare",
            "checked_bag_from": 18.0,
            "checked_bag_note": "First checked bag (23 kg) from ~EUR 18",
            "seat_from": 5.0,
            "seat_note": "Seat selection add-on from ~EUR 5",
            "currency": "EUR",
        }

    def _apply_ancillaries(self, offers: list, ancillary: dict) -> None:
        bags_note = ancillary.get("bags_note")
        seat_note = ancillary.get("seat_note")
        bags_from = ancillary.get("bags_from")
        checked_bag_note = ancillary.get("checked_bag_note")
        checked_bag_from = ancillary.get("checked_bag_from")
        anc_currency = ancillary.get("currency", "EUR")
        for offer in offers:
            if bags_note:
                offer.conditions["cabin_bag"] = bags_note
            if seat_note:
                offer.conditions["seat"] = seat_note
            if checked_bag_note:
                offer.conditions["checked_bag"] = checked_bag_note
            if bags_from is not None:
                offer.bags_price["cabin_bag"] = bags_from
            if checked_bag_from is not None and offer.currency.upper() == anc_currency.upper():
                offer.bags_price["checked_bag"] = checked_bag_from

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

        _sem = asyncio.Semaphore(3)

        async def _search_pair(orig: str, dest: str) -> FlightSearchResponse | None:
            async with _sem:
                sub_req = req.model_copy(update={"origin": orig, "destination": dest})
                return await self._search_single(sub_req)

        results = await asyncio.gather(
            *[_search_pair(o, d) for o, d in pairs],
            return_exceptions=True,
        )

        for resp in results:
            if isinstance(resp, BaseException) or not resp or not resp.offers:
                continue
            currency = resp.currency or currency
            for o in resp.offers:
                segs = o.outbound.segments if o.outbound else []
                route = "-".join(s.origin for s in segs) if segs else ""
                h = f"{o.price}-{route}"
                if h not in seen_hashes:
                    seen_hashes.add(h)
                    all_offers.append(o)

        all_offers.sort(key=lambda o: o.price)
        return all_offers, currency

    async def _search_single(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """
        Search easyJet for a single origin→dest pair using CDP Chrome.
        """
        t0 = time.monotonic()

        # Reset Chrome profile for fresh session (Akamai tracks sessions)
        await _reset_chrome_profile()
        await asyncio.sleep(0.5)

        context = await _get_context()
        page = await context.new_page()
        await inject_stealth_js(page)
        await auto_block_if_proxied(page)

        # Set up response interception BEFORE navigating
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
        results_page = None

        try:
            logger.info("easyJet: loading homepage for %s→%s", req.origin, req.destination)
            await page.goto(
                "https://www.easyjet.com/en/",
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            # More human-like wait
            await asyncio.sleep(random.uniform(2.5, 4.0))

            try:
                await page.wait_for_load_state("networkidle", timeout=10000)
            except Exception:
                pass

            # Dismiss cookie/consent banners
            await _dismiss_cookies(page)
            await asyncio.sleep(random.uniform(0.5, 1.0))
            await _dismiss_cookies(page)

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

            # Fill the search form with human-like delays
            ok = await self._fill_search_form(page, req)
            if not ok:
                logger.warning("easyJet: form fill failed, aborting")
                return self._empty(req)

            # Listen for new tab BEFORE clicking (easyJet opens results in new tab)
            new_page_event = asyncio.Event()
            new_page_ref: list = [None]

            def _on_new_page(p):
                new_page_ref[0] = p
                new_page_event.set()

            context.on("page", _on_new_page)

            # Human-like delay before clicking search
            await asyncio.sleep(random.uniform(0.5, 1.5))

            # Click "Show flights"
            try:
                await page.get_by_role("button", name="Show flights").click(timeout=5000)
                logger.info("easyJet: clicked 'Show flights', waiting for results")
            except Exception as e:
                logger.warning("easyJet: could not click 'Show flights': %s", e)
                return self._empty(req)

            # Wait for new tab (easyJet opens results in a new tab)
            try:
                await asyncio.wait_for(new_page_event.wait(), timeout=10.0)
                results_page = new_page_ref[0]
                if results_page:
                    results_page.on("response", _on_response)
                    await auto_block_if_proxied(results_page)
                    logger.info("easyJet: results tab opened: %s", results_page.url)
                    try:
                        await results_page.wait_for_load_state("domcontentloaded", timeout=15000)
                    except Exception:
                        pass
            except (asyncio.TimeoutError, Exception):
                # No new tab — page may have navigated in-place (old behavior)
                logger.info("easyJet: no new tab, checking current page: %s", page.url)
                try:
                    await page.wait_for_url("**/buy/flights**", timeout=5000)
                except Exception:
                    pass

            # Wait for the intercepted API response (up to remaining timeout)
            remaining = max(self.timeout - (time.monotonic() - t0), 10)
            deadline = time.monotonic() + remaining
            while not search_data and not akamai_blocked and time.monotonic() < deadline:
                await asyncio.sleep(0.5)

            # If Akamai blocked us, nuke the profile and bail
            if akamai_blocked:
                logger.warning("easyJet: Akamai flagged session, trying Patchright fallback")
                patchright_resp = await self._search_single_with_patchright(req)
                if patchright_resp and patchright_resp.offers:
                    return patchright_resp

                logger.warning("easyJet: Patchright fallback empty, trying DOM scrape fallback")
                active_page = results_page or page
                dom_offers = await self._scrape_dom_fallback(active_page, req)
                if dom_offers:
                    search_hash = hashlib.md5(
                        f"easyjet{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
                    ).hexdigest()[:12]
                    return FlightSearchResponse(
                        search_id=f"fs_{search_hash}",
                        origin=req.origin,
                        destination=req.destination,
                        currency="GBP",
                        offers=dom_offers,
                        total_results=len(dom_offers),
                    )

                logger.warning("easyJet: DOM fallback empty, clearing Chrome profile for next run")
                await _reset_chrome_profile()
                return self._empty(req)

            if not search_data or not search_data.get("journeyPairs"):
                logger.warning("easyJet: no journeyPairs in intercepted response, trying Patchright fallback")
                patchright_resp = await self._search_single_with_patchright(req)
                if patchright_resp and patchright_resp.offers:
                    return patchright_resp
                return self._empty(req)

            currency = search_data.get("metaData", {}).get("currencyCode", "GBP")
            offers = self._parse_journey_pairs(search_data["journeyPairs"], req, currency)

            elapsed = time.monotonic() - t0
            offers.sort(key=lambda o: o.price)

            logger.info(
                "easyJet %s→%s returned %d offers in %.1fs (CDP Chrome)",
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
            logger.error("easyJet CDP error: %s", e)
            return self._empty(req)
        finally:
            for p in [results_page, page]:
                try:
                    if p:
                        await p.close()
                except Exception:
                    pass

    async def _search_single_with_patchright(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """Retry an easyJet search with Patchright when CDP Chrome is blocked."""
        t0 = time.monotonic()
        pw = browser = context = page = results_page = None

        try:
            pw, browser, context, page = await _launch_patchright_browser()

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
                        logger.warning("easyJet: Patchright also hit Akamai 403 on /funnel/api/query")
                        return
                    if status == 200:
                        try:
                            data = await response.json()
                            if isinstance(data, dict) and "journeyPairs" in data:
                                search_data.update(data)
                                logger.info("easyJet: Patchright captured search API response")
                        except Exception as exc:
                            logger.warning("easyJet: Patchright failed to parse API response: %s", exc)

            page.on("response", _on_response)

            logger.info("easyJet: Patchright loading homepage for %s→%s", req.origin, req.destination)
            await page.goto(
                "https://www.easyjet.com/en/",
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            await asyncio.sleep(random.uniform(2.5, 4.0))

            try:
                await page.wait_for_load_state("networkidle", timeout=10000)
            except Exception:
                pass

            await _dismiss_cookies(page)
            await asyncio.sleep(random.uniform(0.5, 1.0))
            await _dismiss_cookies(page)

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
                logger.warning("easyJet: Patchright form fill failed")
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
                logger.info("easyJet: Patchright clicked 'Show flights'")
            except Exception as exc:
                logger.warning("easyJet: Patchright could not click 'Show flights': %s", exc)
                return self._empty(req)

            try:
                await asyncio.wait_for(new_page_event.wait(), timeout=10.0)
                results_page = new_page_ref[0]
                if results_page:
                    results_page.on("response", _on_response)
                    await auto_block_if_proxied(results_page)
                    logger.info("easyJet: Patchright results tab opened: %s", results_page.url)
                    try:
                        await results_page.wait_for_load_state("domcontentloaded", timeout=15000)
                    except Exception:
                        pass
            except (asyncio.TimeoutError, Exception):
                logger.info("easyJet: Patchright saw no new tab, checking current page: %s", page.url)
                try:
                    await page.wait_for_url("**/buy/flights**", timeout=5000)
                except Exception:
                    pass

            remaining = max(self.timeout - (time.monotonic() - t0), 10)
            deadline = time.monotonic() + remaining
            while not search_data and not akamai_blocked and time.monotonic() < deadline:
                await asyncio.sleep(0.5)

            active_page = results_page or page
            if akamai_blocked:
                logger.warning("easyJet: Patchright session still blocked, trying DOM scrape fallback")
                dom_offers = await self._scrape_dom_fallback(active_page, req)
                if dom_offers:
                    search_hash = hashlib.md5(
                        f"easyjet{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
                    ).hexdigest()[:12]
                    return FlightSearchResponse(
                        search_id=f"fs_{search_hash}",
                        origin=req.origin,
                        destination=req.destination,
                        currency="GBP",
                        offers=dom_offers,
                        total_results=len(dom_offers),
                    )
                return self._empty(req)

            if not search_data or not search_data.get("journeyPairs"):
                logger.warning("easyJet: Patchright captured no journeyPairs, trying DOM scrape fallback")
                dom_offers = await self._scrape_dom_fallback(active_page, req)
                if dom_offers:
                    search_hash = hashlib.md5(
                        f"easyjet{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
                    ).hexdigest()[:12]
                    return FlightSearchResponse(
                        search_id=f"fs_{search_hash}",
                        origin=req.origin,
                        destination=req.destination,
                        currency="GBP",
                        offers=dom_offers,
                        total_results=len(dom_offers),
                    )
                return self._empty(req)

            currency = search_data.get("metaData", {}).get("currencyCode", "GBP")
            offers = self._parse_journey_pairs(search_data["journeyPairs"], req, currency)
            offers.sort(key=lambda o: o.price)

            elapsed = time.monotonic() - t0
            logger.info(
                "easyJet %s→%s returned %d offers in %.1fs (Patchright fallback)",
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
        except ImportError:
            logger.warning("easyJet: Patchright is not installed; fallback unavailable")
            return self._empty(req)
        except Exception as exc:
            logger.error("easyJet Patchright fallback error: %s", exc)
            return self._empty(req)
        finally:
            for current_page in [results_page, page]:
                try:
                    if current_page:
                        await current_page.close()
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

    # ------------------------------------------------------------------
    # Form interaction
    # ------------------------------------------------------------------

    async def _fill_search_form(self, page, req: FlightSearchRequest) -> bool:
        """Fill the easyJet homepage search form with human-like timing."""
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

    # IATA → human-readable name for autocomplete fallback
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
        """Fill an airport textbox and select the matching suggestion."""
        # Try IATA code first, then fall back to city name
        result = await self._try_fill_airport(page, label, iata)
        if result:
            return True

        # Fallback: try full airport name (easyJet's autocomplete may prefer names)
        name = self._IATA_NAMES.get(iata)
        if name:
            logger.info("easyJet: retrying %s with name '%s' instead of IATA", label, name)
            result = await self._try_fill_airport(page, label, name, match_hint=iata)
            if result:
                return True

        logger.warning("easyJet: %s field — no matching suggestion found for %s", label, iata)
        return False

    async def _try_fill_airport(self, page, label: str, query: str, match_hint: str = "") -> bool:
        """Type a query and try to click the matching suggestion. Returns True on success."""
        hint = match_hint or query
        try:
            # Remove any overlays that might intercept clicks
            await page.evaluate("""() => {
                document.querySelectorAll(
                    '.modal-lightbox-wrapper, .account-modal, .modal__dialog-wrapper, ' +
                    '[class*="overlay"][style*="z-index"]'
                ).forEach(el => el.remove());
            }""")

            field = page.get_by_role("textbox", name=label)
            if label == "From":
                clear_name = "Clear selected departure airport"
            else:
                clear_name = "Clear selected destination airport"
            try:
                clear_btn = page.get_by_role("button", name=clear_name)
                if await clear_btn.count() > 0:
                    await clear_btn.click(timeout=2000)
                    await asyncio.sleep(random.uniform(0.3, 0.6))
            except Exception:
                pass

            await field.click(timeout=3000)
            await asyncio.sleep(random.uniform(0.3, 0.6))
            # Type character by character with random delay for human-like behavior
            await field.type(query, delay=random.randint(80, 150))
            logger.info("easyJet: typed '%s' in %s field", query, label)
            await asyncio.sleep(random.uniform(3.0, 4.5))  # extra time for autocomplete via proxy

            # Strategy 1: Playwright role-based selectors
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

            # Strategy 2: CSS locator selectors
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

            # Strategy 3: JS-based — find any visible element containing the
            # IATA code in its text and click it (handles custom components)
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
                        // Only check direct text content (not children)
                        const directText = Array.from(el.childNodes)
                            .filter(n => n.nodeType === 3).map(n => n.textContent).join('');
                        const fullText = el.textContent || '';
                        if (regex.test(fullText) && (el.tagName === 'LI' || el.tagName === 'BUTTON'
                            || el.tagName === 'A' || el.getAttribute('role') === 'option'
                            || el.getAttribute('role') === 'listitem'
                            || el.classList.toString().match(/airport|result|suggest|item/i))) {
                            candidates.push(el);
                        }
                    }
                    // Pick the smallest (most specific) element
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

            # Strategy 4: Just click the first visible dropdown item (any match)
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
        """Open the date picker and select the outbound date."""
        target = req.date_from
        target_month_name = target.strftime("%B").upper()  # e.g., "MAY"
        target_year = target.year
        target_month_year = f"{target_month_name} {target_year}"  # e.g., "MAY 2026"

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
                await page.wait_for_selector(
                    '[data-testid="month-title"]', timeout=10000
                )
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
                """Parse 'JUNE 2026' to (year, month_idx)."""
                parts = text.strip().split()
                if len(parts) >= 2:
                    return int(parts[1]), month_order.get(parts[0], 0)
                return (9999, 99)

            target_key = (target_year, target.month)

            # Navigate until target month is visible
            for attempt in range(24):  # Allow more attempts for backward navigation
                # Check if day button is already visible
                if await day_btn.count() > 0 or await day_btn_fallback.count() > 0:
                    break

                # Check which months are currently visible
                try:
                    visible_months = await page.evaluate("""() => {
                        const titles = document.querySelectorAll('[data-testid="month-title"]');
                        return Array.from(titles).map(t => t.textContent.trim().toUpperCase());
                    }""")

                    # If target month is already visible, wait a moment for buttons to render
                    if any(target_month_year in m for m in visible_months):
                        await asyncio.sleep(0.5)
                        if await day_btn.count() > 0 or await day_btn_fallback.count() > 0:
                            break
                        # Button still not found despite month being visible - try JS click
                        logger.info("easyJet: month visible but button not found, trying JS")
                        break

                    # Determine navigation direction
                    if visible_months:
                        first_visible = visible_months[0]
                        first_key = parse_month_year(first_visible)

                        if first_key > target_key:
                            # We're past the target - navigate backward
                            try:
                                prev_btn = page.get_by_role("button", name="Previous month")
                                if await prev_btn.count() > 0:
                                    await prev_btn.click(timeout=2000)
                                    await asyncio.sleep(0.5)
                                    continue
                            except Exception:
                                pass
                            logger.warning("easyJet: cannot navigate backward to %s from %s",
                                           target_month_year, first_visible)
                            break
                        else:
                            # Navigate forward
                            try:
                                await page.get_by_role("button", name="Next month").click(timeout=2000)
                                await asyncio.sleep(0.5)
                            except Exception:
                                break
                    else:
                        # No visible months - try forward
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
                logger.info("easyJet: clicked date %s (aria-label fallback)", target)
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
            # Dismiss date picker dropdown so it doesn't block "Show flights"
            await page.keyboard.press("Escape")
            await asyncio.sleep(0.3)
            return True
        except Exception as e:
            logger.warning("easyJet: date error: %s", e)
            return False

    async def _scrape_dom_fallback(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Extract visible flight cards when the API is blocked but the results page still renders fares."""
        try:
            await asyncio.sleep(5.0)
            flights = await page.evaluate(r'''() => {
                const results = [];
                const cards = document.querySelectorAll(
                    '[class*="flight-card"], [class*="journey"], [class*="FlightCard"], ' +
                    '[data-testid*="flight"], [class*="flight-row"], [class*="flightInfo"]'
                );
                for (const card of cards) {
                    const text = (card.innerText || '').replace(/\s+/g, ' ').trim();
                    if (text.length < 20) continue;
                    const times = text.match(/\b(\d{1,2}:\d{2})\b/g) || [];
                    if (times.length < 2) continue;
                    const priceMatch = text.match(/[£€$]\s*([\d,.]+)/);
                    if (!priceMatch) continue;
                    const price = Number(priceMatch[1].replace(/,/g, ''));
                    if (!Number.isFinite(price) || price <= 0) continue;
                    results.push({
                        dep_time: times[0],
                        arr_time: times[1],
                        price,
                        text: text.slice(0, 200),
                    });
                }
                return results;
            }''')

            if not flights:
                logger.info("easyJet: DOM scrape found no flight cards")
                return []

            logger.info("easyJet: DOM scrape found %d flight cards", len(flights))
            offers: list[FlightOffer] = []
            date_value = req.date_from if hasattr(req.date_from, "strftime") else datetime.strptime(str(req.date_from), "%Y-%m-%d")
            date_str = date_value.strftime("%Y-%m-%d")
            booking_url = self._build_booking_url(req)

            for flight in flights:
                dep_time = str(flight.get("dep_time") or "00:00")
                arr_time = str(flight.get("arr_time") or "00:00")
                price = float(flight.get("price") or 0)
                if price <= 0:
                    continue

                departure = datetime.strptime(f"{date_str}T{dep_time}:00", "%Y-%m-%dT%H:%M:%S")
                arrival = datetime.strptime(f"{date_str}T{arr_time}:00", "%Y-%m-%dT%H:%M:%S")
                if arrival <= departure:
                    arrival += timedelta(days=1)

                segment = FlightSegment(
                    airline="U2",
                    airline_name="easyJet",
                    flight_no="",
                    origin=req.origin,
                    destination=req.destination,
                    departure=departure,
                    arrival=arrival,
                    cabin_class={"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy"),
                )
                duration_seconds = int((arrival - departure).total_seconds())
                route = FlightRoute(
                    segments=[segment],
                    total_duration_seconds=max(duration_seconds, 0),
                    stopovers=0,
                )
                offer_key = f"ej-dom-{req.origin}{req.destination}{date_str}{dep_time}{price}"
                offers.append(
                    FlightOffer(
                        id=f"ej_{hashlib.md5(offer_key.encode()).hexdigest()[:12]}",
                        price=round(price, 2),
                        currency="GBP",
                        price_formatted=f"{price:.2f} GBP",
                        outbound=route,
                        inbound=None,
                        airlines=["easyJet"],
                        owner_airline="U2",
                        booking_url=booking_url,
                        is_locked=False,
                        source="easyjet_direct",
                        source_tier="free",
                    )
                )

            return offers
        except Exception as e:
            logger.warning("easyJet: DOM scrape failed: %s", e)
            return []

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

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
                available = list(flights_by_date.keys())
                logger.warning(
                    "easyJet: target %s not in flight dates %s, using all",
                    target_date, available,
                )
                matched_dates = available

            for date_key in matched_dates:
                for flight in flights_by_date[date_key]:
                    offer = self._parse_single_flight(
                        flight,
                        currency,
                        booking_url,
                        req.cabin_class or "M",
                    )
                    if offer:
                        offers.append(offer)

        return offers

    def _parse_single_flight(
        self, flight: dict, currency: str, booking_url: str, cabin_code: str = "M"
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
            cabin_class={"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(cabin_code or "M", "economy"),
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

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

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


# ── Bookable connector (checkout automation) ─────────────────────────────

class EasyjetBookableConnector:
    """
    Drive EasyJet checkout up to (not including) payment submission.

    Flow: Navigate to booking URL → Select flights → Select fare →
          Fill passengers → Skip extras → STOP at payment page.

    Uses Playwright with Akamai-aware headed Chrome. Never submits payment.
    """

    AIRLINE_NAME = "easyJet"
    SOURCE_TAG = "easyjet_direct"

    async def start_checkout(
        self,
        offer: dict,
        passengers: list[dict],
        checkout_token: str,
        api_key: str,
        *,
        base_url: str | None = None,
    ):
        from .booking_base import (
            CheckoutProgress,
            dismiss_overlays,
            safe_click,
            safe_fill,
            take_screenshot_b64,
            verify_checkout_token,
        )
        import random
        import time

        t0 = time.monotonic()
        booking_url = offer.get("booking_url", "")
        offer_id = offer.get("id", "")

        # Verify checkout token with backend
        try:
            verification = verify_checkout_token(offer_id, checkout_token, api_key, base_url)
            if not verification.get("valid"):
                return CheckoutProgress(
                    status="failed", airline=self.AIRLINE_NAME, source=self.SOURCE_TAG,
                    offer_id=offer_id, booking_url=booking_url,
                    message="Checkout token invalid or expired. Call unlock() first.",
                )
        except Exception as e:
            return CheckoutProgress(
                status="failed", airline=self.AIRLINE_NAME, source=self.SOURCE_TAG,
                offer_id=offer_id, booking_url=booking_url,
                message=f"Token verification failed: {e}",
            )

        if not booking_url:
            return CheckoutProgress(
                status="failed", airline=self.AIRLINE_NAME, source=self.SOURCE_TAG,
                offer_id=offer_id, message="No booking URL available for this offer.",
            )

        from playwright.async_api import async_playwright

        pw = await async_playwright().start()
        browser = await pw.chromium.launch(
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--window-position=-2400,-2400",
                "--window-size=1440,900",
            ],
        )
        _browser_pid = None
        try:
            _browser_pid = browser._impl_obj._browser_process.pid
        except Exception:
            pass
        context = await browser.new_context(
            viewport={"width": random.choice([1366, 1440, 1920]),
                       "height": random.choice([768, 900, 1080])},
            locale="en-GB",
            timezone_id="Europe/London",
        )

        try:
            try:
                from playwright_stealth import stealth_async
                page = await context.new_page()
                await auto_block_if_proxied(page)
                await stealth_async(page)
            except ImportError:
                page = await context.new_page()
                await auto_block_if_proxied(page)

            step = "started"
            pax = passengers[0] if passengers else {}

            # Step 1: Navigate to EasyJet booking page
            logger.info("EasyJet checkout: navigating to %s", booking_url)
            await page.goto(booking_url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(3000)

            # Dismiss EasyJet-specific overlays
            await dismiss_overlays(page)
            for sel in [
                "#ensCloseBanner",
                "button:has-text('Accept all cookies')",
                "[class*='cookie-banner'] button",
            ]:
                await safe_click(page, sel, timeout=2000, desc="easyjet cookies")

            step = "page_loaded"

            # Step 2: Select flights (EasyJet shows flight cards)
            try:
                await page.wait_for_selector(
                    "[class*='flight-grid'], [class*='flight-card'], [data-testid*='flight']",
                    timeout=20000,
                )
            except Exception:
                pass

            await dismiss_overlays(page)

            # Try to match by departure time
            outbound = offer.get("outbound", {})
            segments = outbound.get("segments", []) if isinstance(outbound, dict) else []
            if segments:
                dep = segments[0].get("departure", "")
                dep_time = _extract_hhmm(dep)
                if dep_time:
                    try:
                        card = page.locator(f"text='{dep_time}'").first
                        if await card.is_visible(timeout=3000):
                            await card.click()
                    except Exception:
                        pass

            # Fallback: click first available flight
            for sel in [
                "[class*='flight-card']:first-child",
                "[data-testid*='flight']:first-child",
                "button:has-text('Select'):first-child",
            ]:
                await safe_click(page, sel, timeout=3000, desc="first flight")

            await page.wait_for_timeout(2000)
            step = "flights_selected"

            # Step 3: Select fare (Standard/Flexi)
            for sel in [
                "button:has-text('Standard')",
                "button:has-text('Continue')",
                "[class*='fare'] button:first-child",
                "button:has-text('Select')",
            ]:
                if await safe_click(page, sel, timeout=5000, desc="select fare"):
                    break

            await page.wait_for_timeout(1500)
            await dismiss_overlays(page)
            step = "fare_selected"

            # Step 4: Skip login if prompted
            for sel in [
                "button:has-text('Continue as guest')",
                "button:has-text('Skip')",
                "button:has-text('No thanks')",
                "[data-testid*='guest'] button",
            ]:
                if await safe_click(page, sel, timeout=4000, desc="skip login"):
                    break
            await page.wait_for_timeout(1500)
            await dismiss_overlays(page)
            step = "login_bypassed"

            # Step 5: Fill passenger details
            try:
                await page.wait_for_selector(
                    "input[name*='name'], [class*='passenger-form'], [data-testid*='passenger']",
                    timeout=15000,
                )
            except Exception:
                pass

            # Title
            title = "Mr" if pax.get("gender", "m") == "m" else "Ms"
            for sel in [
                f"select option:has-text('{title}')",
                f"[data-testid*='title'] option:has-text('{title}')",
            ]:
                try:
                    await page.select_option("select[name*='title'], [data-testid*='title'] select",
                                             label=title, timeout=3000)
                    break
                except Exception:
                    pass
            # Fallback: click dropdown
            await safe_click(page, f"button:has-text('{title}')", timeout=2000, desc=f"title {title}")

            # First name
            for sel in [
                "input[name*='firstName']",
                "input[data-testid*='first-name']",
                "input[placeholder*='First name' i]",
            ]:
                if await safe_fill(page, sel, pax.get("given_name", "Test")):
                    break

            # Last name
            for sel in [
                "input[name*='lastName']",
                "input[data-testid*='last-name']",
                "input[placeholder*='Last name' i]",
            ]:
                if await safe_fill(page, sel, pax.get("family_name", "Traveler")):
                    break

            # Email
            for sel in [
                "input[name*='email']",
                "input[data-testid*='email']",
                "input[type='email']",
            ]:
                if await safe_fill(page, sel, pax.get("email", "test@example.com")):
                    break

            # Phone
            for sel in [
                "input[name*='phone']",
                "input[data-testid*='phone']",
                "input[type='tel']",
            ]:
                if await safe_fill(page, sel, pax.get("phone_number", "+441234567890")):
                    break

            step = "passengers_filled"

            # Continue
            for sel in [
                "button:has-text('Continue')",
                "button:has-text('Next')",
                "[data-testid*='continue'] button",
            ]:
                if await safe_click(page, sel, timeout=5000, desc="continue"):
                    break
            await page.wait_for_timeout(2000)
            await dismiss_overlays(page)

            # Step 6: Skip extras (bags, insurance, seats)
            for _ in range(5):
                await dismiss_overlays(page)
                for sel in [
                    "button:has-text('No, thanks')",
                    "button:has-text('Continue')",
                    "button:has-text('Skip')",
                    "button:has-text('No hold luggage')",
                    "button:has-text('Continue without')",
                    "button:has-text('Next')",
                ]:
                    await safe_click(page, sel, timeout=2000, desc="skip extras")
                await page.wait_for_timeout(1500)

            step = "extras_skipped"

            # Skip seats
            for sel in [
                "button:has-text('Skip')",
                "button:has-text('No thanks')",
                "button:has-text('Continue without')",
                "button:has-text('Assign random seats')",
            ]:
                if await safe_click(page, sel, timeout=4000, desc="skip seats"):
                    break
            await page.wait_for_timeout(1500)
            for sel in ["button:has-text('OK')", "button:has-text('Continue')"]:
                await safe_click(page, sel, timeout=3000, desc="confirm skip")

            step = "seats_skipped"
            await page.wait_for_timeout(2000)
            await dismiss_overlays(page)

            # Step 7: Payment page reached — STOP HERE
            step = "payment_page_reached"
            screenshot = await take_screenshot_b64(page)

            page_price = offer.get("price", 0.0)
            try:
                for sel in [
                    "[class*='total-price']",
                    "[data-testid*='total']",
                    "[class*='summary'] [class*='price']",
                ]:
                    el = page.locator(sel).first
                    if await el.is_visible(timeout=2000):
                        text = await el.text_content()
                        if text:
                            import re
                            nums = re.findall(r"[\d,.]+", text)
                            if nums:
                                page_price = float(nums[-1].replace(",", ""))
                        break
            except Exception:
                pass

            elapsed = time.monotonic() - t0
            return CheckoutProgress(
                status="payment_page_reached",
                step=step,
                step_index=8,
                airline=self.AIRLINE_NAME,
                source=self.SOURCE_TAG,
                offer_id=offer_id,
                total_price=page_price,
                currency=offer.get("currency", "EUR"),
                booking_url=booking_url,
                screenshot_b64=screenshot,
                message=(
                    f"easyJet checkout complete — reached payment page in {elapsed:.0f}s. "
                    f"Price: {page_price} {offer.get('currency', 'EUR')}. "
                    f"Payment NOT submitted (safe mode). "
                    f"Complete manually at: {booking_url}"
                ),
                can_complete_manually=True,
                elapsed_seconds=elapsed,
            )

        except Exception as e:
            logger.error("EasyJet checkout error: %s", e, exc_info=True)
            screenshot = ""
            try:
                screenshot = await take_screenshot_b64(page)
            except Exception:
                pass
            return CheckoutProgress(
                status="error",
                step=step,
                airline=self.AIRLINE_NAME,
                source=self.SOURCE_TAG,
                offer_id=offer_id,
                booking_url=booking_url,
                screenshot_b64=screenshot,
                message=f"Checkout error at step '{step}': {e}",
                elapsed_seconds=time.monotonic() - t0,
            )
        finally:
            try:
                await context.close()
            except Exception:
                pass
            try:
                await browser.close()
            except Exception:
                pass
            try:
                await pw.stop()
            except Exception:
                pass
            if _browser_pid:
                try:
                    import subprocess
                    subprocess.run(
                        ["taskkill", "/F", "/T", "/PID", str(_browser_pid)],
                        capture_output=True, timeout=5,
                    )
                except Exception:
                    pass
