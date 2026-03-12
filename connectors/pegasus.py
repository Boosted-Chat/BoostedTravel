"""
Pegasus Airlines CDP Chrome scraper — navigates to flypgs.com and searches flights.

Pegasus (IATA: PC) is Turkey's largest low-cost carrier, operating from
Istanbul Sabiha Gökçen (SAW) and Ankara (ESB) to domestic and international
destinations across Europe, Middle East and North Africa.

The direct API is behind Akamai Bot Manager — requires browser session.
Real Chrome via CDP passes Akamai better than Playwright's bundled Chromium.

Strategy (converted Mar 2026):
1. Launch real system Chrome via CDP (persistent, avoids fingerprinting)
2. Navigate to flypgs.com/en or direct booking URL
3. Dismiss cookie consent banner
4. Fill search form or use direct URL (bypasses form fill)
5. Intercept API responses (availability / search endpoints)
6. Parse results → FlightOffers
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import random
import re
import subprocess
import time
from datetime import datetime
from typing import Any, Optional

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

# ── Anti-fingerprint pools ─────────────────────────────────────────────────
_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_LOCALES = ["en-GB", "en-US", "en-IE", "en-AU"]
_TIMEZONES = [
    "Europe/Istanbul", "Europe/London", "Europe/Berlin",
    "Europe/Paris", "Europe/Rome", "Europe/Madrid",
]

_TURKEY_AIRPORTS = {
    "IST", "SAW", "AYT", "ESB", "ADB", "DLM", "BJV", "TZX", "GZT",
    "VAN", "ERZ", "DIY", "SZF", "KYA", "MLX", "ASR", "EZS", "MQM",
    "HTY", "NAV", "KCM", "EDO", "ONQ", "BAL", "CKZ", "MSR", "IGD",
    "NKT", "GNY", "USQ", "DNZ", "ERC", "AOE", "KSY", "ISE", "YEI",
    "TEQ", "OGU", "BZI", "SFQ",
}

# ── CDP Chrome singleton ──────────────────────────────────────────────
_CDP_PORT = 9454
_USER_DATA_DIR = os.path.join(os.environ.get("TEMP", "/tmp"), "pegasus_cdp_data")
_CHROME_PATHS = [
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    "/usr/bin/google-chrome",
    "/usr/bin/google-chrome-stable",
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
]

_chrome_proc: subprocess.Popen | None = None
_pw_instance = None
_cdp_browser = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


def _find_chrome() -> str:
    for p in _CHROME_PATHS:
        if os.path.isfile(p):
            return p
    raise FileNotFoundError("Chrome not found")


def _launch_chrome():
    global _chrome_proc
    if _chrome_proc and _chrome_proc.poll() is None:
        return
    os.makedirs(_USER_DATA_DIR, exist_ok=True)
    chrome = _find_chrome()
    _chrome_proc = subprocess.Popen(
        [
            chrome,
            f"--remote-debugging-port={_CDP_PORT}",
            f"--user-data-dir={_USER_DATA_DIR}",
            "--disable-blink-features=AutomationControlled",
            "--no-first-run", "--no-default-browser-check",
            "--disable-background-timer-throttling",
            "--disable-backgrounding-occluded-windows",
            "--disable-renderer-backgrounding",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    logger.info("Pegasus: Chrome launched on CDP port %d (pid=%d)", _CDP_PORT, _chrome_proc.pid)


async def _get_browser():
    """Shared real Chrome via CDP (launched once, reused across searches)."""
    global _pw_instance, _cdp_browser
    lock = _get_lock()
    async with lock:
        if _cdp_browser and _cdp_browser.is_connected():
            return _cdp_browser
        _launch_chrome()
        await asyncio.sleep(2)
        from playwright.async_api import async_playwright
        if not _pw_instance:
            _pw_instance = await async_playwright().start()
        for attempt in range(5):
            try:
                _cdp_browser = await _pw_instance.chromium.connect_over_cdp(
                    f"http://127.0.0.1:{_CDP_PORT}"
                )
                logger.info("Pegasus: connected to Chrome via CDP")
                return _cdp_browser
            except Exception:
                if attempt < 4:
                    await asyncio.sleep(1)
        raise RuntimeError(f"Pegasus: cannot connect to Chrome CDP on port {_CDP_PORT}")


class PegasusConnectorClient:
    """Pegasus Airlines Playwright scraper — homepage form search + API interception."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        browser = await _get_browser()

        context = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
            locale=random.choice(_LOCALES),
            timezone_id=random.choice(_TIMEZONES),
            service_workers="block",
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        )

        try:
            page = await context.new_page()

            all_captured: list = []
            api_event = asyncio.Event()

            async def on_response(response):
                try:
                    url = response.url.lower()
                    ct = response.headers.get("content-type", "")
                    if response.status == 200 and "json" in ct and "flypgs" in url:
                        logger.debug("Pegasus response: %s (status=%d)", response.url[:150], response.status)
                    if response.status == 200 and (
                        "availability" in url
                        or "/api/search" in url
                        or "flight/search" in url
                        or "offers" in url
                        or "air-bounds" in url
                        or "fares" in url
                        or "pegasus/availability" in url
                        or "/search/results" in url
                        or "low-fare" in url
                        or ("/apint/" in url and ("search" in url or "availability" in url))
                        or ("web.flypgs.com" in url and ("search" in url or "availability" in url or "fare" in url))
                        or "flexible-search" in url
                        or ("/booking" in url and "flypgs" in url)
                    ):
                        ct = response.headers.get("content-type", "")
                        if "json" in ct:
                            data = await response.json()
                            if data and isinstance(data, (dict, list)):
                                logger.debug(
                                    "Pegasus API intercept: url=%s keys=%s",
                                    response.url[:120],
                                    list(data.keys())[:10] if isinstance(data, dict) else f"list[{len(data)}]",
                                )
                                if isinstance(data, dict) and "departureRouteList" in data:
                                    routes = data["departureRouteList"]
                                    logger.debug("Pegasus availability: %d routes", len(routes) if isinstance(routes, list) else -1)
                                    if isinstance(routes, list) and routes:
                                        import json as _json
                                        logger.debug("Pegasus availability sample route:\n%s", _json.dumps(routes[0], indent=2, default=str)[:2000])
                                all_captured.append(data)
                                api_event.set()
                except Exception:
                    pass

            page.on("response", on_response)

            logger.info("Pegasus: searching %s→%s on %s", req.origin, req.destination, req.date_from)

            # ── Warm-up: establish session cookies on web.flypgs.com ─────
            try:
                await page.goto("https://web.flypgs.com/", wait_until="domcontentloaded", timeout=15000)
                await asyncio.sleep(random.uniform(1.5, 3.0))
            except Exception:
                pass

            # ── STRATEGY 1: Direct booking URL (bypasses form fill) ──────
            dep = req.date_from.strftime("%Y-%m-%d")
            direct_url = (
                f"https://web.flypgs.com/booking?"
                f"language=en&adultCount={req.adults}&childCount={req.children or 0}"
                f"&infantCount={req.infants or 0}&departurePort={req.origin}"
                f"&arrivalPort={req.destination}&currency={req.currency or 'EUR'}"
                f"&dateOption=1&departureDate={dep}"
            )

            direct_success = False
            try:
                await page.goto(direct_url, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(random.uniform(2.0, 4.0))
                title = await page.title()
                logger.info("Pegasus: direct booking page title='%s'", title[:60])
                if "bulunamad" not in title.lower() and "not found" not in title.lower() and "error" not in title.lower():
                    direct_success = True
            except Exception as e:
                logger.debug("Pegasus: direct booking URL failed: %s", e)

            # ── STRATEGY 2: Homepage form fill (fallback) ────────────────
            if not direct_success:
                urls_to_try = [
                    "https://www.flypgs.com/",
                    "https://www.flypgs.com/en",
                    "https://www.flypgs.com/en/cheap-flight",
                ]

                form_loaded = False
                for attempt, url in enumerate(urls_to_try):
                    if attempt > 0:
                        logger.debug("Pegasus: trying alternate URL: %s", url)
                        await asyncio.sleep(random.uniform(2.0, 4.0))
                        try:
                            await page.context.clear_cookies()
                        except Exception:
                            pass

                    await page.goto(url, wait_until="domcontentloaded", timeout=int(self.timeout * 1000))
                    await asyncio.sleep(random.uniform(1.5, 3.0))

                    try:
                        await page.wait_for_selector("#fromWhere", timeout=15000)
                        logger.info("Pegasus: form loaded successfully from %s", url)
                        form_loaded = True
                        break
                    except Exception:
                        title = await page.title()
                        logger.debug("Pegasus: attempt %d (%s) — title='%s', no form", attempt + 1, url, title)
                        if "bulunamad" not in title.lower() and "not found" not in title.lower():
                            await asyncio.sleep(5.0)
                            if await page.locator("#fromWhere").count() > 0:
                                logger.info("Pegasus: form found after extra wait from %s", url)
                                form_loaded = True
                                break

                if not form_loaded:
                    logger.warning("Pegasus: could not load booking form after trying all URLs")
                    return self._empty(req)

                await self._dismiss_cookies(page)
                await asyncio.sleep(0.5)
                await self._dismiss_cookies(page)

                # Verify form survived cookie dismissal
                if await page.locator("#fromWhere").count() == 0:
                    logger.debug("Pegasus: form disappeared after cookie dismissal, waiting…")
                    try:
                        await page.wait_for_selector("#fromWhere", timeout=10000)
                    except Exception:
                        pass

                # Wait for airport data
                try:
                    await page.wait_for_selector(".SelectBox__airport__item, [data-port-code]", timeout=15000)
                    logger.debug("Pegasus: %d airport items loaded", await page.locator(".SelectBox__airport__item").count())
                except Exception:
                    logger.debug("Pegasus: airport items did not load in time, proceeding anyway")

                # Dismiss announcements popup
                try:
                    close_btn = page.locator(".c-announcements img[class*='cancel'], .c-announcements button")
                    if await close_btn.count() > 0:
                        await close_btn.first.click(timeout=2000)
                        await asyncio.sleep(0.3)
                except Exception:
                    pass

                ok = await self._fill_search_form(page, req)
                if not ok:
                    logger.warning("Pegasus: form fill failed")
                    return self._empty(req)

                await self._click_search(page)

            # ── Wait for API response (common to both strategies) ────────
            remaining = max(self.timeout - (time.monotonic() - t0), 10)
            deadline = time.monotonic() + remaining
            offers: list = []

            while time.monotonic() < deadline:
                try:
                    wait_time = max(deadline - time.monotonic(), 0.1)
                    await asyncio.wait_for(api_event.wait(), timeout=wait_time)
                except asyncio.TimeoutError:
                    break

                for cap_data in all_captured:
                    parsed = self._parse_response(cap_data, req)
                    if parsed:
                        offers = parsed
                        break

                if offers:
                    break

                api_event.clear()
                await asyncio.sleep(1.0)

            if not offers:
                logger.debug("Pegasus: %d API responses captured, none had flight offers", len(all_captured))
                offers = await self._extract_from_dom(page, req)

            if not offers:
                await asyncio.sleep(3.0)
                offers = await self._extract_from_dom(page, req)

            elapsed = time.monotonic() - t0
            if offers:
                return self._build_response(offers, req, elapsed)
            return self._empty(req)

        except Exception as e:
            logger.error("Pegasus Playwright error: %s", e)
            return self._empty(req)
        finally:
            await context.close()

    # ── Cookie / overlay dismissal ────────────────────────────────────

    async def _remove_overlays(self, page) -> None:
        """Disable overlays via CSS (no DOM mutations to avoid SPA re-renders)."""
        try:
            await page.evaluate("""() => {
                if (!document.querySelector('#pegasus-overlay-fix')) {
                    const style = document.createElement('style');
                    style.id = 'pegasus-overlay-fix';
                    style.textContent = `
                        .c-modal-overlay, [class*="modal-overlay"],
                        efilli-layout-dynamic,
                        .o-currency.js-currency-select-web,
                        .c-announcements,
                        [class*="popup"], [class*="Popup"],
                        [class*="banner"][style*="fixed"],
                        [class*="overlay"][style*="fixed"] {
                            display: none !important;
                            pointer-events: none !important;
                            opacity: 0 !important;
                            z-index: -1 !important;
                        }
                        .o-header {
                            pointer-events: none !important;
                        }
                        .o-header * {
                            pointer-events: auto !important;
                        }
                    `;
                    document.head.appendChild(style);
                }
                // Also nuke any fixed/absolute positioned full-screen elements blocking the form
                document.querySelectorAll('body > div').forEach(el => {
                    const s = getComputedStyle(el);
                    if ((s.position === 'fixed' || s.position === 'absolute') &&
                        parseInt(s.zIndex || '0') > 100 &&
                        el.offsetWidth > window.innerWidth * 0.8 &&
                        el.offsetHeight > window.innerHeight * 0.5 &&
                        !el.querySelector('#fromWhere')) {
                        el.style.display = 'none';
                        el.style.pointerEvents = 'none';
                    }
                });
                document.body.style.overflow = 'auto';
            }""")
        except Exception:
            pass

    async def _dismiss_cookies(self, page) -> None:
        # Inject CSS to disable overlay blocking (no DOM mutations)
        await self._remove_overlays(page)

        # Try clicking common accept buttons
        for label in [
            "Accept All", "Accept all", "ACCEPT ALL",
            "Accept", "I agree", "Accept all cookies",
            "Kabul Et", "Tümünü Kabul Et",
        ]:
            try:
                btn = page.get_by_role(
                    "button", name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE)
                )
                if await btn.count() > 0:
                    await btn.first.click(timeout=2000)
                    await asyncio.sleep(0.5)
                    return
            except Exception:
                continue

    # ── Form filling ───────────────────────────────────────────────────

    async def _fill_search_form(self, page, req: FlightSearchRequest) -> bool:
        ok = await self._fill_airport_field(page, "origin", req.origin)
        if not ok:
            logger.warning("Pegasus: origin fill failed")
            return False

        # Wait for destination SelectBox to become enabled (SPA loads valid routes)
        try:
            await page.wait_for_selector("#toWhere", timeout=10000)
        except Exception:
            # #toWhere might not exist — check for alternative destination inputs
            dest_info = await page.evaluate("""() => {
                const inputs = document.querySelectorAll('input[type="text"], input[name*="nereye"], input[name*="where"], input[placeholder*="arrival"], input[placeholder*="nereye"], input[placeholder*="destination"]');
                const result = [];
                inputs.forEach(el => {
                    result.push(`${el.tagName}#${el.id}.${el.className.slice(0,40)} name=${el.name} placeholder=${el.placeholder.slice(0,30)}`);
                });
                // Also check SelectBox elements
                const boxes = document.querySelectorAll('.SelectBox');
                result.push(`SelectBoxes: ${boxes.length}`);
                boxes.forEach((b, i) => {
                    const inp = b.querySelector('input');
                    result.push(`Box${i}: ${inp ? inp.id + '/' + inp.name : 'no-input'} cls=${b.className.slice(0,50)}`);
                });
                return result.join('\\n');
            }""")
            logger.debug("Pegasus: destination input diagnostics:\\n%s", dest_info)
        
        for _ in range(20):
            cls = await page.evaluate(
                "() => document.querySelector('#toWhere')?.closest('.SelectBox')?.className || ''"
            )
            if "disabled" not in cls.lower():
                break
            await asyncio.sleep(0.3)
        await asyncio.sleep(0.5)

        ok = await self._fill_airport_field(page, "destination", req.destination)
        if not ok:
            logger.warning("Pegasus: destination fill failed")
            return False
        await asyncio.sleep(0.8)

        # Set one-way AFTER airports to avoid form state issues
        await self._set_one_way(page)
        await asyncio.sleep(0.5)

        ok = await self._fill_date(page, req)
        if not ok:
            logger.warning("Pegasus: date fill failed")
            return False
        await asyncio.sleep(0.3)

        return True

    async def _fill_airport_field(self, page, field_type: str, iata: str) -> bool:
        """Fill airport using Pegasus-specific #fromWhere / #toWhere inputs."""
        input_id = "fromWhere" if field_type == "origin" else "toWhere"

        try:
            field = page.locator(f"#{input_id}")
            if await field.count() == 0:
                logger.debug("Pegasus: #%s not found", input_id)
                return False

            # Remove overlays before interacting
            await self._remove_overlays(page)
            await asyncio.sleep(0.5)

            # Click the input — try multiple strategies
            clicked = False
            for strategy in ["normal", "force", "js"]:
                if clicked:
                    break
                try:
                    if strategy == "normal":
                        await field.click(timeout=3000)
                    elif strategy == "force":
                        await field.click(timeout=3000, force=True)
                    else:
                        await page.evaluate(f"""() => {{
                            const el = document.querySelector('#{input_id}');
                            if (el) {{
                                el.dispatchEvent(new MouseEvent('mousedown', {{bubbles: true, cancelable: true}}));
                                el.dispatchEvent(new MouseEvent('mouseup', {{bubbles: true, cancelable: true}}));
                                el.dispatchEvent(new MouseEvent('click', {{bubbles: true, cancelable: true}}));
                                el.focus();
                            }}
                        }}""")
                    clicked = True
                except Exception:
                    pass
            
            await asyncio.sleep(1.0)

            # Wait for suggestion items to appear (SPA renders on input click)
            try:
                await page.wait_for_selector(".SelectBox__airport__item, [data-port-code]", timeout=5000)
            except Exception:
                logger.debug("Pegasus: suggestions did not appear after click for %s", field_type)

            # STRATEGY A: If the exact item exists in the DOM, click it directly (skip typing)
            # This works on the Turkish page where items are pre-loaded but typing filter breaks
            exact_item = page.locator(
                f".SelectBox__airport__item[data-port-code='{iata}']"
            )
            if await exact_item.count() > 0:
                # Make sure the dropdown is open (shown)
                active_item = page.locator(
                    f".SelectBox__list-wrapper--show .SelectBox__airport__item[data-port-code='{iata}']"
                )
                target = active_item if await active_item.count() > 0 else exact_item
                
                # First unhide the item if hidden, then click via JS
                selected = await page.evaluate(f"""() => {{
                    const active = document.querySelector('.SelectBox__list-wrapper--show .SelectBox__airport__item[data-port-code="{iata}"]')
                        || document.querySelector('.SelectBox__airport__item[data-port-code="{iata}"]');
                    if (active) {{
                        active.classList.remove('SelectBox__airport__item__hidden');
                        active.scrollIntoView({{block: 'center'}});
                        active.click();
                        return true;
                    }}
                    return false;
                }}""")
                if selected:
                    logger.info("Pegasus: selected %s via direct item click for %s", iata, field_type)
                    await asyncio.sleep(0.5)
                    return True

            # STRATEGY B: Type IATA code to filter suggestions, then click
            await page.evaluate(f"""() => {{
                const el = document.querySelector('#{input_id}');
                if (el) {{ el.focus(); el.value = ''; el.dispatchEvent(new Event('input', {{bubbles: true}})); }}
            }}""")
            await asyncio.sleep(0.3)
            
            typed = False
            try:
                await field.press_sequentially(iata, delay=120, timeout=5000)
                typed = True
            except Exception:
                pass
            
            if not typed:
                await page.evaluate(f"""() => {{
                    const el = document.querySelector('#{input_id}');
                    if (el) {{ el.focus(); el.value = ''; }}
                }}""")
                await asyncio.sleep(0.2)
                await page.keyboard.type(iata, delay=120)
                await page.evaluate(f"""() => {{
                    const el = document.querySelector('#{input_id}');
                    if (el) {{
                        el.dispatchEvent(new Event('input', {{bubbles: true}}));
                        el.dispatchEvent(new Event('change', {{bubbles: true}}));
                    }}
                }}""")
            
            await asyncio.sleep(1.5)

            # Try clicking filtered results
            active_exact = page.locator(
                f".SelectBox__list-wrapper--show .SelectBox__airport__item[data-port-code='{iata}']:not(.SelectBox__airport__item__hidden)"
            )
            global_exact = page.locator(
                f".SelectBox__airport__item[data-port-code='{iata}']:not(.SelectBox__airport__item__hidden)"
            )
            target = active_exact if await active_exact.count() > 0 else global_exact
            if await target.count() > 0:
                try:
                    await target.first.click(timeout=3000, force=True)
                    logger.info("Pegasus: selected %s via typed filter for %s", iata, field_type)
                    await asyncio.sleep(0.5)
                    return True
                except Exception:
                    pass

            # STRATEGY C: JS force-click regardless of hidden state
            selected = await page.evaluate(f"""() => {{
                const item = document.querySelector('.SelectBox__list-wrapper--show .SelectBox__airport__item[data-port-code="{iata}"]')
                    || document.querySelector('.SelectBox__airport__item[data-port-code="{iata}"]');
                if (item) {{
                    item.classList.remove('SelectBox__airport__item__hidden');
                    item.click();
                    return true;
                }}
                return false;
            }}""")
            if selected:
                logger.info("Pegasus: selected %s via JS force-click for %s", iata, field_type)
                await asyncio.sleep(0.5)
                return True

            logger.warning("Pegasus: could not find %s in suggestions for %s", iata, field_type)
            return False

        except Exception as e:
            logger.debug("Pegasus: %s field error: %s", field_type, e)
            return False

    async def _set_one_way(self, page) -> None:
        """Click the 'One way' button in the Pegasus booking form."""
        await self._remove_overlays(page)
        try:
            # Pegasus uses a specific button with class DirectionButtons__item
            btn = page.locator("button[data-round='false']")
            if await btn.count() > 0:
                try:
                    await btn.first.click(timeout=3000)
                except Exception:
                    await btn.first.click(timeout=3000, force=True)
                logger.info("Pegasus: set one-way via data-round button")
                return
        except Exception:
            pass

        # Fallback: text match
        try:
            btn = page.get_by_role("button", name=re.compile(r"one.?way", re.IGNORECASE))
            if await btn.count() > 0:
                await btn.first.click(timeout=3000)
                return
        except Exception:
            pass

        for label in ["One way", "One-way", "Tek Yön"]:
            try:
                ow = page.get_by_text(label, exact=False).first
                if await ow.count() > 0:
                    await ow.click(timeout=2000)
                    return
            except Exception:
                continue

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        """Fill date using Pegasus's flatpickr calendar."""
        target = req.date_from
        try:
            await self._remove_overlays(page)
            
            # Open the flatpickr calendar — try multiple strategies
            cal_opened = False
            
            # Strategy 1: click the hidden flatpickr input (JS click since it's display:none)
            dep_input = page.locator(".flatpickr-input.tstnm_fly_search_tab_1_departure_date_area")
            if await dep_input.count() > 0:
                await page.evaluate("""() => {
                    const el = document.querySelector('.flatpickr-input.tstnm_fly_search_tab_1_departure_date_area');
                    if (el) el.click();
                }""")
                await asyncio.sleep(0.5)
                if await page.locator(".flatpickr-calendar.open").count() > 0:
                    cal_opened = True

            # Strategy 2: click the visible date display
            if not cal_opened:
                date_display = page.locator(".DateInput.js-dep-date, [class*='DateInput__mobile-input']").first
                if await date_display.count() > 0:
                    try:
                        await date_display.click(timeout=3000, force=True)
                        await asyncio.sleep(0.5)
                        if await page.locator(".flatpickr-calendar.open").count() > 0:
                            cal_opened = True
                    except Exception:
                        pass

            # Strategy 3: text-based click
            if not cal_opened:
                for name in ["Departure date", "Departure"]:
                    try:
                        field = page.get_by_text(name).first
                        if await field.count() > 0:
                            await field.click(timeout=3000, force=True)
                            await asyncio.sleep(0.5)
                            if await page.locator(".flatpickr-calendar.open").count() > 0:
                                cal_opened = True
                                break
                    except Exception:
                        continue

            if not cal_opened:
                logger.debug("Pegasus: could not open date picker")

            await asyncio.sleep(0.5)

            # The Pegasus calendar shows ~6 months at once (183 days)
            # Try clicking the target day directly via aria-label
            # The label format depends on locale: English "April 15, 2026" or Turkish "Nisan 15, 2026"
            en_months = ["January", "February", "March", "April", "May", "June",
                         "July", "August", "September", "October", "November", "December"]
            tr_months = ["Ocak", "Şubat", "Mart", "Nisan", "Mayıs", "Haziran",
                         "Temmuz", "Ağustos", "Eylül", "Ekim", "Kasım", "Aralık"]
            month_idx = target.month - 1
            en_label = f"{en_months[month_idx]} {target.day}, {target.year}"
            tr_label = f"{tr_months[month_idx]} {target.day}, {target.year}"

            for day_label in [en_label, tr_label]:
                day_cell = page.locator(f".flatpickr-day[aria-label='{day_label}']")
                if await day_cell.count() > 0:
                    clicked = await page.evaluate(f"""() => {{
                        const day = document.querySelector('.flatpickr-day[aria-label="{day_label}"]');
                        if (day) {{ day.click(); return true; }}
                        return false;
                    }}""")
                    if clicked:
                        logger.info("Pegasus: selected date %s via JS click", day_label)
                        await asyncio.sleep(0.5)
                        return True

            # If not visible, navigate months forward
            target_month_str_en = en_months[month_idx]
            target_month_str_tr = tr_months[month_idx]
            target_year = target.year

            for _ in range(12):
                # Check if the target month is visible in the flatpickr calendar
                months_visible = page.locator(".flatpickr-current-month .cur-month")
                years_visible = page.locator(".flatpickr-current-month .numInput.cur-year")
                found = False

                for i in range(await months_visible.count()):
                    month_text = (await months_visible.nth(i).text_content() or "").strip()
                    year_val = await years_visible.nth(i).get_attribute("value") or ""
                    if (target_month_str_en.lower() in month_text.lower()
                            or target_month_str_tr.lower() in month_text.lower()) and str(target_year) in year_val:
                        found = True
                        break

                if found:
                    break

                # Click next month arrow via JS
                next_btn = page.locator(".flatpickr-next-month")
                if await next_btn.count() > 0 and not await next_btn.first.evaluate(
                    "el => el.classList.contains('flatpickr-disabled')"
                ):
                    await page.evaluate("""() => {
                        const btn = document.querySelector('.flatpickr-next-month');
                        if (btn) btn.click();
                    }""")
                    await asyncio.sleep(0.4)
                else:
                    break

            # Try clicking the target day again after month navigation (try both locales)
            for day_label in [en_label, tr_label]:
                day_cell = page.locator(f".flatpickr-day[aria-label='{day_label}']")
                if await day_cell.count() > 0:
                    await page.evaluate(f"""() => {{
                        const day = document.querySelector('.flatpickr-day[aria-label="{day_label}"]');
                        if (day) day.click();
                    }}""")
                    logger.info("Pegasus: selected date %s via JS click (after nav)", day_label)
                    await asyncio.sleep(0.5)
                    return True

            # Fallback: find by day number text
            day_num = str(target.day)
            day_cells = page.locator(
                ".flatpickr-day:not(.prevMonthDay):not(.nextMonthDay):not(.flatpickr-disabled)"
            ).filter(has_text=re.compile(rf"^{day_num}$"))
            if await day_cells.count() > 0:
                await day_cells.first.click(timeout=3000, force=True)
                logger.info("Pegasus: selected day %s via text match", day_num)
                await asyncio.sleep(0.5)
                return True

            logger.warning("Pegasus: could not find day %s in calendar", day_label)
            return False

        except Exception as e:
            logger.warning("Pegasus: date error: %s", e)
            return False

    async def _click_search(self, page) -> None:
        await self._remove_overlays(page)
        # Pegasus uses "SEARCH CHEAP FLIGHTS" (English) or "UCUZ UÇUŞ ARA" (Turkish)
        for label in [
            "SEARCH CHEAP FLIGHTS", "Search Cheap Flights",
            "UCUZ UÇUŞ ARA", "Ucuz Uçuş Ara",
            "Search", "SEARCH", "Search flights", "Find flights",
            "Ara", "ARA",
        ]:
            try:
                btn = page.get_by_role(
                    "button", name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE)
                )
                if await btn.count() > 0:
                    await btn.first.click(timeout=5000)
                    logger.info("Pegasus: clicked search button '%s'", label)
                    return
            except Exception:
                continue
        # Fallback: target by known class on the flight search tab
        try:
            search_btn = page.locator(
                "button.SearchButton__orange-button.tstnm_fly_search_tab_1_search_button"
            ).first
            if await search_btn.count() > 0:
                await search_btn.click(timeout=5000, force=True)
                logger.info("Pegasus: clicked search via class selector")
                return
        except Exception:
            pass
        try:
            await page.locator("button[type='submit']").first.click(timeout=3000)
        except Exception:
            await page.keyboard.press("Enter")

    # ── DOM fallback ───────────────────────────────────────────────────

    async def _extract_from_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        try:
            await asyncio.sleep(3)
            data = await page.evaluate("""() => {
                if (window.__NEXT_DATA__) return window.__NEXT_DATA__;
                if (window.__NUXT__) return window.__NUXT__;
                if (window.appData) return window.appData;
                if (window.__INITIAL_STATE__) return window.__INITIAL_STATE__;
                const scripts = document.querySelectorAll('script[type="application/json"]');
                for (const s of scripts) {
                    try {
                        const d = JSON.parse(s.textContent);
                        if (d && (d.flights || d.outbound || d.journeys || d.fares)) return d;
                    } catch {}
                }
                return null;
            }""")
            if data:
                return self._parse_response(data, req)
        except Exception:
            pass
        return []

    # ── Response parsing ───────────────────────────────────────────────

    def _parse_response(self, data: Any, req: FlightSearchRequest) -> list[FlightOffer]:
        if isinstance(data, list):
            data = {"flights": data}
        currency = self._resolve_currency(data, req)
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        # Pegasus availability API returns departureRouteList → dailyFlightList → flightList
        if "departureRouteList" in data:
            routes = data["departureRouteList"]
            if isinstance(routes, list):
                for route in routes:
                    if not isinstance(route, dict):
                        continue
                    daily_flights = route.get("dailyFlightList") or []
                    if not isinstance(daily_flights, list):
                        continue
                    for daily in daily_flights:
                        if not isinstance(daily, dict):
                            continue
                        # cheapestFare at day level as price fallback
                        day_cheapest = None
                        day_currency = currency
                        cf = daily.get("cheapestFare")
                        if isinstance(cf, dict):
                            day_cheapest = cf.get("amount")
                            day_currency = cf.get("currency") or currency
                        # Each day has flightList with individual flights
                        flight_list = daily.get("flightList") or []
                        if isinstance(flight_list, list):
                            for flight in flight_list:
                                offer = self._parse_pegasus_flight(
                                    flight, day_currency, req, booking_url,
                                    fallback_price=day_cheapest,
                                )
                                if offer:
                                    offers.append(offer)
            if offers:
                return offers

        outbound_raw = (
            data.get("outboundFlights")
            or data.get("outbound")
            or (data.get("journeys", {}).get("outbound") if isinstance(data.get("journeys"), dict) else None)
            or data.get("departureDateFlights")
            or data.get("flights", [])
        )
        if not isinstance(outbound_raw, list):
            outbound_raw = []

        for flight in outbound_raw:
            offer = self._parse_single_flight(flight, currency, req, booking_url)
            if offer:
                offers.append(offer)
        return offers

    def _resolve_currency(self, data: dict, req: FlightSearchRequest) -> str:
        if data.get("currency"):
            return data["currency"]
        if req.origin in _TURKEY_AIRPORTS and req.destination in _TURKEY_AIRPORTS:
            return "TRY"
        return req.currency or "EUR"

    def _parse_pegasus_flight(
        self, flight: dict, currency: str, req: FlightSearchRequest, booking_url: str,
        fallback_price: float | None = None,
    ) -> Optional[FlightOffer]:
        """Parse an individual flight from Pegasus's flightList inside dailyFlightList."""
        if not isinstance(flight, dict):
            return None

        # ── Extract price ────────────────────────────────────────────
        price = None

        # 1) fareBundleList (some endpoints)
        fare_bundles = flight.get("fareBundleList") or flight.get("fareList") or flight.get("fares") or []
        if isinstance(fare_bundles, list) and fare_bundles:
            prices = []
            for fb in fare_bundles:
                if isinstance(fb, dict):
                    p = (fb.get("price") or fb.get("amount") or fb.get("totalPrice")
                         or fb.get("basePrice") or fb.get("adultPrice"))
                    if isinstance(p, dict):
                        p = p.get("amount") or p.get("value")
                    if p is not None:
                        try:
                            prices.append(float(p))
                        except (TypeError, ValueError):
                            pass
            if prices:
                price = min(prices)

        # 2) Single fare object (availability endpoint structure)
        if price is None:
            fare = flight.get("fare") or {}
            if isinstance(fare, dict):
                for key in ("amount", "price", "totalPrice", "basePrice", "adultPrice"):
                    val = fare.get(key)
                    if val is not None:
                        try:
                            price = float(val)
                            break
                        except (TypeError, ValueError):
                            pass
                # Extract currency from fare
                fc = fare.get("currency") or fare.get("currencyCode")
                if fc:
                    currency = str(fc)

        # 3) Direct fields on the flight object
        if price is None:
            price = (flight.get("price") or flight.get("totalPrice")
                     or flight.get("lowestFare") or flight.get("cheapestFare"))
            if isinstance(price, dict):
                currency = price.get("currency") or currency
                price = price.get("amount") or price.get("value")

        # 4) Fallback to day-level cheapestFare
        if price is None and fallback_price is not None:
            price = fallback_price

        if price is None:
            return None
        try:
            price = float(price)
        except (TypeError, ValueError):
            return None
        if price <= 0:
            return None

        # Extract currency from fare bundles if available
        for fb in (fare_bundles if isinstance(fare_bundles, list) else []):
            if isinstance(fb, dict):
                fc = fb.get("currency") or fb.get("currencyCode")
                if isinstance(fc, dict):
                    fc = fc.get("code")
                if fc:
                    currency = str(fc)
                    break

        # ── Build segments ───────────────────────────────────────────
        seg_raw = (flight.get("segmentList") or flight.get("segments")
                   or flight.get("legs") or [])
        segments: list[FlightSegment] = []
        if isinstance(seg_raw, list) and seg_raw:
            for seg in seg_raw:
                if isinstance(seg, dict):
                    segments.append(self._build_segment(seg, req.origin, req.destination))

        # If no explicit segments, the flight itself IS the segment
        if not segments:
            # Build from departure/arrival locations specific to Pegasus
            dep_loc = flight.get("departureLocation") or {}
            arr_loc = flight.get("arrivalLocation") or {}
            origin = dep_loc.get("portCode") or flight.get("origin") or req.origin
            dest = arr_loc.get("portCode") or flight.get("destination") or req.destination
            dep_dt = flight.get("departureDateTime") or flight.get("departure") or ""
            arr_dt = flight.get("arrivalDateTime") or flight.get("arrival") or ""
            flight_no = str(flight.get("flightNo") or flight.get("flightNumber") or "").strip()
            airline = flight.get("airline") or "PC"

            segments.append(FlightSegment(
                airline=airline,
                airline_name="Pegasus Airlines",
                flight_no=f"{airline}{flight_no}" if flight_no and not flight_no.startswith(airline) else flight_no,
                origin=origin,
                destination=dest,
                departure=self._parse_dt(dep_dt),
                arrival=self._parse_dt(arr_dt),
                cabin_class="M",
            ))

        total_dur = 0
        if segments and segments[0].departure and segments[-1].arrival:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())

        # Duration from API data
        if total_dur <= 0:
            fd = flight.get("flightDuration")
            if isinstance(fd, dict):
                vals = fd.get("values") or []
                if isinstance(vals, list) and len(vals) >= 2:
                    try:
                        total_dur = int(vals[0]) * 3600 + int(vals[1]) * 60
                    except (TypeError, ValueError):
                        pass

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur, 0),
            stopovers=max(len(segments) - 1, 0),
        )
        flight_key = (
            flight.get("segmentId") or flight.get("flightKey") or flight.get("id")
            or (flight.get("flightNo", "") + "_" + str(flight.get("departureDateTime", "")))
        )
        return FlightOffer(
            id=f"pc_{hashlib.md5(str(flight_key).encode()).hexdigest()[:12]}",
            price=round(price, 2),
            currency=currency,
            price_formatted=f"{price:.2f} {currency}",
            outbound=route,
            inbound=None,
            airlines=["Pegasus Airlines"],
            owner_airline="PC",
            booking_url=booking_url,
            is_locked=False,
            source="pegasus_direct",
            source_tier="free",
        )

    def _parse_single_flight(
        self, flight: dict, currency: str, req: FlightSearchRequest, booking_url: str,
    ) -> Optional[FlightOffer]:
        price = (
            flight.get("price") or flight.get("totalPrice")
            or flight.get("farePrice") or flight.get("lowestFare")
            or self._extract_cheapest_fare(flight)
        )
        if price is None:
            return None
        try:
            price = float(price)
        except (TypeError, ValueError):
            return None
        if price <= 0:
            return None

        segments_raw = flight.get("segments") or flight.get("legs") or flight.get("flights") or []
        segments: list[FlightSegment] = []
        if segments_raw and isinstance(segments_raw, list):
            for seg in segments_raw:
                segments.append(self._build_segment(seg, req.origin, req.destination))
        else:
            segments.append(self._build_segment(flight, req.origin, req.destination))

        total_dur = 0
        if segments[0].departure and segments[-1].arrival:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur, 0),
            stopovers=max(len(segments) - 1, 0),
        )
        flight_key = (
            flight.get("flightKey") or flight.get("id")
            or flight.get("flightNumber", "") + "_" + segments[0].departure.isoformat()
        )
        return FlightOffer(
            id=f"pc_{hashlib.md5(str(flight_key).encode()).hexdigest()[:12]}",
            price=round(price, 2),
            currency=currency,
            price_formatted=f"{price:.2f} {currency}",
            outbound=route,
            inbound=None,
            airlines=["Pegasus Airlines"],
            owner_airline="PC",
            booking_url=booking_url,
            is_locked=False,
            source="pegasus_direct",
            source_tier="free",
        )

    def _build_segment(self, seg: dict, default_origin: str, default_dest: str) -> FlightSegment:
        dep_str = seg.get("departure") or seg.get("departureDate") or seg.get("departureTime") or seg.get("std") or ""
        arr_str = seg.get("arrival") or seg.get("arrivalDate") or seg.get("arrivalTime") or seg.get("sta") or ""
        flight_no = str(
            seg.get("flightNumber") or seg.get("flight_no") or seg.get("flightNo") or seg.get("number") or ""
        ).replace(" ", "")
        origin = seg.get("origin") or seg.get("departureAirport") or seg.get("departureStation") or default_origin
        destination = seg.get("destination") or seg.get("arrivalAirport") or seg.get("arrivalStation") or default_dest
        return FlightSegment(
            airline="PC", airline_name="Pegasus Airlines", flight_no=flight_no,
            origin=origin, destination=destination,
            departure=self._parse_dt(dep_str), arrival=self._parse_dt(arr_str),
            cabin_class="M",
        )

    @staticmethod
    def _extract_cheapest_fare(flight: dict) -> Optional[float]:
        fares = flight.get("fares") or flight.get("fareBundles") or flight.get("bundles") or []
        prices: list[float] = []
        for f in fares:
            p = f.get("price") or f.get("amount") or f.get("totalPrice") or f.get("basePrice")
            if p is not None:
                try:
                    prices.append(float(p))
                except (TypeError, ValueError):
                    continue
        return min(prices) if prices else None

    # ── Helpers ────────────────────────────────────────────────────────

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("Pegasus %s→%s returned %d offers in %.1fs (Playwright)", req.origin, req.destination, len(offers), elapsed)
        search_hash = hashlib.md5(f"pegasus{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
            currency=offers[0].currency if offers else (req.currency or "EUR"),
            offers=offers, total_results=len(offers),
        )

    @staticmethod
    def _parse_dt(s: Any) -> datetime:
        if not s:
            return datetime(2000, 1, 1)
        s = str(s)
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            pass
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M"):
            try:
                return datetime.strptime(s[:len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://www.flypgs.com/en/booking?from={req.origin}"
            f"&to={req.destination}&departure={dep}"
            f"&adults={req.adults}&children={req.children}&infants={req.infants}"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(f"pegasus{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
            currency=req.currency or "EUR", offers=[], total_results=0,
        )
