"""
ITA Airways connector — CDP Chrome booking page + API interception.

ITA Airways (IATA: AZ) — FCO/MXP hubs, SkyTeam member.

Strategy:
  Launch Chrome with CDP, navigate to ITA booking form, fill origin/dest/date,
  click search, intercept the api.shop.ita-airways.com/one-booking/v2/search/air-bounds
  response which contains structured flight data.

Form automation (100% working):
  - Booking URL: /gb/en/book-and-prepare/book-flights.html
  - Origin/destination: input[placeholder="From|To"] + keyboard typing + suggestion click
  - Calendar: Click date field → ReactModal opens with two-month display
  - One-way: Use page.get_by_text("One way", exact=True) inside calendar modal
  - Month navigation: button[aria-label="Move forward to switch to the next month"]
  - Day selection: Find target month header X position, pick td closest to that center
  - Calendar close: .ReactModal__Content [aria-label*="lose"] selector
  - Button: "Search flights" → changes to "Find flights" after date selection

API details:
  - Search endpoint: api.shop.ita-airways.com/one-booking/v2/search/air-bounds
  - Prices in response are in CENTS (divide by 100)
  - Flight details in dictionaries.flight (keyed by flightId)

BOT DETECTION (Critical limitation):
  shop.ita-airways.com is protected by Cloudflare with aggressive bot detection:
  - TLS fingerprinting (JA3/JA4) - detects CDP/Playwright signature
  - JavaScript challenge ("Performing security verification" page)
  - IP reputation scoring
  - Behavioral fingerprinting
  
  Symptoms:
  - Direct navigation to shop subdomain shows "Performing security verification"
  - Form submission returns 403 "not authorized to access this resource"
  - API calls return 403 "Blocked" regardless of CDP state
  
  Attempted workarounds (none fully effective without proxy):
  - CDP disconnect before button click: Partially works, but 403 persists
  - Human-like delays and mouse movements: Not sufficient
  - Fresh Chrome profile each time: Prevents stale state but doesn't bypass CF
  - Direct shop URL navigation: Hits CF challenge page directly
  
  REQUIRED FOR RELIABLE ACCESS:
  - Set LETSFG_PROXY env var with residential proxy (e.g., http://user:pass@proxy:port)
  - Or use a VPN with clean IP reputation
  - The form automation works perfectly; the blocking is purely server-side
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import random
import shutil
import subprocess
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
from .browser import (
    _BLOCKED_RESOURCE_TYPES,
    _BLOCKED_URL_PATTERNS,
    _launched_procs,
    _launched_pw_instances,
    find_chrome,
    inject_stealth_js,
    is_headless,
    proxy_chrome_args,
    proxy_is_configured,
    stealth_popen_kwargs,
)

# ── ITA-specific resource blocking (allows CF challenge scripts) ──

_ITA_ALLOWED_URL_PATTERNS = (
    "challenges.cloudflare.com",
    "cdnjs.cloudflare.com",
    "turnstile",
    "cf-challenge",
    "hcaptcha.com",
    "recaptcha",
)


async def _ita_block_handler(route):
    """Block heavy resources except Cloudflare challenge scripts.
    
    ITA Airways uses Cloudflare Turnstile — we must allow CF scripts
    to run or the challenge will fail. But we still block images, fonts,
    video, analytics, etc. to save proxy bandwidth.
    """
    req = route.request
    url = req.url.lower()
    
    # Always allow CF challenge scripts (mandatory for bypass)
    for pattern in _ITA_ALLOWED_URL_PATTERNS:
        if pattern in url:
            await route.continue_()
            return
    
    # Block by resource type (images, fonts, video, websockets, etc.)
    if req.resource_type in _BLOCKED_RESOURCE_TYPES:
        await route.abort()
        return
    
    # Block analytics/tracking/ads by URL pattern
    for pattern in _BLOCKED_URL_PATTERNS:
        check = pattern.replace("*", "")
        if check in url:
            await route.abort()
            return
    
    await route.continue_()


async def _ita_auto_block(page) -> None:
    """Enable ITA-specific resource blocking if any proxy is configured.
    
    Blocks images, fonts, video, analytics to save bandwidth while allowing
    Cloudflare challenge scripts to pass.
    """
    # Check both global proxy and ITA-specific proxy
    if proxy_is_configured() or _get_ita_proxy_url():
        await page.route("**/*", _ita_block_handler)
        logger.debug("ITA: resource blocking enabled (proxy detected)")

logger = logging.getLogger(__name__)

_DEBUG_PORT = 9470
_USER_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", ".ita_chrome_profile"
)

_pw_instance = None
_browser = None
_chrome_proc = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


def _get_ita_proxy_url() -> str:
    """Get proxy URL for ITA Airways.
    
    Priority:
    1. ITA_PROXY env var (connector-specific)
    2. LETSFG_PROXY env var (global)
    3. Build from DECODO_PROXY_* env vars
    """
    # Check connector-specific proxy first
    ita_proxy = os.environ.get("ITA_PROXY", "").strip()
    if ita_proxy:
        return ita_proxy
    
    # Check global LETSFG_PROXY
    global_proxy = os.environ.get("LETSFG_PROXY", "").strip()
    if global_proxy:
        return global_proxy
    
    # Build from DECODO_PROXY_* env vars
    server = os.environ.get("DECODO_PROXY_SERVER", "").strip()
    user = os.environ.get("DECODO_PROXY_USER", "").strip()
    passwd = os.environ.get("DECODO_PROXY_PASS", "").strip()
    if server and user and passwd:
        # Parse server and insert credentials
        # server format: http://host:port or host:port
        if server.startswith("http://"):
            return f"http://{user}:{passwd}@{server[7:]}"
        elif server.startswith("https://"):
            return f"https://{user}:{passwd}@{server[8:]}"
        else:
            return f"http://{user}:{passwd}@{server}"
    
    return ""


def _proxy_has_auth() -> bool:
    """Check if an ITA-compatible proxy is configured with username/password."""
    raw = _get_ita_proxy_url()
    if not raw:
        return False
    return "@" in raw  # Basic check for user:pass@host format


async def _get_browser():
    """Launch or connect to a Chrome instance.
    
    When proxy with auth is configured, uses Playwright's native launch
    (which handles proxy auth). Otherwise uses subprocess Chrome with CDP.
    """
    global _pw_instance, _browser, _chrome_proc

    lock = _get_lock()
    async with lock:
        # Clean up previous browser/process
        if _chrome_proc:
            try:
                _chrome_proc.kill()
                _chrome_proc.wait(timeout=3)
            except Exception:
                pass
            _chrome_proc = None
        if _browser:
            try:
                await _browser.close()
            except Exception:
                pass
            _browser = None
        if _pw_instance:
            try:
                await _pw_instance.stop()
            except Exception:
                pass
            _pw_instance = None

        # Fresh profile
        if os.path.exists(_USER_DATA_DIR):
            shutil.rmtree(_USER_DATA_DIR, ignore_errors=True)
        os.makedirs(_USER_DATA_DIR, exist_ok=True)

        # Try Patchright first (better anti-detection), fall back to Playwright
        try:
            from patchright.async_api import async_playwright
            using_patchright = True
            logger.info("ITA: using Patchright (enhanced anti-detection)")
        except ImportError:
            from playwright.async_api import async_playwright
            using_patchright = False

        pw = await async_playwright().start()
        _pw_instance = pw
        _launched_pw_instances.append(pw)

        # Check for ITA-specific proxy (ITA_PROXY, LETSFG_PROXY, or DECODO_PROXY_*)
        proxy_url = _get_ita_proxy_url()
        if proxy_url and _proxy_has_auth():
            # Parse proxy URL into Playwright format
            # Expected: http://user:pass@host:port
            from urllib.parse import urlparse
            parsed = urlparse(proxy_url)
            proxy_settings = {
                "server": f"{parsed.scheme}://{parsed.hostname}:{parsed.port or 80}",
            }
            if parsed.username:
                proxy_settings["username"] = parsed.username
            if parsed.password:
                proxy_settings["password"] = parsed.password
            
            headless = is_headless()  # True on Cloud Run, False for local debugging
            logger.info("ITA: launching Chrome with proxy %s (headless=%s)", 
                       proxy_settings["server"], headless)
            
            # Chrome args for anti-detection and headless compatibility
            chrome_args = [
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--disable-extensions",
                "--disable-infobars",
                "--disable-notifications",
                "--disable-popup-blocking",
                "--ignore-certificate-errors",
                "--no-first-run",
                "--no-sandbox",
            ]
            if headless:
                # Headless-specific args (Cloud Run has no display)
                chrome_args.extend([
                    "--window-position=-2400,-2400",
                    "--window-size=1400,900",
                ])
            else:
                chrome_args.append("--window-size=1400,900")
            
            _browser = await pw.chromium.launch(
                headless=headless,
                args=chrome_args,
                proxy=proxy_settings,
            )
            logger.info("ITA: Chrome launched with proxy (Patchright=%s, headless=%s)", using_patchright, headless)
        else:
            # No proxy or no auth - use subprocess CDP
            logger.info("ITA: no proxy configured (proxy_url=%r, has_auth=%s), using CDP mode",
                       proxy_url[:30] if proxy_url else None, _proxy_has_auth())
            chrome = find_chrome()
            args = [
                chrome,
                f"--remote-debugging-port={_DEBUG_PORT}",
                f"--user-data-dir={_USER_DATA_DIR}",
                "--no-first-run",
                *proxy_chrome_args(),
                "--disable-blink-features=AutomationControlled",
                "--window-size=1400,900",
                "about:blank",
            ]
            _chrome_proc = subprocess.Popen(
                args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
            _launched_procs.append(_chrome_proc)
            await asyncio.sleep(3)

            _browser = await pw.chromium.connect_over_cdp(
                f"http://127.0.0.1:{_DEBUG_PORT}"
            )
            logger.info(
                "ITA: Chrome connected on CDP port %d (pid %d)",
                _DEBUG_PORT,
                _chrome_proc.pid,
            )

        return _browser


class ITAAirwaysConnectorClient:
    """ITA Airways — CDP Chrome booking page + API interception."""

    def __init__(self, timeout: float = 120.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        try:
            browser = await _get_browser()
            context = (
                browser.contexts[0]
                if browser.contexts
                else await browser.new_context(
                    viewport={"width": 1400, "height": 900},
                    locale="en-GB",
                )
            )
            # Use existing page (from about:blank) — keeps one tab so it
            # survives the CDP disconnect/reconnect cycle.
            page = context.pages[0] if context.pages else await context.new_page()
            
            # NOTE: Do NOT call _ita_auto_block (page.route) or inject_stealth_js here.
            # Both are harmful for ITA — CF detects page.route's Fetch.requestPaused
            # and stealth JS defineProperty hooks make the CF challenge worse.
            # Resource blocking is only safe in proxy mode where CF is less aggressive.
            use_proxy = _proxy_has_auth()
            if use_proxy:
                await _ita_auto_block(page)

            # --- API response interception ---
            captured: dict = {}
            api_event = asyncio.Event()

            async def _on_response(response):
                url = response.url
                ct = response.headers.get("content-type", "")
                if response.status != 200 or "json" not in ct:
                    return
                try:
                    body = await response.body()
                    if len(body) < 200:
                        return
                    # Debug: log all JSON responses from shop/api domains
                    if "shop.ita" in url or "api.shop" in url:
                        logger.debug("ITA: JSON response %s (%d bytes)", url[:100], len(body))
                    import json as _json
                    data = _json.loads(body)
                    if not isinstance(data, dict):
                        return
                    # Primary: api.shop.ita-airways.com/one-booking/v2/search/air-bounds
                    data_obj = data.get("data")
                    if isinstance(data_obj, dict) and "airBoundGroups" in data_obj:
                        captured["search"] = data
                        api_event.set()
                        logger.info(
                            "ITA: captured air-bounds API (%d bytes, %d groups)",
                            len(body),
                            len(data_obj["airBoundGroups"]),
                        )
                except Exception:
                    pass

            page.on("response", _on_response)

            # Also listen on context for new pages (in case ITA opens a new tab)
            def _on_new_page(new_page):
                logger.info("ITA: new page opened: %s", new_page.url[:80])
                new_page.on("response", _on_response)
            context.on("page", _on_new_page)

            # --- Navigate to booking page ---
            logger.info("ITA: navigating to booking page")
            await page.goto(
                "https://www.ita-airways.com/gb/en/book-and-prepare/book-flights.html",
                wait_until="load",  # use load instead of networkidle (blocked resources cause timeout)
                timeout=60000,  # increased for Cloudflare challenge
            )
            
            # Wait for Cloudflare JS challenge to complete (takes ~5-10s)
            # Check for real page title, not "Just a moment..." challenge page
            for _cf in range(15):  # up to 15 seconds
                _title = await page.title()
                if "Book flights" in _title or "ITA Airways" in _title:
                    logger.info("ITA: Cloudflare bypass complete after %ds", _cf)
                    break
                if "Just a moment" in _title or "moment" in _title.lower():
                    logger.debug("ITA: waiting for Cloudflare challenge... (%ds)", _cf)
                await asyncio.sleep(1)
            else:
                logger.warning("ITA: Cloudflare challenge may not have completed (title=%r)", _title)
            
            await asyncio.sleep(2)  # Extra stabilization
            
            # Debug: log page state after load
            _title = await page.title()
            _url = page.url
            _body = await page.evaluate("() => document.body?.innerText?.slice(0, 500) || ''")
            logger.info("ITA: page loaded - title=%r url=%s", _title, _url[:100])
            logger.info("ITA: page body preview: %s", _body[:300].replace('\n', ' '))

            # --- Human-like warmup behavior ---
            # Move mouse around, scroll, wait - helps bypass behavioral fingerprinting
            try:
                # Random mouse movements
                for _ in range(3):
                    x = random.randint(200, 1200)
                    y = random.randint(200, 700)
                    await page.mouse.move(x, y, steps=random.randint(5, 15))
                    await asyncio.sleep(random.uniform(0.3, 0.8))
                # Scroll down a bit
                await page.mouse.wheel(0, random.randint(100, 300))
                await asyncio.sleep(random.uniform(0.5, 1.0))
                # Scroll back up
                await page.mouse.wheel(0, random.randint(-150, -50))
                await asyncio.sleep(random.uniform(0.5, 1.0))
            except Exception:
                pass

            # --- Accept cookies & remove OneTrust overlay ---
            # The OneTrust cookie banner places a dark overlay (onetrust-pc-dark-filter)
            # that intercepts ALL pointer events, blocking Playwright locator clicks.
            # Must be forcefully removed via JS before any form interaction.
            await page.evaluate("""() => {
                // Click accept button
                const btn = document.querySelector('#onetrust-accept-btn-handler');
                if (btn) btn.click();
                // Remove all OneTrust overlays and banner elements
                for (const sel of [
                    '.onetrust-pc-dark-filter',
                    '#onetrust-banner-sdk',
                    '#onetrust-consent-sdk',
                ]) {
                    const el = document.querySelector(sel);
                    if (el) el.remove();
                }
            }""")
            await asyncio.sleep(1)

            # Wait for form inputs to appear (widget initialization)
            for _w in range(15):
                has_inputs = await page.evaluate("""() => {
                    const from = document.querySelector('input[placeholder="From"]');
                    const to = document.querySelector('input[placeholder="To"]');
                    return !!(from && to);
                }""")
                if has_inputs:
                    logger.info("ITA: form inputs ready after %ds", _w)
                    break
                await asyncio.sleep(1)
            else:
                logger.info("ITA: form inputs not found after 15s, proceeding anyway")

            # Helper for human-like typing with variable delays
            async def human_type(text: str):
                for char in text:
                    await page.keyboard.type(char, delay=0)
                    await asyncio.sleep(random.uniform(0.08, 0.20))

            # --- Fill origin airport ---
            logger.info("ITA: filling origin %s", req.origin)
            try:
                origin_inp = page.locator('input[placeholder="From"]')
                await origin_inp.click(click_count=3, timeout=5000, force=True)
                await asyncio.sleep(random.uniform(0.2, 0.5))
                await page.keyboard.press("Backspace")
                await asyncio.sleep(random.uniform(0.2, 0.4))
                await human_type(req.origin)
                
                # Wait for suggestions to appear (up to 4 seconds)
                for _sw in range(8):
                    has_suggestions = await page.evaluate("""() => {
                        const items = document.querySelectorAll('li.sel-item, [role="option"]');
                        return items.length > 0;
                    }""")
                    if has_suggestions:
                        logger.debug("ITA: origin suggestions appeared after %dms", _sw * 500)
                        break
                    await asyncio.sleep(0.5)
                
                await asyncio.sleep(0.5)  # Extra stabilization
                
                # Click on matching suggestion
                clicked = await page.evaluate("""(code) => {
                    for (const li of document.querySelectorAll('li.sel-item, [role="option"]')) {
                        const text = (li.innerText || '').toLowerCase();
                        if (text.includes(code.toLowerCase())) {
                            li.click();
                            return true;
                        }
                    }
                    // Fall back to first suggestion
                    const first = document.querySelector('li.sel-item, [role="option"]');
                    if (first) { first.click(); return true; }
                    return false;
                }""", req.origin)
                
                if not clicked:
                    logger.warning("ITA: no origin suggestions found for %s, pressing Enter", req.origin)
                    await page.keyboard.press("Enter")
                    
            except Exception as e:
                logger.warning("ITA: origin fill failed: %s", e)
            await asyncio.sleep(random.uniform(0.8, 1.5))

            # --- Fill destination airport ---
            logger.info("ITA: filling destination %s", req.destination)
            try:
                dest_inp = page.locator('input[placeholder="To"]')
                await dest_inp.click(click_count=3, timeout=5000, force=True)
                await asyncio.sleep(random.uniform(0.2, 0.5))
                await page.keyboard.press("Backspace")
                await asyncio.sleep(random.uniform(0.2, 0.4))
                await human_type(req.destination)
                
                # Wait for suggestions to appear (up to 4 seconds)
                for _sw in range(8):
                    has_suggestions = await page.evaluate("""() => {
                        const items = document.querySelectorAll('li.sel-item, [role="option"]');
                        return items.length > 0;
                    }""")
                    if has_suggestions:
                        logger.debug("ITA: destination suggestions appeared after %dms", _sw * 500)
                        break
                    await asyncio.sleep(0.5)
                
                await asyncio.sleep(0.5)  # Extra stabilization
                
                # Click on matching suggestion
                clicked = await page.evaluate("""(code) => {
                    for (const li of document.querySelectorAll('li.sel-item, [role="option"]')) {
                        const text = (li.innerText || '').toLowerCase();
                        if (text.includes(code.toLowerCase())) {
                            li.click();
                            return true;
                        }
                    }
                    // Fall back to first suggestion
                    const first = document.querySelector('li.sel-item, [role="option"]');
                    if (first) { first.click(); return true; }
                    return false;
                }""", req.destination)
                
                if not clicked:
                    logger.warning("ITA: no destination suggestions found for %s, pressing Enter", req.destination)
                    await page.keyboard.press("Enter")
                    
            except Exception as e:
                logger.warning("ITA: dest fill failed: %s", e)
            await asyncio.sleep(1.0)

            # Verify form state
            form_vals = await page.evaluate("""() => ({
                origin: document.querySelector('input[placeholder="From"]')?.value || '',
                dest: document.querySelector('input[placeholder="To"]')?.value || '',
            })""")
            logger.info("ITA: form state: %s", form_vals)

            # --- Set departure date ---
            target_month = req.date_from.strftime("%B")
            target_day = req.date_from.day
            logger.info("ITA: setting date %s %d", target_month, target_day)

            # Open calendar by clicking date area
            date_opened = False
            date_box = await page.evaluate("""() => {
                const toInp = document.querySelector('input[placeholder="To"]');
                if (toInp) {
                    const r = toInp.getBoundingClientRect();
                    return {x: r.right + 150, y: r.top + r.height/2};
                }
                return null;
            }""")
            if date_box:
                await page.mouse.click(date_box["x"], date_box["y"])
                await asyncio.sleep(2)
                cal_tds = await page.evaluate("""() => {
                    return [...document.querySelectorAll('td')].filter(
                        td => /^\\d{1,2}$/.test((td.innerText||'').trim())
                    ).length;
                }""")
                if cal_tds >= 10:
                    date_opened = True
                    logger.info("ITA: calendar opened (%d tds)", cal_tds)

            if date_opened:
                # --- Click "One way" checkbox INSIDE the calendar modal ---
                # This is a custom web component; only Playwright's get_by_text
                # can find it (not evaluate-based DOM queries). Previous runs
                # confirmed this makes the button change to "Find flights" and
                # removes the return date requirement.
                logger.info("ITA: checking One-way checkbox in calendar")
                ow_clicked = False
                try:
                    ow_loc = page.get_by_text("One way", exact=True)
                    if await ow_loc.count() > 0:
                        await ow_loc.first.click(timeout=3000)
                        ow_clicked = True
                        logger.info("ITA: One-way clicked via get_by_text")
                except Exception as e:
                    logger.debug("ITA: get_by_text One way: %s", e)

                if not ow_clicked:
                    try:
                        ow_loc2 = page.locator("text=One way")
                        if await ow_loc2.count() > 0:
                            await ow_loc2.first.click(timeout=3000)
                            ow_clicked = True
                            logger.info("ITA: One-way clicked via text= locator")
                    except Exception as e:
                        logger.debug("ITA: text=One way: %s", e)

                if ow_clicked:
                    await asyncio.sleep(1.0)

                # Navigate to target month using Playwright locator clicks
                for nav_i in range(12):
                    vis = await page.evaluate("""(month) => {
                        return document.body.innerText.includes(month);
                    }""", target_month)
                    if vis:
                        logger.info("ITA: month %s visible after %d clicks",
                                    target_month, nav_i)
                        break
                    # Try both with and without trailing period (ITA uses both)
                    nav_clicked = False
                    for aria_sel in [
                        'button[aria-label="Move forward to switch to the next month."]',
                        'button[aria-label="Move forward to switch to the next month"]',
                    ]:
                        next_btn = page.locator(aria_sel)
                        if await next_btn.count() > 0:
                            await next_btn.first.click()
                            nav_clicked = True
                            break
                    if not nav_clicked:
                        # Fallback: find any forward/next button
                        await page.evaluate("""() => {
                            for (const b of document.querySelectorAll('button')) {
                                const al = (b.getAttribute('aria-label') || '').toLowerCase();
                                if (al.includes('next') || al.includes('forward'))
                                    { b.click(); return; }
                            }
                        }""")
                    await asyncio.sleep(0.7)

                await asyncio.sleep(0.5)

                # Click day using mouse.click(x,y) — required for React state.
                # Pick from the panel that shows the TARGET month (left or right
                # panel in the two-month calendar). Find the month header x-pos
                # and pick the td closest to it.
                day_pos = await page.evaluate("""(params) => {
                    const {dayNum, monthName} = params;
                    // Find x-center of the target month header
                    // Look specifically for header-like elements (h2, h3, div, span)
                    // that contain "MonthName YYYY" text
                    let monthCenterX = 0;
                    let headerInfo = '';
                    for (const el of document.querySelectorAll('h1, h2, h3, h4, div, span, p, caption')) {
                        const t = (el.innerText || '').trim();
                        // Match exact month name (e.g., "June" or "June 2026")
                        if (t === monthName || t.startsWith(monthName + ' ')) {
                            const r = el.getBoundingClientRect();
                            // Header should be < 300px wide (one panel, not full modal)
                            // and in the calendar area
                            if (r.width > 30 && r.width < 400 && r.y > 100 && r.y < 600) {
                                monthCenterX = r.x + r.width / 2;
                                headerInfo = t + ' @ x=' + Math.round(r.x) + ' w=' + Math.round(r.width) + ' y=' + Math.round(r.y);
                                break;
                            }
                        }
                    }
                    // Find all tds with the target day number
                    const tds = document.querySelectorAll('td');
                    const matches = [];
                    for (const d of tds) {
                        if ((d.innerText || '').trim() === String(dayNum)) {
                            const r = d.getBoundingClientRect();
                            if (r.width > 0)
                                matches.push({
                                    x: r.x + r.width/2,
                                    y: r.y + r.height/2,
                                    dist: Math.abs(r.x + r.width/2 - monthCenterX)
                                });
                        }
                    }
                    // Pick the td closest to the target month header
                    if (!matches.length) return null;
                    matches.sort((a, b) => a.dist - b.dist);
                    return {x: matches[0].x, y: matches[0].y, headerInfo, monthCenterX, matchCount: matches.length};
                }""", {"dayNum": target_day, "monthName": target_month})
                if day_pos:
                    await page.mouse.click(day_pos["x"], day_pos["y"])
                    logger.info("ITA: day %d clicked at (%.0f, %.0f) [header: %s, monthCX: %.0f, matches: %d]",
                                target_day, day_pos["x"], day_pos["y"],
                                day_pos.get("headerInfo", "?"), day_pos.get("monthCenterX", 0),
                                day_pos.get("matchCount", 0))
                else:
                    logger.warning("ITA: day %d not found in calendar", target_day)
                await asyncio.sleep(1.0)

                # For round-trip, click return date too
                if req.date_to:
                    ret_month = req.date_to.strftime("%B")
                    ret_day = req.date_to.day
                    # Navigate to return month if different
                    for _ in range(6):
                        if await page.evaluate("(m) => document.body.innerText.includes(m)", ret_month):
                            break
                        for aria_sel in [
                            'button[aria-label="Move forward to switch to the next month."]',
                            'button[aria-label="Move forward to switch to the next month"]',
                        ]:
                            next_btn = page.locator(aria_sel)
                            if await next_btn.count() > 0:
                                await next_btn.first.click()
                                break
                        await asyncio.sleep(0.7)
                    ret_pos = await page.evaluate("""(dayNum) => {
                        const tds = document.querySelectorAll('td');
                        const matches = [];
                        for (const d of tds) {
                            if ((d.innerText || '').trim() === String(dayNum)) {
                                const r = d.getBoundingClientRect();
                                if (r.width > 0)
                                    matches.push({x: r.x + r.width/2, y: r.y + r.height/2, left: r.x});
                            }
                        }
                        matches.sort((a, b) => b.left - a.left);
                        return matches[0] || null;
                    }""", ret_day)
                    if ret_pos:
                        await page.mouse.click(ret_pos["x"], ret_pos["y"])
                        logger.info("ITA: return day %d clicked", ret_day)
                    await asyncio.sleep(1.0)
            else:
                logger.warning("ITA: could not open calendar")

            # Close calendar — click the X button at top-right of the modal.
            # The X button is inside .ReactModal__Content, near the top-right.
            # Must use bounding-box detection because it may be a <button>,
            # <span>, <svg>, or <div> element.
            logger.info("ITA: closing calendar")
            cal_closed = False

            # Method 1: Find close button by aria-label or class containing "close"
            for sel in [
                '.ReactModal__Content [aria-label*="lose"]',
                '.ReactModal__Content [aria-label*="lose" i]',
                '.ReactModal__Content button.close',
                '.ReactModal__Content .close-button',
                '.ReactModal__Content button:first-child',
            ]:
                try:
                    loc = page.locator(sel)
                    if await loc.count() > 0:
                        await loc.first.click(timeout=2000)
                        cal_closed = True
                        logger.info("ITA: clicked close via selector: %s", sel)
                        break
                except Exception:
                    pass

            # Method 2: Find the top-right corner element of the modal
            if not cal_closed:
                x_pos = await page.evaluate("""() => {
                    const modal = document.querySelector('.ReactModal__Content');
                    if (!modal) return null;
                    const mr = modal.getBoundingClientRect();
                    // Find any element near top-right of modal (X button area)
                    const candidates = modal.querySelectorAll('*');
                    for (const el of candidates) {
                        const r = el.getBoundingClientRect();
                        const children = el.children.length;
                        // X button: small element near top-right
                        if (r.width > 5 && r.width < 80 && r.height > 5 && r.height < 80
                            && r.x > mr.x + mr.width - 120
                            && r.y < mr.y + 100
                            && children <= 2) {
                            return {x: r.x + r.width/2, y: r.y + r.height/2, tag: el.tagName, w: Math.round(r.width)};
                        }
                    }
                    // Fallback: top-right corner of modal
                    return {x: mr.x + mr.width - 40, y: mr.y + 40, tag: 'fallback', w: 0};
                }""")
                if x_pos:
                    await page.mouse.click(x_pos["x"], x_pos["y"])
                    cal_closed = True
                    logger.info("ITA: clicked X at (%.0f, %.0f) tag=%s w=%d",
                                x_pos["x"], x_pos["y"], x_pos.get("tag", "?"), x_pos.get("w", 0))

            # Method 3: Escape key
            if not cal_closed:
                await page.keyboard.press("Escape")
                logger.info("ITA: pressed Escape as fallback")

            await asyncio.sleep(2.0)

            # Verify calendar is closed and check button text
            post_cal = await page.evaluate("""() => {
                const modal = document.querySelector('.ReactModal__Content');
                const visible = modal && modal.getBoundingClientRect().width > 0
                    && getComputedStyle(modal.closest('.ReactModal__Overlay') || modal).display !== 'none';
                let btnText = '';
                for (const el of document.querySelectorAll('maui-button, button')) {
                    const t = (el.innerText || '').trim().toLowerCase();
                    if (t.includes('find flight') || t.includes('search flight')) {
                        btnText = t; break;
                    }
                }
                return { calVisible: !!visible, btnText };
            }""")
            logger.info("ITA: post-calendar: %s", post_cal)

            # If calendar still visible, try Escape again
            if post_cal.get("calVisible"):
                logger.info("ITA: calendar still visible, pressing Escape again")
                await page.keyboard.press("Escape")
                await asyncio.sleep(1.5)

            # --- Wait for widget to be ready ---
            widget_ready = False
            for _w in range(10):
                btn_text = await page.evaluate("""() => {
                    for (const el of document.querySelectorAll('maui-button, button')) {
                        const t = (el.innerText || '').trim().toLowerCase();
                        if (t.includes('find flight') || t.includes('search flight'))
                            return t;
                    }
                    return null;
                }""")
                if btn_text:
                    widget_ready = True
                    logger.info("ITA: widget ready (%s) after %ds", btn_text, _w)
                    break
                await asyncio.sleep(1)
            if not widget_ready:
                logger.info("ITA: widget not initialized after 10s, trying anyway")

            # Debug: check form state before clicking search
            pre_state = await page.evaluate("""() => {
                const body = (document.body.innerText || '');
                const hasError = body.includes('error has occurred');
                const calOpen = !!document.querySelector('.ReactModal__Overlay');
                const depVal = document.querySelector('input[placeholder="From"]')?.value || '';
                const destVal = document.querySelector('input[placeholder="To"]')?.value || '';
                return {hasError, calOpen, depVal, destVal};
            }""")
            logger.info("ITA: pre-search state: %s", pre_state)

            try:
                await page.screenshot(path="_ita_pre_search.png", timeout=5000)
            except Exception:
                pass

            # --- Click search button ---
            # Different strategy depending on proxy:
            # - With proxy: Click button normally (browser handles CF with residential IP)
            # - Without proxy: CDP disconnect approach (disconnect CDP, let Chrome pass CF alone)
            
            if use_proxy:
                # Residential proxy - click the button and let browser handle navigation
                # This is more natural than direct URL navigation and may pass CF better
                logger.info("ITA: clicking Find flights button (proxy mode)")
                
                clicked = False
                # Method 1: Playwright role locator
                try:
                    btn = page.get_by_role("button", name="Find flights")
                    if await btn.count() > 0:
                        await btn.first.click(timeout=5000)
                        clicked = True
                        logger.info("ITA: clicked 'Find flights' via role")
                except Exception as e:
                    logger.debug("ITA: role click failed: %s", e)
                
                # Method 2: maui-button locator
                if not clicked:
                    try:
                        btn = page.locator('maui-button:has-text("Find flights")')
                        if await btn.count() > 0:
                            await btn.first.click(timeout=5000)
                            clicked = True
                            logger.info("ITA: clicked via maui-button locator")
                    except Exception as e:
                        logger.debug("ITA: maui-button click failed: %s", e)
                
                # Method 3: JS click
                if not clicked:
                    try:
                        await page.evaluate("""() => {
                            for (const el of document.querySelectorAll('maui-button, button')) {
                                const t = (el.innerText || '').trim().toLowerCase();
                                if (t.includes('find') && t.includes('flight')) {
                                    el.click();
                                    return;
                                }
                            }
                        }""")
                        clicked = True
                        logger.info("ITA: clicked via JS")
                    except Exception:
                        pass
                
                if not clicked:
                    logger.warning("ITA: could not click Find flights button")
                
                # Wait for navigation to shop page
                logger.info("ITA: waiting for navigation to shop.ita...")
                on_shop = False
                for _nav in range(20):  # up to 20 seconds
                    await asyncio.sleep(1)
                    if "shop.ita-airways.com" in page.url:
                        on_shop = True
                        logger.info("ITA: reached shop page after %ds: %s", _nav + 1, page.url[:80])
                        break
                
                if not on_shop:
                    logger.warning("ITA: navigation to shop page failed, URL: %s", page.url[:80])
                
                # On shop page, wait for CF challenge and flight results
                if on_shop:
                    # Wait for CF challenge to complete
                    cf_done = False
                    for _cf2 in range(30):  # up to 30 seconds for CF
                        cf_state = await page.evaluate("""() => {
                            const body = (document.body.innerText || '').toLowerCase();
                            const title = document.title?.toLowerCase() || '';
                            return {
                                hasCF: title.includes('moment') || body.includes('security') && body.includes('verification'),
                            };
                        }""")
                        if not cf_state.get("hasCF"):
                            logger.info("ITA: shop CF challenge passed after %ds", _cf2)
                            cf_done = True
                            break
                        await asyncio.sleep(1)
                    
                    if cf_done:
                        await asyncio.sleep(2)  # Let page initialize
                        
                        # Handle cookie consent modal on shop page
                        # LHG uses "Accept all" button 
                        try:
                            consent_btn = page.get_by_role("button", name="Accept all")
                            if await consent_btn.count() > 0:
                                await consent_btn.click(timeout=3000)
                                logger.info("ITA: clicked 'Accept all' cookie consent")
                                await asyncio.sleep(2)
                        except Exception as e:
                            logger.debug("ITA: no cookie consent button: %s", e)
                        
                        # Also try JS click for any consent modal
                        await page.evaluate("""() => {
                            // LHG consent buttons
                            for (const txt of ['Accept all', 'accept all', 'Accept All', 'Akzeptieren']) {
                                for (const btn of document.querySelectorAll('button')) {
                                    if (btn.innerText?.trim() === txt) { btn.click(); return; }
                                }
                            }
                        }""")
                        
                        # Wait for flight results to appear
                        for _res in range(20):  # up to 20 more seconds for results
                            has_results = await page.evaluate("""() => {
                                const body = (document.body.innerText || '').toLowerCase();
                                return body.includes('from €') || body.includes('eur ')
                                    || body.includes('select your') || body.includes('outbound')
                                    || (body.includes('price') && body.includes('select'))
                                    || body.includes('flight details');
                            }""")
                            if has_results:
                                logger.info("ITA: shop page loaded with results after %ds total", _cf2 + _res + 2)
                                break
                            await asyncio.sleep(1)
                        else:
                            logger.warning("ITA: results not detected after waiting")
                    else:
                        logger.warning("ITA: shop CF challenge did not complete after 30s")
                
                # Extra wait for results to load
                await asyncio.sleep(3)
                
                # Check page state
                page_state = await page.evaluate("""() => {
                    const body = (document.body.innerText || '').toLowerCase();
                    return {
                        hasFlights: body.includes('economy') || body.includes('eur ')
                            || body.includes('from €') || body.includes('select'),
                        hasError: body.includes('error') || body.includes('not authorized')
                            || body.includes('problem') || body.includes('try again later'),
                        hasBlockError: body.includes('try again later') || body.includes('not authorized'),
                        hasCF: body.includes('security') && body.includes('verification'),
                        url: location.href,
                    };
                }""")
                logger.info("ITA: page state after click: %s", page_state)
                
                try:
                    await page.screenshot(path="_ita_debug_results.png", timeout=5000)
                except Exception:
                    pass
                
            else:
                # No proxy — CDP disconnect approach (proven working).
                # CF detects the CDP protocol itself. By disconnecting CDP before
                # the button click, Chrome passes CF as a regular browser.
                logger.info("ITA: CDP disconnect mode — scheduling button click")

                # Get button coordinates for dispatchEvent
                btn_info = await page.evaluate("""() => {
                    for (const el of document.querySelectorAll('maui-button, button, [role="button"]')) {
                        const t = (el.innerText || '').trim();
                        if (t.toLowerCase().includes('find') || t.toLowerCase().includes('search')) {
                            const r = el.getBoundingClientRect();
                            if (r.width > 0) return {x: r.x + r.width/2, y: r.y + r.height/2, text: t.slice(0, 40)};
                        }
                    }
                    return null;
                }""")
                if not btn_info:
                    logger.warning("ITA: no search button found for CDP disconnect click")
                    return self._empty(req)

                bx, by = btn_info["x"], btn_info["y"]
                logger.info("ITA: button '%s' at (%.0f, %.0f)", btn_info["text"], bx, by)

                # Schedule full dispatchEvent click sequence via setTimeout.
                # Plain el.click() doesn't trigger Angular form submission on <MAUI-BUTTON>.
                # Need full MouseEvent sequence + el.click() + shadow DOM inner button click.
                await page.evaluate("""(params) => {
                    const {bx, by} = params;
                    setTimeout(() => {
                        console.log('ITA: setTimeout fired, clicking at', bx, by);
                        // Method 1: Full mouse event sequence at coordinates
                        const el = document.elementFromPoint(bx, by);
                        if (el) {
                            ['mousedown', 'mouseup', 'click'].forEach(type => {
                                el.dispatchEvent(new MouseEvent(type, {
                                    bubbles: true, cancelable: true, view: window,
                                    clientX: bx, clientY: by, button: 0, buttons: 1,
                                }));
                            });
                            el.click();
                        }
                        // Method 2: Find button by text + shadow DOM inner button
                        for (const el2 of document.querySelectorAll('maui-button, button')) {
                            const t = (el2.innerText || '').trim().toLowerCase();
                            if (t.includes('find flight') || t.includes('search flight')) {
                                el2.click();
                                const inner = el2.shadowRoot?.querySelector('button') || el2.querySelector('button');
                                if (inner) inner.click();
                                break;
                            }
                        }
                    }, 3000);
                }""", {"bx": bx, "by": by})

                # Disconnect CDP — Chrome continues as standalone subprocess
                logger.info("ITA: disconnecting CDP")
                try:
                    await _browser.close()
                except Exception:
                    pass
                try:
                    await _pw_instance.stop()
                except Exception:
                    pass
                _browser = None
                _pw_instance = None

                # Wait for: 3s setTimeout + navigation + CF challenge + results load
                wait_secs = 65
                logger.info("ITA: waiting %ds for search to complete (no CDP)", wait_secs)
                await asyncio.sleep(wait_secs)

                # Reconnect via Patchright (better anti-detection than plain Playwright)
                try:
                    from patchright.async_api import async_playwright as _ap2
                except ImportError:
                    from playwright.async_api import async_playwright as _ap2

                pw2 = await _ap2().start()
                _pw_instance = pw2
                _launched_pw_instances.append(pw2)
                try:
                    _browser = await pw2.chromium.connect_over_cdp(
                        f"http://127.0.0.1:{_DEBUG_PORT}"
                    )
                except Exception as reconn_err:
                    logger.warning("ITA: reconnect failed: %s", reconn_err)
                    return self._empty(req)
                logger.info("ITA: reconnected to Chrome")

                context = _browser.contexts[0] if _browser.contexts else None
                if not context:
                    logger.warning("ITA: no context after reconnect")
                    return self._empty(req)

                # Find the results page (shop.ita-airways.com)
                page = None
                for p in context.pages:
                    try:
                        pu = p.url
                    except Exception:
                        continue
                    if "shop.ita" in pu or "availability" in pu:
                        page = p
                        logger.info("ITA: found shop page: %s", pu[:80])
                        break
                if not page:
                    page = context.pages[-1] if context.pages else None
                    if page:
                        logger.info("ITA: no shop page, using last: %s", page.url[:80])

                if not page:
                    logger.warning("ITA: no page after reconnect")
                    return self._empty(req)

                # Check page state
                page_state = await page.evaluate("""() => {
                    const body = (document.body.innerText || '').toLowerCase();
                    return {
                        hasFlights: body.includes('economy') || body.includes('eur ')
                            || body.includes('from €') || body.includes('select'),
                        hasError: body.includes('error') || body.includes('not authorized'),
                        hasBlockError: body.includes('try again later') || body.includes('not authorized'),
                        hasCF: body.includes('security') && body.includes('verification'),
                        url: location.href,
                    };
                }""")
                logger.info("ITA: page state after reconnect: %s", page_state)

                # Attach response listener and reload to capture API data
                # (the initial page load happened without CDP, so we missed the API call;
                # reloading triggers a fresh air-bounds request that we can intercept)
                page.on("response", _on_response)
                logger.info("ITA: reloading page to capture API response")
                try:
                    await page.reload(wait_until="networkidle", timeout=30000)
                except Exception as reload_err:
                    logger.debug("ITA: reload: %s", reload_err)
                await asyncio.sleep(5)

            # --- Wait for the air-bounds API response ---
            # The response interceptor was set up before navigation.
            # Wait for it to capture the search API call.
            try:
                await asyncio.wait_for(api_event.wait(), timeout=30)
            except asyncio.TimeoutError:
                logger.info("ITA: API not captured after 30s, trying DOM scrape")
                try:
                    await page.screenshot(path="_ita_debug_results.png", timeout=10000)
                except Exception:
                    pass

            # --- Parse results ---
            offers: list[FlightOffer] = []
            if "search" in captured:
                offers = self._parse_search(captured["search"], req)

            # Fallback: scrape DOM for prices
            if not offers:
                offers = await self._scrape_dom(page, req)

            # Check if we're still blocked
            if not offers:
                logger.warning(
                    "ITA: no offers found. CF may have blocked the request. "
                    "CDP disconnect approach works best with a residential IP."
                )

            offers.sort(key=lambda o: o.price)
            elapsed = time.monotonic() - t0
            logger.info("ITA %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

            sh = hashlib.md5(
                f"ita{req.origin}{req.destination}{req.date_from}".encode()
            ).hexdigest()[:12]
            return FlightSearchResponse(
                search_id=f"fs_{sh}",
                origin=req.origin,
                destination=req.destination,
                currency=offers[0].currency if offers else "EUR",
                offers=offers,
                total_results=len(offers),
            )
        except Exception as e:
            logger.warning("ITA search error: %s", e)
            return self._empty(req)
        finally:
            try:
                page.remove_listener("response", _on_response)
                await page.close()
            except Exception:
                pass

    # ---- Parsers ----

    def _parse_search(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse api.shop.ita-airways.com/one-booking/v2/search/air-bounds response.

        Structure:
          data.airBoundGroups[]: each is a route option (direct or connecting)
            boundDetails: { originLocationCode, destinationLocationCode, duration, segments: [{flightId}] }
            airBounds[]: fare options per route (economy, classic, flex, business)
              airOffer.totalPrice: { value (cents!), currencyCode }
          dictionaries.flight: { flightId: { departure, arrival, marketingAirlineCode, ... } }
          dictionaries.airline: { code: name }
        """
        offers: list[FlightOffer] = []
        booking_url = self._booking_url(req)

        search_data = data.get("data", {})
        dicts = data.get("dictionaries", {})
        flight_dict = dicts.get("flight", {})
        airline_dict = dicts.get("airline", {})
        groups = search_data.get("airBoundGroups", [])

        if not groups:
            return offers

        seen_keys: set[str] = set()

        for group in groups:
            bd = group.get("boundDetails", {})
            origin_code = bd.get("originLocationCode", req.origin)
            dest_code = bd.get("destinationLocationCode", req.destination)
            route_dur = bd.get("duration", 0)
            seg_refs = bd.get("segments", [])

            # Build segments from flight dictionary
            segments: list[FlightSegment] = []
            airlines_set: set[str] = set()
            for sref in seg_refs:
                fid = sref.get("flightId", "")
                fl = flight_dict.get(fid, {})
                dep = fl.get("departure", {})
                arr = fl.get("arrival", {})
                mkt_code = fl.get("marketingAirlineCode", "AZ")
                mkt_num = fl.get("marketingFlightNumber", "")
                airline_name = airline_dict.get(mkt_code, "ITA Airways")
                airlines_set.add(airline_name)

                segments.append(FlightSegment(
                    airline=mkt_code,
                    airline_name=airline_name,
                    flight_no=f"{mkt_code}{mkt_num}",
                    origin=dep.get("locationCode", origin_code),
                    destination=arr.get("locationCode", dest_code),
                    departure=self._parse_dt(dep.get("dateTime")),
                    arrival=self._parse_dt(arr.get("dateTime")),
                ))

            stopovers = max(len(segments) - 1, 0)
            route = FlightRoute(
                segments=segments or [FlightSegment(
                    airline="AZ", airline_name="ITA Airways", flight_no="AZ",
                    origin=req.origin, destination=req.destination,
                    departure=datetime.combine(req.date_from, datetime.min.time().replace(hour=8)),
                    arrival=datetime.combine(req.date_from, datetime.min.time().replace(hour=8)),
                )],
                total_duration_seconds=route_dur,
                stopovers=stopovers,
            )

            # Take cheapest fare per group (first airBound is usually isCheapestOffer)
            for ab in group.get("airBounds", []):
                tp = ab.get("airOffer", {}).get("totalPrice", {})
                price_cents = tp.get("value")
                currency = tp.get("currencyCode", "EUR")

                if price_cents is None:
                    total_prices = ab.get("prices", {}).get("totalPrices", [])
                    if total_prices:
                        price_cents = total_prices[0].get("total")
                        currency = total_prices[0].get("currencyCode", "EUR")

                if price_cents is None or price_cents <= 0:
                    continue

                price = round(price_cents / 100, 2)

                ff_code = ab.get("fareFamilyCode", "")
                dedup_key = f"az_{origin_code}{dest_code}{price}{ff_code}"
                if dedup_key in seen_keys:
                    continue
                seen_keys.add(dedup_key)

                oid = hashlib.md5(dedup_key.encode()).hexdigest()[:12]
                airlines_list = sorted(airlines_set) if airlines_set else ["ITA Airways"]
                offers.append(FlightOffer(
                    id=f"az_{oid}",
                    price=price,
                    currency=currency,
                    price_formatted=f"{price:.2f} {currency}",
                    outbound=route,
                    inbound=None,
                    airlines=airlines_list,
                    owner_airline="AZ",
                    booking_url=booking_url,
                    is_locked=False,
                    source="itaairways_direct",
                    source_tier="free",
                ))
                break  # Only cheapest fare per group

        return offers

    async def _scrape_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Fallback: scrape pricing from page DOM."""
        booking_url = self._booking_url(req)
        try:
            data = await page.evaluate(r"""() => {
                const results = [];
                const body = document.body?.innerText || '';
                const priceRe = /(?:EUR|€)\s*([\d.,]+)|(\d{1,4}(?:[.,]\d{3})*(?:[.,]\d{2})?)\s*(?:EUR|€)/gi;
                let m;
                while ((m = priceRe.exec(body)) !== null) {
                    const raw = (m[1] || m[2] || '').replace(/\./g, '').replace(',', '.');
                    const p = parseFloat(raw);
                    if (p > 15 && p < 10000) results.push(p);
                }
                return [...new Set(results)].sort((a, b) => a - b).slice(0, 10);
            }""")
            offers = []
            for price in (data or []):
                dep_dt = datetime.combine(req.date_from, datetime.min.time().replace(hour=8))
                seg = FlightSegment(
                    airline="AZ", airline_name="ITA Airways", flight_no="AZ",
                    origin=req.origin, destination=req.destination,
                    departure=dep_dt, arrival=dep_dt,
                )
                route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)
                key = f"az_dom_{req.origin}{req.destination}{price}"
                oid = hashlib.md5(key.encode()).hexdigest()[:12]
                offers.append(FlightOffer(
                    id=f"az_{oid}",
                    price=round(price, 2),
                    currency="EUR",
                    price_formatted=f"{price:.2f} EUR",
                    outbound=route,
                    inbound=None,
                    airlines=["ITA Airways"],
                    owner_airline="AZ",
                    conditions={"price_type": "starting_from"},
                    booking_url=booking_url,
                    is_locked=False,
                    source="itaairways_direct",
                    source_tier="free",
                ))
            return offers
        except Exception as e:
            logger.warning("ITA DOM scrape failed: %s", e)
            return []

    # ---- Helpers ----

    async def _fill_field(self, page, selector: str, code: str):
        """Fill an airport input field using mouse.click at computed position.

        Uses page.evaluate to get the element position, then page.mouse.click
        for the actual click (bypasses all Playwright visibility/overlay checks),
        then page.keyboard for typing (sends proper CDP key events that React handles).
        """
        # Get element position via JS
        pos = await page.evaluate(f"""() => {{
            const inp = document.querySelector('{selector}');
            if (!inp) return null;
            inp.scrollIntoView();
            const rect = inp.getBoundingClientRect();
            return {{x: rect.x + rect.width/2, y: rect.y + rect.height/2, w: rect.width}};
        }}""")
        if not pos or pos.get("w", 0) < 10:
            logger.warning("ITA: field %s not found or too small: %s", selector, pos)
            return

        # Triple-click to select all text, then delete + type
        await page.mouse.click(pos["x"], pos["y"], click_count=3)
        await asyncio.sleep(0.3)
        await page.keyboard.press("Backspace")
        await asyncio.sleep(0.2)
        await page.keyboard.type(code, delay=100)
        await asyncio.sleep(2.5)

        # Pick matching suggestion from typeahead dropdown
        await page.evaluate("""(code) => {
            for (const li of document.querySelectorAll('li.sel-item, [role="option"]')) {
                const t = (li.innerText || '').toLowerCase();
                if (t.includes(code.toLowerCase())) { li.click(); return; }
            }
            document.querySelector('li.sel-item')?.click();
        }""", code)

    @staticmethod
    def _parse_dt(s) -> datetime:
        if not s:
            return datetime(2000, 1, 1)
        s = str(s)
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            pass
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%d/%m/%Y %H:%M"):
            try:
                return datetime.strptime(s[:len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%d/%m/%Y")
        return (
            f"https://www.ita-airways.com/gb/en/book-and-prepare/book-flights.html"
            f"?from={req.origin}&to={req.destination}"
            f"&departureDate={dep}&adults={req.adults or 1}&tripType=OW"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        sh = hashlib.md5(
            f"ita{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{sh}",
            origin=req.origin,
            destination=req.destination,
            currency="EUR",
            offers=[],
            total_results=0,
        )
