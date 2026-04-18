"""
Citilink CDP Chrome connector — Navitaire IBE form fill.

Citilink (IATA: QG) is an Indonesian low-cost carrier (Garuda subsidiary).
Booking engine at book.citilink.co.id is Navitaire behind Cloudflare WAF —
all HTTP requests return 403. Real Chrome required.

Strategy (CDP Chrome + form fill + response/DOM scraping):
1. Launch real system Chrome headed + off-screen.
2. Navigate to book.citilink.co.id or www.citilink.co.id.
3. Fill search form (origin, destination, date, passengers).
4. Submit → wait for availability page.
5. Intercept Navitaire API responses or scrape rendered DOM.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
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
from .browser import (
    find_chrome,
    stealth_popen_kwargs,
    _launched_procs,
    acquire_browser_slot,
    release_browser_slot,
    proxy_chrome_args,
    auto_block_if_proxied,
)

logger = logging.getLogger(__name__)

_BOOK_URL = "https://book.citilink.co.id/Search.aspx"
_HOME_URL = "https://www.citilink.co.id/"

# Indonesian domestic + short-haul international routes
_VALID_IATA: set[str] = {
    "CGK",  # Jakarta (Soekarno-Hatta)
    "HLP",  # Jakarta (Halim)
    "SUB",  # Surabaya
    "DPS",  # Denpasar (Bali)
    "JOG",  # Yogyakarta (Adisucipto)
    "YIA",  # Yogyakarta (YIA)
    "SOC",  # Solo
    "SRG",  # Semarang
    "BDO",  # Bandung
    "BPN",  # Balikpapan
    "UPG",  # Makassar
    "MDC",  # Manado
    "PDG",  # Padang
    "KNO",  # Medan (Kualanamu)
    "PLM",  # Palembang
    "PKU",  # Pekanbaru
    "BTH",  # Batam
    "PNK",  # Pontianak
    "TKG",  # Bandar Lampung
    "BDJ",  # Banjarmasin
    "LOP",  # Lombok
    "AMQ",  # Ambon
    "DJB",  # Jambi
    "DJJ",  # Jayapura
    "LBJ",  # Labuan Bajo
    "KOE",  # Kupang
    "KUL",  # Kuala Lumpur
    "SIN",  # Singapore
    "PEN",  # Penang
    "BKK",  # Bangkok
    "JED",  # Jeddah
    "MED",  # Medina
}

# CDP Chrome state
_DEBUG_PORT = 9335
_USER_DATA_DIR = os.path.join(os.path.expanduser("~"), ".letsfg_citilink_cdp")
_browser = None
_pw_instance = None
_chrome_proc: Optional[subprocess.Popen] = None
_context = None


async def _get_browser():
    """Get or launch persistent Chrome browser for Citilink."""
    global _browser, _pw_instance, _chrome_proc, _context

    if _browser is not None:
        try:
            if _browser.is_connected():
                return _browser
        except Exception:
            pass

    from playwright.async_api import async_playwright

    pw = None
    try:
        pw = await async_playwright().start()
        _browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{_DEBUG_PORT}")
        _pw_instance = pw
        logger.info("Citilink: connected to existing Chrome on port %d", _DEBUG_PORT)
        return _browser
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
        "--no-default-browser-check",
        "--disable-blink-features=AutomationControlled",
        "--disable-http2",
        *proxy_chrome_args(),
        "--window-position=-2400,-2400",
        "--window-size=1366,768",
        "about:blank",
    ]
    _chrome_proc = subprocess.Popen(args, **stealth_popen_kwargs())
    _launched_procs.append(_chrome_proc)
    await asyncio.sleep(2)

    pw = await async_playwright().start()
    _pw_instance = pw
    _browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{_DEBUG_PORT}")
    logger.info("Citilink: Chrome launched on CDP port %d (pid %d)", _DEBUG_PORT, _chrome_proc.pid)
    return _browser


async def _get_context():
    global _context
    # Always create a fresh context to avoid stale F5 cookies
    # The F5 BIG-IP WAF issues session cookies that may become invalid
    if _context is not None:
        try:
            await _context.close()
        except Exception:
            pass
        _context = None
    browser = await _get_browser()
    _context = await browser.new_context(
        viewport={"width": 1366, "height": 768},
        locale="en-US",
    )
    return _context


async def _reset_profile():
    """Kill Chrome and wipe profile on Cloudflare block."""
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
    
    # Also kill any Chrome listening on our port (may have been pre-warmed by main.py)
    try:
        import subprocess as _sp
        result = _sp.run(["lsof", "-ti", f":{_DEBUG_PORT}"], capture_output=True, text=True, timeout=5)
        if result.stdout.strip():
            for pid in result.stdout.strip().split('\n'):
                if pid:
                    _sp.run(["kill", "-9", pid], timeout=5)
                    logger.info("Citilink: killed process %s on port %d", pid, _DEBUG_PORT)
    except Exception as e:
        logger.debug("Citilink: failed to kill port %d processes: %s", _DEBUG_PORT, e)
    
    if os.path.isdir(_USER_DATA_DIR):
        try:
            shutil.rmtree(_USER_DATA_DIR)
        except Exception:
            pass


class CitilinkConnectorClient:
    """Citilink — Navitaire booking via CDP Chrome."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        if req.origin not in _VALID_IATA or req.destination not in _VALID_IATA:
            return self._empty(req)

        await acquire_browser_slot()
        try:
            ob_result = await self._search_cdp(req, t0)
        finally:
            release_browser_slot()

        if req.return_from and ob_result.total_results > 0:
            ib_req = req.model_copy(update={"origin": req.destination, "destination": req.origin, "date_from": req.return_from, "return_from": None})
            if ib_req.origin in _VALID_IATA and ib_req.destination in _VALID_IATA:
                await acquire_browser_slot()
                try:
                    ib_result = await self._search_cdp(ib_req, t0)
                finally:
                    release_browser_slot()
                if ib_result.total_results > 0:
                    ob_result.offers = self._combine_rt(ob_result.offers, ib_result.offers, req)
                    ob_result.total_results = len(ob_result.offers)

        return ob_result

    async def _search_cdp(
        self, req: FlightSearchRequest, t0: float
    ) -> FlightSearchResponse:
        context = await _get_context()
        page = await context.new_page()

        search_data: dict = {}
        cf_blocked = False
        search_clicked = False

        async def _on_response(response):
            nonlocal cf_blocked
            url = response.url
            status = response.status
            ct = response.headers.get("content-type", "")

            if status == 403:
                if "challenge" in url.lower() or "cloudflare" in url.lower():
                    cf_blocked = True
                return

            if status == 200 and "json" in ct:
                lurl = url.lower()
                # Skip lowfare calendar calls (these fire during form fill, not search results)
                if "lowfare" in lurl and not search_clicked:
                    return
                    
                # Skip resource/config endpoints - these overwrite the actual search results
                skip_patterns = ["/resources/", "/i18n/", "/config", "/settings", "/assets/"]
                if any(p in lurl for p in skip_patterns):
                    return
                    
                # Only capture actual availability/search results
                if search_clicked or any(kw in lurl for kw in [
                    "avail", "search", "flight", "fare", "schedule",
                    "price", "journey", "offer",
                ]):
                    try:
                        data = await response.json()
                        if isinstance(data, (dict, list)):
                            # Don't overwrite if we already have availability data
                            if "api" in search_data and "availability" in str(search_data.get("api_url", "")).lower():
                                logger.info("Citilink: skipping API %s (already have availability data)", url[:60])
                                return
                            search_data["api"] = data
                            search_data["api_url"] = url
                            logger.info("Citilink: captured API from %s", url[:80])
                    except Exception:
                        pass

        page.on("response", _on_response)

        try:
            # Try Navitaire booking engine first (less aggressive WAF than main site)
            logger.info("Citilink: loading booking site for %s->%s", req.origin, req.destination)
            await page.goto(_BOOK_URL, wait_until="domcontentloaded", timeout=25000)
            await asyncio.sleep(5)

            title = await page.title()
            content = await page.content()
            current_url = page.url
            # Log content snippet for debugging WAF/challenge pages
            snippet = re.sub(r'\s+', ' ', content[:500]).strip()
            logger.info("Citilink: booking page — url=%s title=%r content_len=%d snippet=%r", current_url, title[:80], len(content), snippet[:300])

            # Detect blocks: explicit 403, forbidden, OR suspiciously tiny page (< 2KB)
            booking_blocked = (
                "403" in title or "forbidden" in title.lower()
                or "you have been blocked" in content.lower()
                or (len(content) < 2000 and not title)  # WAF challenge: tiny page, no title
            )

            if booking_blocked and len(content) < 2000:
                # Might be F5 BIG-IP JS challenge — wait longer for JS to execute
                logger.info("Citilink: tiny page (%d bytes), waiting for JS challenge resolution...", len(content))
                await asyncio.sleep(10)
                title = await page.title()
                content = await page.content()
                current_url = page.url
                snippet = re.sub(r'\s+', ' ', content[:500]).strip()
                logger.info("Citilink: after JS wait — url=%s title=%r content_len=%d snippet=%r", current_url, title[:80], len(content), snippet[:300])
                # Re-check block status after wait
                booking_blocked = (
                    "403" in title or "forbidden" in title.lower()
                    or "you have been blocked" in content.lower()
                    or len(content) < 2000
                )

            if booking_blocked:
                # Fallback to main website
                logger.info("Citilink: booking site blocked, trying main site")
                await page.goto(_HOME_URL, wait_until="domcontentloaded", timeout=25000)
                await asyncio.sleep(5)

                title = await page.title()
                content = await page.content()
                current_url = page.url
                snippet = re.sub(r'\s+', ' ', content[:500]).strip()
                logger.info("Citilink: main page — url=%s title=%r content_len=%d snippet=%r", current_url, title[:80], len(content), snippet[:300])
                if "403" in title or "forbidden" in title.lower() or "you have been blocked" in content.lower() or len(content) < 2000:
                    logger.warning("Citilink: WAF blocked (403 Forbidden) — title=%r url=%s", title, current_url)
                    raise RuntimeError("403 Forbidden - WAF blocked")

            # Wait for possible Cloudflare challenge
            await self._wait_for_cf(page)

            title = await page.title()
            if "just a moment" in title.lower():
                logger.warning("Citilink: stuck on Cloudflare challenge")
                await _reset_profile()
                raise RuntimeError("Cloudflare challenge blocked")

            # Dismiss popups
            await self._dismiss_popups(page)

            # Check if we got redirected away from booking page (e.g. F5 challenge → homepage)
            on_booking = "book.citilink.co.id" in page.url.lower()

            if not booking_blocked and not on_booking:
                # F5 resolved but redirected us to homepage — navigate back (cookies are set now)
                logger.info("Citilink: WAF redirected to %s, re-navigating to booking page", page.url[:80])
                await page.goto(_BOOK_URL, wait_until="domcontentloaded", timeout=25000)
                await asyncio.sleep(5)
                on_booking = "book.citilink.co.id" in page.url.lower()
                title = await page.title()
                content = await page.content()
                snippet = re.sub(r'\s+', ' ', content[:500]).strip()
                logger.info("Citilink: re-nav booking — url=%s title=%r content_len=%d snippet=%r", page.url, title[:80], len(content), snippet[:300])
                if "403" in title or "forbidden" in title.lower() or (len(content) < 2000 and not title):
                    on_booking = False

            # Fill search form based on which page we're actually on
            if on_booking:
                ok = await self._fill_navitaire_form(page, req)
            else:
                # Log DOM structure to discover correct form selectors
                try:
                    form_info = await page.evaluate("""() => {
                        const inputs = [...document.querySelectorAll('input, select, textarea')].map(el => ({
                            tag: el.tagName, id: el.id, name: el.name,
                            type: el.type, placeholder: el.placeholder,
                            cls: el.className.slice(0, 80), visible: el.offsetParent !== null
                        })).filter(e => e.visible);
                        const buttons = [...document.querySelectorAll('button, [role="button"], a.btn, input[type="submit"]')].map(el => ({
                            tag: el.tagName, id: el.id, text: el.textContent.trim().slice(0, 40),
                            cls: el.className.slice(0, 80), visible: el.offsetParent !== null
                        })).filter(e => e.visible);
                        const iframes = [...document.querySelectorAll('iframe')].map(el => ({
                            src: el.src, id: el.id, name: el.name
                        }));
                        return {inputs: inputs.slice(0, 20), buttons: buttons.slice(0, 15), iframes: iframes.slice(0, 5)};
                    }""")
                    logger.info("Citilink: DOM forms — inputs=%r", form_info.get("inputs", []))
                    logger.info("Citilink: DOM forms — buttons=%r", form_info.get("buttons", []))
                    if form_info.get("iframes"):
                        logger.info("Citilink: DOM forms — iframes=%r", form_info.get("iframes", []))
                except Exception as e:
                    logger.warning("Citilink: DOM inspect error: %s", e)

                # On homepage or other page — try homepage form
                ok = await self._fill_form(page, req)
                if not ok:
                    # Last resort: try booking page directly
                    logger.info("Citilink: homepage form failed, trying direct Navitaire URL")
                    await page.goto(_BOOK_URL, wait_until="domcontentloaded", timeout=20000)
                    await asyncio.sleep(3)

                    title = await page.title()
                    content = await page.content()
                    if "403" in title or "forbidden" in title.lower() or "you have been blocked" in content.lower():
                        logger.warning("Citilink: WAF blocked on booking site (403 Forbidden)")
                        raise RuntimeError("403 Forbidden - WAF blocked")

                    await self._wait_for_cf(page)
                    ok = await self._fill_navitaire_form(page, req)

            if ok:
                # Wait for results page to load after search click
                search_clicked = True
                remaining = max(self.timeout - (time.monotonic() - t0), 10)
                
                # The homepage is likely a SPA — don't wait long for URL change
                pre_search_url = page.url
                try:
                    await page.wait_for_url(lambda u: u != pre_search_url, timeout=3000)
                    logger.info("Citilink: URL changed to %s", page.url[:120])
                except Exception:
                    logger.info("Citilink: no URL change after search click, staying on %s", page.url[:80])
                
                # Check if a new page/popup was opened
                all_pages = context.pages
                if len(all_pages) > 1:
                    # Switch to the newest page (search results)
                    page = all_pages[-1]
                    logger.info("Citilink: new tab opened, switching to it")
                    try:
                        await page.wait_for_load_state("domcontentloaded", timeout=10000)
                    except Exception:
                        pass
                    # Re-attach response listener on new page
                    page.on("response", _on_response)
                
                # Brief wait for AJAX results to load
                await asyncio.sleep(2)
                
                # Log the URL we landed on after search
                try:
                    post_url = page.url
                    post_title = await page.title()
                    post_content_len = len(await page.content())
                    logger.info("Citilink: after search — url=%s title=%r content_len=%d pages=%d", 
                                post_url[:120], post_title[:80], post_content_len, len(all_pages))
                except Exception as e:
                    logger.warning("Citilink: error getting page info after search: %s", str(e)[:100])
                
                # Brief wait for API responses if any (SPA may use AJAX)
                deadline = time.monotonic() + min(remaining - 5, 5)  # Max 5s wait
                while time.monotonic() < deadline:
                    if search_data or cf_blocked:
                        break
                    await asyncio.sleep(0.5)

            # Parse results
            logger.info("Citilink: parsing results, search_data keys=%s, api_url=%s", 
                        list(search_data.keys()), search_data.get("api_url", "none")[:60] if search_data.get("api_url") else "none")
            offers = []
            if "api" in search_data:
                # Log the API response structure for debugging
                api_data = search_data["api"]
                logger.info("Citilink: API data type=%s", type(api_data).__name__)
                if isinstance(api_data, dict):
                    keys = list(api_data.keys())[:15]
                    logger.info("Citilink: API response keys=%s", keys)
                    # Try to find nested data containers
                    for k in ['data', 'result', 'response', 'flights', 'fares', 'lowFares', 'outbound']:
                        if k in api_data:
                            nested = api_data[k]
                            if isinstance(nested, dict):
                                logger.info("Citilink: API response.%s keys=%s", k, list(nested.keys())[:15])
                            elif isinstance(nested, list) and nested:
                                logger.info("Citilink: API response.%s is list[%d], first=%s", k, len(nested), 
                                            str(nested[0])[:200] if nested[0] else None)
                elif isinstance(api_data, list) and api_data:
                    logger.info("Citilink: API response is list[%d], first_keys=%s", len(api_data), 
                                list(api_data[0].keys())[:15] if isinstance(api_data[0], dict) else type(api_data[0]))
                offers = self._parse_api(search_data["api"], req)
            if not offers:
                html = await page.content()
                # Debug: inspect DOM for flight-related elements
                try:
                    dom_info = await page.evaluate("""() => {
                        // Look for any elements that might contain flight results
                        const keywords = ['flight', 'fare', 'price', 'journey', 'segment', 'departure', 'arrival', 'QG', 'IDR', 'Rp'];
                        const allText = document.body.innerText.slice(0, 10000);
                        const foundKeywords = keywords.filter(kw => allText.toLowerCase().includes(kw.toLowerCase()));
                        
                        // Find any visible cards/rows that look like results
                        const cards = [...document.querySelectorAll('[class*="card"], [class*="flight"], [class*="result"], [class*="fare"], [class*="journey"], .row, .list-group-item, tr')]
                            .filter(el => el.offsetParent !== null && el.innerText.length > 20)
                            .slice(0, 5)
                            .map(el => ({
                                tag: el.tagName,
                                cls: el.className.slice(0, 60),
                                text: el.innerText.replace(/\\s+/g, ' ').slice(0, 150)
                            }));
                        
                        // Check for any price-looking text
                        const priceMatches = allText.match(/(?:Rp\\.?\\s*|IDR\\s*)?\\d{1,3}([.,]\\d{3})+/g) || [];
                        
                        return {
                            keywords: foundKeywords,
                            cards: cards,
                            prices: priceMatches.slice(0, 10),
                            url: window.location.href,
                            bodyLen: document.body.innerText.length
                        };
                    }""")
                    logger.info("Citilink: DOM inspection - keywords=%s prices=%s bodyLen=%d url=%s", 
                                dom_info.get('keywords', []), dom_info.get('prices', [])[:5], 
                                dom_info.get('bodyLen', 0), dom_info.get('url', '')[:80])
                    if dom_info.get('cards'):
                        for card in dom_info['cards'][:3]:
                            logger.info("Citilink: DOM card - tag=%s cls=%s text=%s", 
                                        card.get('tag'), card.get('cls'), card.get('text', '')[:100])
                except Exception as e:
                    logger.warning("Citilink: DOM inspection error: %s", e)
                offers = self._parse_html(html, req)

            offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
            elapsed = time.monotonic() - t0
            logger.info("Citilink %s->%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

            h = hashlib.md5(
                f"citilink{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
            ).hexdigest()[:12]
            return FlightSearchResponse(
                search_id=f"fs_{h}",
                origin=req.origin,
                destination=req.destination,
                currency="IDR",
                offers=offers,
                total_results=len(offers),
            )

        except RuntimeError:
            raise  # Let block-detection errors propagate for retry logic
        except Exception as e:
            logger.error("Citilink CDP error: %s", e)
            # Reset profile on proxy/tunnel errors to get fresh connection
            err_str = str(e).lower()
            if "tunnel" in err_str or "proxy" in err_str or "connection" in err_str:
                logger.info("Citilink: resetting profile due to connection error")
                await _reset_profile()
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    async def _wait_for_cf(self, page, max_wait: float = 15.0):
        """Wait for Cloudflare challenge to pass."""
        deadline = time.monotonic() + max_wait
        while time.monotonic() < deadline:
            title = await page.title()
            if "just a moment" not in title.lower() and "challenge" not in title.lower():
                return
            await asyncio.sleep(1)

    async def _dismiss_popups(self, page):
        for label in ["Accept", "Accept All", "I agree", "OK", "Got it", "Close"]:
            try:
                btn = page.get_by_role("button", name=label)
                if await btn.count() > 0:
                    await btn.first.click(timeout=2000)
                    await asyncio.sleep(0.3)
                    break
            except Exception:
                continue

    async def _fill_form(self, page, req: FlightSearchRequest) -> bool:
        """Fill Citilink homepage search form (www.citilink.co.id)."""
        try:
            # Citilink homepage form uses these IDs (discovered via DOM inspection):
            # - #airportfrom (origin), #airportto (dest), #departuredate, #searchButton
            # - #customSwitch is round-trip toggle (checkbox, unchecked = one-way)

            # One-way: check if customSwitch is checked (round-trip) and uncheck it
            try:
                switch = page.locator('#customSwitch')
                if await switch.count() > 0:
                    is_checked = await switch.is_checked()
                    if is_checked:
                        # Click label to uncheck (toggle to one-way)
                        await page.locator('label[for="customSwitch"]').click(timeout=2000)
                        await asyncio.sleep(0.3)
            except Exception:
                pass

            await asyncio.sleep(0.5)

            # Origin field (#airportfrom)
            origin_ok = await self._fill_city(
                page, req.origin,
                ['#airportfrom', 'input[name="airportfrom"]', 'input[placeholder="Dari"]'],
                "origin",
            )
            if not origin_ok:
                logger.warning("Citilink: origin field not found")
                return False

            await asyncio.sleep(0.5)

            # Destination field (#airportto)
            dest_ok = await self._fill_city(
                page, req.destination,
                ['#airportto', 'input[name="airportto"]', 'input[placeholder="Ke"]'],
                "destination",
            )
            if not dest_ok:
                logger.warning("Citilink: destination field not found")
                return False

            await asyncio.sleep(0.5)

            # Date field (#departuredate) — uses jQuery datepicker
            # Also need to fill hidden field #startdate
            dep_date = datetime.strptime(str(req.date_from), "%Y-%m-%d")
            date_filled = False
            
            # Set the hidden startdate field (format: YYYY-MM-DD or DD/MM/YYYY)
            date_str_dmy = dep_date.strftime("%d/%m/%Y")
            date_str_ymd = dep_date.strftime("%Y-%m-%d")
            try:
                await page.evaluate(f"""() => {{
                    // Set hidden startdate (used by form submission)
                    const startdate = document.querySelector('#startdate');
                    if (startdate) {{
                        startdate.value = '{date_str_ymd}';
                    }}
                    // Set visible departuredate
                    const departuredate = document.querySelector('#departuredate');
                    if (departuredate) {{
                        departuredate.value = '{date_str_dmy}';
                        departuredate.dispatchEvent(new Event('change', {{ bubbles: true }}));
                    }}
                    // For one-way, also set enddate to empty string
                    const enddate = document.querySelector('#enddate');
                    if (enddate) {{
                        enddate.value = '';
                    }}
                    // Set arrivaldate to empty (one-way)
                    const arrivaldate = document.querySelector('#arrivaldate');
                    if (arrivaldate) {{
                        arrivaldate.value = '';
                        // Remove required attribute for one-way
                        arrivaldate.removeAttribute('required');
                    }}
                }}""")
                logger.info("Citilink: set date via JavaScript: %s (startdate=%s)", date_str_dmy, date_str_ymd)
                date_filled = True
            except Exception as e:
                logger.warning("Citilink: date fill error: %s", e)
            
            # Set passenger field (required)
            try:
                await page.evaluate("""() => {
                    const passenger = document.querySelector('#passenger');
                    if (passenger) {
                        passenger.value = '1 Dewasa, 0 Anak-anak, 0 Bayi';
                    }
                }""")
            except Exception:
                pass
            
            await asyncio.sleep(0.5)
            
            for selector in ['#departuredate', 'input[name="departuredate"]']:
                try:
                    el = page.locator(selector).first
                    if await el.count() > 0:
                        # Just click to dismiss any overlay, date already set via JS
                        await el.click(timeout=2000)
                        await asyncio.sleep(0.3)
                        # Close datepicker if it opened
                        await page.keyboard.press("Escape")
                        await asyncio.sleep(0.2)
                        break
                except Exception:
                    continue

            # Verify form values before submitting
            try:
                form_vals = await page.evaluate("""() => {
                    const origin = document.querySelector('#airportfrom');
                    const dest = document.querySelector('#airportto');
                    const date = document.querySelector('#departuredate');
                    const btn = document.querySelector('#searchButton');
                    // Check form action/method and button onclick
                    const form = btn ? btn.closest('form') : null;
                    
                    // Get ALL form inputs including hidden ones
                    const allInputs = form ? [...form.querySelectorAll('input, select, textarea')].map(el => ({
                        name: el.name, id: el.id, type: el.type, value: el.value.slice(0, 50),
                        hidden: el.type === 'hidden', required: el.required
                    })).filter(e => e.name || e.id) : [];
                    
                    // Check form validity
                    const isValid = form ? form.checkValidity() : null;
                    const invalidFields = form ? [...form.querySelectorAll(':invalid')].map(e => e.name || e.id) : [];
                    
                    return {
                        origin: origin ? origin.value : null,
                        dest: dest ? dest.value : null,
                        date: date ? date.value : null,
                        formAction: form ? form.action : null,
                        formMethod: form ? form.method : null,
                        formTarget: form ? form.target : null,
                        btnOnclick: btn ? btn.getAttribute('onclick') : null,
                        btnType: btn ? btn.type : null,
                        btnTag: btn ? btn.tagName : null,
                        allInputs: allInputs,
                        isValid: isValid,
                        invalidFields: invalidFields,
                    };
                }""")
                logger.info("Citilink: form values before search: origin=%r dest=%r date=%r", 
                            form_vals.get("origin"), form_vals.get("dest"), form_vals.get("date"))
                logger.info("Citilink: form action=%r method=%r target=%r btn_onclick=%r btn_type=%r btn_tag=%r",
                            form_vals.get("formAction"), form_vals.get("formMethod"), 
                            form_vals.get("formTarget"), form_vals.get("btnOnclick"),
                            form_vals.get("btnType"), form_vals.get("btnTag"))
                logger.info("Citilink: form isValid=%s invalidFields=%s", 
                            form_vals.get("isValid"), form_vals.get("invalidFields"))
                logger.info("Citilink: form all inputs=%s", form_vals.get("allInputs", []))
            except Exception as e:
                logger.warning("Citilink: form verify error: %s", e)

            # Listen for popups before clicking search
            popup_page = None
            def _on_popup(p):
                nonlocal popup_page
                popup_page = p
            page.context.on("page", _on_popup)

            # Click search button (#searchButton, text "Cari Penerbangan")
            pre_url = page.url
            logger.info("Citilink: attempting search button click, current URL=%s", pre_url[:80])
            
            # Strategy 1: Try clicking #searchButton with short timeout
            search_btn = page.locator('#searchButton').first
            if await search_btn.count() > 0:
                logger.info("Citilink: found #searchButton, clicking...")
                try:
                    # Use short timeout (5s) — SPA likely won't navigate
                    async with page.expect_navigation(timeout=5000, wait_until="domcontentloaded"):
                        await search_btn.click(timeout=3000)
                    logger.info("Citilink: clicked search, navigated to %s", page.url[:80])
                    return True
                except Exception as e:
                    logger.warning("Citilink: button click didn't navigate: %s", str(e)[:60])
                    # Check if we actually navigated despite the exception
                    if page.url != pre_url:
                        logger.info("Citilink: navigation happened anyway, now at %s", page.url[:80])
                        return True
            
            # Strategy 2: Try triggering click event via JavaScript (dispatches mouse events)
            logger.info("Citilink: trying JavaScript click dispatch...")
            try:
                click_result = await page.evaluate("""() => {
                    const btn = document.querySelector('#searchButton');
                    if (!btn) return {clicked: false, error: 'Button not found'};
                    
                    // Check if form has validation that needs to pass
                    const form = btn.closest('form');
                    if (form && form.checkValidity && !form.checkValidity()) {
                        return {clicked: false, error: 'Form validation failed', invalid: [...form.querySelectorAll(':invalid')].map(e => e.name || e.id)};
                    }
                    
                    // Dispatch a proper click event
                    const evt = new MouseEvent('click', {bubbles: true, cancelable: true, view: window});
                    btn.dispatchEvent(evt);
                    return {clicked: true, formAction: form ? form.action : null};
                }""")
                logger.info("Citilink: JS click dispatch result: %s", click_result)
                
                if click_result.get("clicked"):
                    # Brief wait to see if click triggers navigation
                    await asyncio.sleep(2)
                    
                    # Check if page changed
                    flight_check = await page.evaluate("""() => {
                        const body = document.body.innerText;
                        return { bodyLen: body.length, url: window.location.href };
                    }""")
                    
                    if flight_check.get('bodyLen', 0) < 5000 and page.url == pre_url:
                        # Click didn't trigger anything - body still tiny, URL same
                        # Fall through to try form.submit()
                        logger.info("Citilink: click dispatch didn't trigger navigation (bodyLen=%d), trying form.submit()", 
                                    flight_check.get('bodyLen', 0))
                    else:
                        # Something changed - poll for results
                        logger.info("Citilink: click triggered change (bodyLen=%d), polling for results", flight_check.get('bodyLen', 0))
                        for wait_attempt in range(6):  # Max 30 seconds (6 * 5s)
                            await asyncio.sleep(5)
                            
                            flight_check = await page.evaluate("""() => {
                                const body = document.body.innerText;
                                const hasPrice = /(?:Rp\\.?\\s*|IDR\\s*)\\d{1,3}([.,]\\d{3})+/.test(body);
                                const hasQG = /QG\\s*\\d+/.test(body);
                                const prices = body.match(/(?:Rp\\.?\\s*|IDR\\s*)?\\d{1,3}(?:[.,]\\d{3})+/g) || [];
                                const flightPrices = prices.filter(p => {
                                    const num = parseInt(p.replace(/[^\\d]/g, ''));
                                    return num >= 200000 && num <= 10000000;
                                });
                                return { hasPrice, hasQG, priceCount: flightPrices.length, bodyLen: body.length };
                            }""")
                            
                            logger.info("Citilink: poll %d - hasPrice=%s priceCount=%d bodyLen=%d",
                                        wait_attempt + 1, flight_check.get('hasPrice'), 
                                        flight_check.get('priceCount', 0), flight_check.get('bodyLen', 0))
                            
                            if flight_check.get('hasPrice') or flight_check.get('priceCount', 0) >= 2:
                                logger.info("Citilink: found flight content after %d polls", wait_attempt + 1)
                                break
                            if wait_attempt >= 2 and flight_check.get('bodyLen', 0) > 10000:
                                break
                        
                        post_url = page.url
                        logger.info("Citilink: after JS click dispatch, URL=%s", post_url[:80])
                        return True
            except Exception as e:
                err_msg = str(e)
                # "Execution context was destroyed" means page navigated - this is SUCCESS!
                if "context was destroyed" in err_msg.lower() or "navigation" in err_msg.lower():
                    logger.info("Citilink: navigation detected via context destruction, waiting for new page")
                    await asyncio.sleep(3)
                    try:
                        await page.wait_for_load_state("domcontentloaded", timeout=15000)
                    except Exception:
                        pass
                    
                    # Poll for the Angular SPA to load and render content
                    post_url = page.url
                    logger.info("Citilink: after navigation, URL=%s", post_url[:80])
                    
                    # The Navitaire Angular app at book2.citilink.co.id needs time to load
                    if "book2.citilink.co.id" in post_url or "book.citilink.co.id" in post_url:
                        logger.info("Citilink: on booking SPA, polling for content to load...")
                        for poll in range(12):  # Max 60 seconds (12 * 5s)
                            await asyncio.sleep(5)
                            try:
                                spa_state = await page.evaluate("""() => {
                                    const body = document.body ? document.body.innerText : '';
                                    const bodyLen = body.length;
                                    
                                    // Look for flight-related content
                                    const hasPrice = /(?:Rp\\.?\\s*|IDR\\s*)\\d{1,3}([.,]\\d{3})+/.test(body);
                                    const hasQG = /QG\\s*\\d+/.test(body);
                                    const hasFlight = /flight|penerbangan|departure|arrival|berangkat|tiba/i.test(body);
                                    const hasTime = /\\d{2}:\\d{2}/.test(body);
                                    
                                    // Count price-like patterns
                                    const prices = body.match(/(?:Rp\\.?\\s*|IDR\\s*)?\\d{1,3}(?:[.,]\\d{3})+/g) || [];
                                    const flightPrices = prices.filter(p => {
                                        const num = parseInt(p.replace(/[^\\d]/g, ''));
                                        return num >= 200000 && num <= 10000000;
                                    });
                                    
                                    // Check for Angular loading indicators
                                    const loading = document.querySelector('.loading, .spinner, [class*="loading"], [class*="spinner"], app-loading');
                                    const isLoading = loading && loading.offsetParent !== null;
                                    
                                    // Check for flight cards/rows
                                    const flightCards = document.querySelectorAll('[class*="flight"], [class*="journey"], [class*="fare"], [class*="segment"], [class*="itinerary"]');
                                    
                                    return {
                                        bodyLen, hasPrice, hasQG, hasFlight, hasTime,
                                        priceCount: flightPrices.length,
                                        isLoading,
                                        cardCount: flightCards.length,
                                        url: window.location.href
                                    };
                                }""")
                                
                                logger.info("Citilink: SPA poll %d - bodyLen=%d hasPrice=%s priceCount=%d cardCount=%d isLoading=%s",
                                            poll + 1, spa_state.get('bodyLen', 0), spa_state.get('hasPrice'),
                                            spa_state.get('priceCount', 0), spa_state.get('cardCount', 0),
                                            spa_state.get('isLoading'))
                                
                                # Success conditions: found flight content
                                if spa_state.get('hasPrice') and spa_state.get('priceCount', 0) >= 2:
                                    logger.info("Citilink: SPA loaded with flight prices after %d polls", poll + 1)
                                    break
                                if spa_state.get('cardCount', 0) >= 3 and spa_state.get('bodyLen', 0) > 5000:
                                    logger.info("Citilink: SPA loaded with flight cards after %d polls", poll + 1)
                                    break
                                if spa_state.get('bodyLen', 0) > 20000 and spa_state.get('hasFlight'):
                                    logger.info("Citilink: SPA loaded with flight content after %d polls", poll + 1)
                                    break
                                    
                                # If loading indicator is gone and we have some content, stop
                                if not spa_state.get('isLoading') and spa_state.get('bodyLen', 0) > 5000 and poll >= 3:
                                    logger.info("Citilink: SPA finished loading after %d polls", poll + 1)
                                    break
                                    
                            except Exception as poll_err:
                                logger.warning("Citilink: SPA poll error: %s", str(poll_err)[:80])
                                # Context destroyed again means another navigation - wait
                                if "context" in str(poll_err).lower():
                                    await asyncio.sleep(3)
                                    try:
                                        await page.wait_for_load_state("domcontentloaded", timeout=10000)
                                    except Exception:
                                        pass
                    
                    return True
                logger.warning("Citilink: JS click dispatch error: %s", err_msg[:100])
            
            # Strategy 3: Try submitting the form via JavaScript (bypasses click handlers)
            logger.info("Citilink: trying JavaScript form submission...")
            try:
                nav_result = await page.evaluate("""() => {
                    // Find the search form (contains #searchButton)
                    const btn = document.querySelector('#searchButton');
                    const form = btn ? btn.closest('form') : document.querySelector('form[action*="flightsearch"]');
                    if (form) {
                        // Check for any visible validation errors before submitting
                        const validationErr = document.querySelector('.alert-danger, .error-message, .is-invalid');
                        if (validationErr && validationErr.offsetParent !== null) {
                            return {submitted: false, error: 'Validation error: ' + validationErr.textContent.slice(0, 100)};
                        }
                        
                        // First, dispatch submit event (this triggers JS handlers that may do AJAX)
                        const submitEvent = new Event('submit', {bubbles: true, cancelable: true});
                        const eventResult = form.dispatchEvent(submitEvent);
                        
                        // If event wasn't cancelled, try requestSubmit
                        if (eventResult) {
                            if (form.requestSubmit) {
                                form.requestSubmit(btn);
                                return {submitted: true, method: 'requestSubmit', action: form.action, eventCancelled: false};
                            }
                            form.submit();
                            return {submitted: true, method: 'submit', action: form.action, eventCancelled: false};
                        } else {
                            // Event was cancelled - JS handler took over
                            return {submitted: true, method: 'eventDispatch', action: form.action, eventCancelled: true};
                        }
                    }
                    return {submitted: false, error: 'Form not found'};
                }""")
                logger.info("Citilink: JS form submit result: %s", nav_result)
                
                if nav_result.get("submitted"):
                    # Wait for page to load after form submission
                    await asyncio.sleep(3)
                    
                    # Check for any validation errors that appeared
                    errors = await page.evaluate("""() => {
                        const errs = [...document.querySelectorAll('.alert-danger, .error-message, .is-invalid, .text-danger, [class*="error"]')]
                            .filter(el => el.offsetParent !== null && el.innerText.trim())
                            .map(el => el.innerText.trim().slice(0, 100));
                        return errs;
                    }""")
                    if errors:
                        logger.warning("Citilink: validation errors after submit: %s", errors)
                    
                    try:
                        await page.wait_for_load_state("domcontentloaded", timeout=15000)
                    except Exception:
                        pass
                    
                    # Check what happened
                    post_url = page.url
                    post_body_len = len(await page.content())
                    logger.info("Citilink: after JS submit, URL=%s bodyLen=%d", post_url[:80], post_body_len)
                    
                    # If we navigated to flightsearch, wait for results to render
                    if "flightsearch" in post_url.lower() or post_url != pre_url:
                        # Wait for results to load
                        try:
                            await page.wait_for_load_state("networkidle", timeout=15000)
                        except Exception:
                            pass
                        await asyncio.sleep(2)
                        return True
                    
                    # URL didn't change but form might have submitted - check body size
                    if post_body_len > 50000:
                        return True
                    
                    # Form might be validating or something - wait a bit
                    for _ in range(3):
                        await asyncio.sleep(2)
                        post_body_len = len(await page.content())
                        if post_body_len > 50000 or page.url != pre_url:
                            return True
                    
                elif nav_result.get("error"):
                    logger.warning("Citilink: JS submit failed: %s", nav_result.get("error"))
            except Exception as e:
                logger.warning("Citilink: JS form submit error: %s", str(e)[:100])
            
            # Strategy 3: Fallback button click without expect_navigation
            for selector in ['button:has-text("Cari Penerbangan")', 'button[type="submit"]']:
                try:
                    el = page.locator(selector).first
                    if await el.count() > 0:
                        logger.info("Citilink: fallback click on %s", selector)
                        await el.click(timeout=5000)
                        await asyncio.sleep(3)
                        if page.url != pre_url:
                            logger.info("Citilink: navigated to %s", page.url[:80])
                        return True
                except Exception as e:
                    logger.debug("Citilink: fallback %s failed: %s", selector, e)
            
            logger.warning("Citilink: all search button strategies failed")

            return False

        except Exception as e:
            logger.error("Citilink form fill error: %s", e)
            return False

    async def _fill_navitaire_form(self, page, req: FlightSearchRequest) -> bool:
        """Fill Navitaire booking form at book.citilink.co.id."""
        try:
            # Navitaire has specific field patterns
            # Origin station
            for selector in [
                '#TextBoxMarketOrigin1', '#TextBoxMarketOrigin0',
                'input[id*="Origin"]', 'input[name*="origin"]',
            ]:
                try:
                    el = page.locator(selector).first
                    if await el.count() > 0:
                        await el.click(timeout=2000)
                        await el.fill(req.origin)
                        await asyncio.sleep(1)
                        # Navitaire autocomplete
                        try:
                            opt = page.locator(f'li:has-text("{req.origin}")').first
                            if await opt.count() > 0:
                                await opt.click(timeout=2000)
                        except Exception:
                            await el.press("Tab")
                        break
                except Exception:
                    continue

            # Destination station
            for selector in [
                '#TextBoxMarketDestination1', '#TextBoxMarketDestination0',
                'input[id*="Destination"]', 'input[name*="destination"]',
            ]:
                try:
                    el = page.locator(selector).first
                    if await el.count() > 0:
                        await el.click(timeout=2000)
                        await el.fill(req.destination)
                        await asyncio.sleep(1)
                        try:
                            opt = page.locator(f'li:has-text("{req.destination}")').first
                            if await opt.count() > 0:
                                await opt.click(timeout=2000)
                        except Exception:
                            await el.press("Tab")
                        break
                except Exception:
                    continue

            # Date
            dep_date = datetime.strptime(str(req.date_from), "%Y-%m-%d")
            for selector in ['#TextBoxMarketDepartDate1', 'input[id*="DepartDate"]']:
                try:
                    el = page.locator(selector).first
                    if await el.count() > 0:
                        await el.fill(dep_date.strftime("%d/%m/%Y"))
                        break
                except Exception:
                    continue

            # One-way radio
            try:
                await page.locator('#RadioButtonMarketStructureOneWay, input[value="OneWay"]').first.click(timeout=2000)
            except Exception:
                pass

            # Search button
            for selector in [
                '#buttonSubmit', '#ControlGroupSearchView_ButtonSubmit',
                'button:has-text("Search")', 'input[type="submit"]',
            ]:
                try:
                    el = page.locator(selector).first
                    if await el.count() > 0:
                        await el.click(timeout=3000)
                        logger.info("Citilink: clicked Navitaire search")
                        return True
                except Exception:
                    continue

            return False

        except Exception as e:
            logger.error("Citilink Navitaire form error: %s", e)
            return False

    async def _fill_city(
        self, page, iata: str, selectors: list[str], label: str
    ) -> bool:
        for selector in selectors:
            try:
                el = page.locator(selector).first
                if await el.count() > 0:
                    await el.click(timeout=2000)
                    await asyncio.sleep(0.3)
                    # Clear first, then type
                    await el.fill("")
                    await asyncio.sleep(0.2)
                    await el.fill(iata)
                    await asyncio.sleep(1.5)  # Wait longer for autocomplete
                    
                    # Autocomplete - check for jQuery UI autocomplete or custom dropdown
                    for opt_sel in [
                        f'.ui-autocomplete li:has-text("{iata}")',
                        f'.ui-menu-item:has-text("{iata}")',
                        f'li:has-text("{iata}")',
                        f'[data-code="{iata}"]',
                        f'.autocomplete-item:has-text("{iata}")',
                        f'ul.ui-autocomplete li a:has-text("{iata}")',
                    ]:
                        try:
                            opt = page.locator(opt_sel).first
                            if await opt.count() > 0:
                                await opt.click(timeout=2000)
                                logger.info("Citilink: filled %s = %s (autocomplete)", label, iata)
                                await asyncio.sleep(0.5)
                                return True
                        except Exception:
                            continue
                    
                    # No autocomplete clicked - try pressing down arrow to select first item then Enter
                    try:
                        await el.press("ArrowDown")
                        await asyncio.sleep(0.2)
                        await el.press("Enter")
                        logger.info("Citilink: filled %s = %s (arrow+enter)", label, iata)
                        await asyncio.sleep(0.5)
                        return True
                    except Exception:
                        pass
                    
                    # Last resort: just press Enter
                    await el.press("Enter")
                    logger.info("Citilink: filled %s = %s (enter only)", label, iata)
                    return True
            except Exception as e:
                logger.warning("Citilink: %s field fill error with selector %s: %s", label, selector, e)
                continue
        return False

    def _parse_api(self, data, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse Navitaire DotRez API JSON.
        
        DotRez structure (NSK v1):
        {
          "data": {
            "results": [
              {
                "journeyKey": "...",
                "segments": [{
                  "identifier": {"carrierCode": "QG", "flightNumber": "123"},
                  "designator": {"origin": "CGK", "destination": "DPS", 
                                 "departure": "2026-05-15T08:00:00", "arrival": "2026-05-15T10:00:00"}
                }],
                "fareAvailabilityKey": "fare_key_abc",
                "fares": [{"fareAvailabilityKey": "..."}]
              }
            ],
            "faresAvailable": {
              "fare_key_abc": {
                "totals": {"passengerTotals": {"ADT": {"fareTotal": 1234567}}},
                "passengerFares": [{"fareAmount": 500000}],
                "fares": [{"passengerFares": [...]}]
              }
            }
          }
        }
        """
        offers = []
        dep_date = datetime.strptime(str(req.date_from), "%Y-%m-%d")

        if not isinstance(data, dict):
            return []
        
        # Get the inner data container
        inner = data.get("data", data)
        if not isinstance(inner, dict):
            return []
            
        # Get faresAvailable lookup (prices are here)
        fares_available = inner.get("faresAvailable", {})
        if fares_available:
            logger.info("Citilink: faresAvailable has %d entries, first keys: %s", 
                        len(fares_available), list(fares_available.keys())[:3])
            # Log structure of first fare entry for debugging
            for fk, fv in list(fares_available.items())[:1]:
                if isinstance(fv, dict):
                    logger.info("Citilink: faresAvailable[%s] keys=%s", fk[:30], list(fv.keys())[:10])
                    # Log totals structure
                    if fv.get("totals"):
                        logger.info("Citilink: faresAvailable.totals=%s", str(fv["totals"])[:200])
                    if fv.get("fares"):
                        logger.info("Citilink: faresAvailable.fares[0]=%s", str(fv["fares"][0])[:300] if fv["fares"] else "empty")
        
        # Debug: log what data.results actually is
        raw_results = inner.get("results")
        logger.info("Citilink: data.results type=%s len=%s", 
                    type(raw_results).__name__, 
                    len(raw_results) if isinstance(raw_results, (list, dict)) else "N/A")
        if isinstance(raw_results, dict):
            logger.info("Citilink: data.results (dict) keys=%s", list(raw_results.keys())[:15])
        elif isinstance(raw_results, list) and raw_results:
            logger.info("Citilink: data.results[0] type=%s", type(raw_results[0]).__name__)
            if isinstance(raw_results[0], dict):
                logger.info("Citilink: data.results[0] keys=%s", list(raw_results[0].keys())[:15])
        
        # Get results - could be list or dict
        results = []
        if isinstance(raw_results, list):
            results = raw_results
        elif isinstance(raw_results, dict):
            # Results might be keyed by journey key
            results = list(raw_results.values()) if raw_results else []
        
        # Fallback to other locations
        if not results:
            results = (
                inner.get("trips") or
                inner.get("journeys") or
                (inner.get("availability", {}).get("trips") if isinstance(inner.get("availability"), dict) else None) or
                []
            )
        
        logger.info("Citilink: _parse_api processing %d results", len(results))
        
        # If no results but we have faresAvailable, build offers from faresAvailable directly
        if not results and fares_available:
            logger.info("Citilink: No results array, building offers from faresAvailable")
            for fare_key, fare_data in fares_available.items():
                if not isinstance(fare_data, dict):
                    continue
                
                # Extract price from totals or fares
                price = self._extract_price_from_fare_available(fare_data)
                if not price or price <= 0:
                    continue
                
                # Build a basic offer without segment details
                _qg_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
                segment = FlightSegment(
                    airline="QG",
                    airline_name="Citilink",
                    flight_no="",
                    origin=req.origin,
                    destination=req.destination,
                    departure=dep_date,
                    arrival=dep_date,
                    duration_seconds=0,
                    cabin_class=_qg_cabin,
                )
                route = FlightRoute(segments=[segment], total_duration_seconds=0, stopovers=0)
                fid = hashlib.md5(f"qg_{fare_key}_{price}_{req.date_from}".encode()).hexdigest()[:12]
                offers.append(FlightOffer(
                    id=f"qg_{fid}",
                    price=round(price, 2),
                    currency="IDR",
                    price_formatted=f"IDR {price:,.0f}",
                    outbound=route,
                    inbound=None,
                    airlines=["Citilink"],
                    owner_airline="QG",
                    booking_url=_BOOK_URL,
                    is_locked=False,
                    source="citilink_direct",
                    source_tier="free",
                ))
            return offers
        
        # Log structure of first result for debugging
        if results and isinstance(results[0], dict):
            r0 = results[0]
            logger.info("Citilink: results[0] keys=%s", list(r0.keys())[:15])
            
            # Handle case where results[0] contains 'trips' array (DotRez NSK v1 structure)
            if r0.get("trips") and isinstance(r0["trips"], list):
                trips = r0["trips"]
                logger.info("Citilink: found results[0].trips with %d items", len(trips))
                if trips:
                    t0 = trips[0]
                    if isinstance(t0, dict):
                        logger.info("Citilink: trips[0] keys=%s", list(t0.keys())[:20])
                        # Log journey details - check both possible keys
                        for ja_key in ["journeysAvailable", "journeysAvailableByMarket"]:
                            if t0.get(ja_key):
                                ja = t0[ja_key]
                                logger.info("Citilink: trips[0].%s type=%s len=%s", ja_key,
                                            type(ja).__name__, len(ja) if isinstance(ja, (list, dict)) else "N/A")
                                if isinstance(ja, list) and ja:
                                    logger.info("Citilink: %s[0] keys=%s", ja_key, list(ja[0].keys())[:15] if isinstance(ja[0], dict) else "not dict")
                                    if isinstance(ja[0], dict) and ja[0].get("journeys"):
                                        jlist = ja[0]["journeys"]
                                        logger.info("Citilink: %s[0].journeys type=%s len=%s", ja_key, type(jlist).__name__, len(jlist) if isinstance(jlist, (list, dict)) else "N/A")
                                        if isinstance(jlist, dict):
                                            first_jkey = list(jlist.keys())[0] if jlist else None
                                            if first_jkey and isinstance(jlist[first_jkey], dict):
                                                jv = jlist[first_jkey]
                                                logger.info("Citilink: journey[%s] keys=%s", first_jkey[:30], list(jv.keys())[:15])
                                                if jv.get("segments"):
                                                    logger.info("Citilink: journey.segments[0]=%s", str(jv["segments"][0])[:400] if jv["segments"] else "empty")
                                elif isinstance(ja, dict):
                                    first_key = list(ja.keys())[0] if ja else None
                                    if first_key and isinstance(ja[first_key], dict):
                                        logger.info("Citilink: %s[%s] keys=%s", ja_key, first_key[:30], list(ja[first_key].keys())[:15])
                                        jav = ja[first_key]
                                        if jav.get("segments"):
                                            logger.info("Citilink: journey.segments[0]=%s", str(jav["segments"][0])[:400] if jav["segments"] else "empty")
                                break  # Found journey data, stop looking
                        # Also check for direct segments/journeys at trip level
                        if t0.get("segments"):
                            logger.info("Citilink: trips[0].segments[0]=%s", str(t0["segments"][0])[:300] if t0["segments"] else "empty")
                # Use trips as our results for processing
                results = []
                for trip in trips:
                    if not isinstance(trip, dict):
                        continue
                    # Extract journeys from journeysAvailable or journeysAvailableByMarket
                    ja = trip.get("journeysAvailable") or trip.get("journeysAvailableByMarket")
                    if not ja:
                        continue
                    if isinstance(ja, dict):
                        # journeysAvailableByMarket is a dict keyed by market (e.g. "CGK~DPS")
                        for market_key, market_val in ja.items():
                            # Log the structure for debugging
                            logger.info("Citilink: market[%s] type=%s", market_key[:20], type(market_val).__name__)
                            
                            if isinstance(market_val, list):
                                # market_val is a LIST of journey objects
                                logger.info("Citilink: market[%s] has %d items", market_key[:20], len(market_val))
                                if market_val:
                                    mv0 = market_val[0]
                                    if isinstance(mv0, dict):
                                        logger.info("Citilink: market[0] keys=%s", list(mv0.keys())[:15])
                                        if mv0.get("segments"):
                                            logger.info("Citilink: market[0].segments[0]=%s", str(mv0["segments"][0])[:350] if mv0["segments"] else "empty")
                                results.extend([j for j in market_val if isinstance(j, dict)])
                            elif isinstance(market_val, dict):
                                logger.info("Citilink: market[%s] keys=%s", market_key[:20], list(market_val.keys())[:15])
                                
                                # Look for journeys inside the market dict
                                journeys = market_val.get("journeys", {})
                                if isinstance(journeys, dict) and journeys:
                                    logger.info("Citilink: market.journeys has %d entries", len(journeys))
                                    for jkey, journey in journeys.items():
                                        if isinstance(journey, dict):
                                            results.append(journey)
                                elif isinstance(journeys, list) and journeys:
                                    results.extend([j for j in journeys if isinstance(j, dict)])
                                
                                # Fallback: maybe market_val itself IS a journey (has segments directly)
                                if market_val.get("segments") and not journeys:
                                    results.append(market_val)
                    elif isinstance(ja, list):
                        # journeysAvailable could be a list of market objects
                        for market in ja:
                            if isinstance(market, dict):
                                journeys = market.get("journeys", {})
                                if isinstance(journeys, dict):
                                    for jkey, journey in journeys.items():
                                        if isinstance(journey, dict):
                                            results.append(journey)
                                elif isinstance(journeys, list):
                                    results.extend([j for j in journeys if isinstance(j, dict)])
                                if market.get("segments"):
                                    results.append(market)
                    # Also check for direct segments at trip level
                    if trip.get("segments"):
                        results.append(trip)
                logger.info("Citilink: extracted %d journey results from trips", len(results))
            elif r0.get("segments"):
                seg0 = r0["segments"][0] if r0["segments"] else {}
                if isinstance(seg0, dict):
                    logger.info("Citilink: results[0].segments[0] keys=%s", list(seg0.keys())[:15])
                    if seg0.get("identifier"):
                        logger.info("Citilink: segment.identifier=%s", seg0["identifier"])
                    if seg0.get("designator"):
                        logger.info("Citilink: segment.designator=%s", seg0["designator"])
            if r0.get("fares"):
                fare0 = r0["fares"][0] if r0["fares"] else {}
                if isinstance(fare0, dict):
                    logger.info("Citilink: results[0].fares[0] keys=%s", list(fare0.keys())[:15])

        for i, result in enumerate(results[:3]):  # Log first 3 only
            if not isinstance(result, dict):
                continue
            logger.info("Citilink: result[%d] segments=%s", i, str(result.get("segments", []))[:300] if result.get("segments") else "NONE")

        for result in results:
            if not isinstance(result, dict):
                continue

            # Extract segment info (flight details)
            segments = result.get("segments", [])
            seg_info = self._extract_segment_info(segments, dep_date)
            
            # Get fares for this result
            result_fares = result.get("fares", [])
            if not result_fares:
                # Try to use fareAvailabilityKey to lookup
                fare_key = result.get("fareAvailabilityKey")
                if fare_key and fare_key in fares_available:
                    result_fares = [fares_available[fare_key]]
            
            # If still no fares, try the result itself as a fare
            if not result_fares:
                result_fares = [result]
            
            for fare in result_fares:
                if not isinstance(fare, dict):
                    continue
                
                # Log fare structure for debugging
                fare_keys = list(fare.keys())[:10]
                fare_key = fare.get("fareAvailabilityKey") or fare.get("fareKey")
                logger.info("Citilink: fare keys=%s fareAvailabilityKey=%s", fare_keys, fare_key[:30] if fare_key else None)
                
                # Get price from fare
                price = self._extract_price(fare, fares_available)
                logger.info("Citilink: extracted price=%s from fare", price)
                if not price or price <= 0:
                    continue
                
                # Use extracted segment info or defaults
                flight_no = seg_info.get("flight_no", "")
                logger.info("Citilink: offer creation - seg_info=%s flight_no=%s", {k: str(v)[:30] for k, v in seg_info.items()}, flight_no)
                dep_dt = seg_info.get("departure", dep_date)
                arr_dt = seg_info.get("arrival", dep_date)
                origin = seg_info.get("origin", req.origin)
                destination = seg_info.get("destination", req.destination)
                
                if arr_dt < dep_dt:
                    arr_dt += timedelta(days=1)
                dur = int((arr_dt - dep_dt).total_seconds()) if arr_dt > dep_dt else 0

                _qg_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
                segment = FlightSegment(
                    airline="QG",
                    airline_name="Citilink",
                    flight_no=str(flight_no),
                    origin=origin,
                    destination=destination,
                    departure=dep_dt,
                    arrival=arr_dt,
                    duration_seconds=dur,
                    cabin_class=_qg_cabin,
                )
                route = FlightRoute(
                    segments=[segment],
                    total_duration_seconds=dur,
                    stopovers=len(segments) - 1 if len(segments) > 1 else 0,
                )
                fid = hashlib.md5(
                    f"qg_{flight_no}_{price}_{req.date_from}_{dep_dt.isoformat() if hasattr(dep_dt, 'isoformat') else dep_dt}".encode()
                ).hexdigest()[:12]
                offers.append(FlightOffer(
                    id=f"qg_{fid}",
                    price=round(price, 2),
                    currency="IDR",
                    price_formatted=f"IDR {price:,.0f}",
                    outbound=route,
                    inbound=None,
                    airlines=["Citilink"],
                    owner_airline="QG",
                    booking_url=_BOOK_URL,
                    is_locked=False,
                    source="citilink_direct",
                    source_tier="free",
                ))

        return offers
    
    def _extract_segment_info(self, segments: list, dep_date: datetime) -> dict:
        """Extract flight details from Navitaire segments."""
        info = {
            "flight_no": "",
            "departure": dep_date,
            "arrival": dep_date,
            "origin": "",
            "destination": "",
        }
        
        if not segments:
            return info
            
        first_seg = segments[0] if isinstance(segments, list) else segments
        if not isinstance(first_seg, dict):
            return info
        
        # Flight number from identifier
        identifier = first_seg.get("identifier", {})
        logger.info("Citilink: extract_segment_info identifier=%s type=%s", str(identifier)[:100], type(identifier).__name__)
        if isinstance(identifier, dict):
            # DotRez structure: identifier.carrier + identifier.identifier
            carrier = identifier.get("carrier") or identifier.get("carrierCode", "QG")
            fn = identifier.get("identifier") or identifier.get("flightNumber", "")
            logger.info("Citilink: extract_segment_info carrier=%s fn=%s", carrier, fn)
            if fn:
                info["flight_no"] = f"{carrier}{fn}"
                logger.info("Citilink: extract_segment_info set flight_no=%s", info["flight_no"])
        
        # Alternative flight number locations
        if not info["flight_no"]:
            info["flight_no"] = (
                first_seg.get("flightNumber") or
                first_seg.get("flightNo") or
                first_seg.get("flight", {}).get("flightNumber", "") if isinstance(first_seg.get("flight"), dict) else "" or
                ""
            )
        
        # Designator has origin/destination/times
        designator = first_seg.get("designator", {})
        if isinstance(designator, dict):
            info["origin"] = designator.get("origin", "")
            info["destination"] = designator.get("destination", "")
            
            dep_str = designator.get("departure", "")
            arr_str = designator.get("arrival", "")
            
            if dep_str:
                parsed = self._parse_dt(dep_str, dep_date)
                if parsed:
                    info["departure"] = parsed
            if arr_str:
                parsed = self._parse_dt(arr_str, dep_date)
                if parsed:
                    info["arrival"] = parsed
        
        # Alternative time locations
        if info["departure"] == dep_date:
            for key in ["departureTime", "departure", "std", "departureDateTime"]:
                val = first_seg.get(key)
                if val:
                    parsed = self._parse_dt(str(val), dep_date)
                    if parsed:
                        info["departure"] = parsed
                        break
        
        if info["arrival"] == dep_date:
            for key in ["arrivalTime", "arrival", "sta", "arrivalDateTime"]:
                val = first_seg.get(key)
                if val:
                    parsed = self._parse_dt(str(val), dep_date)
                    if parsed:
                        info["arrival"] = parsed
                        break
        
        # If multi-segment, get final destination from last segment
        if len(segments) > 1:
            last_seg = segments[-1]
            if isinstance(last_seg, dict):
                last_des = last_seg.get("designator", {})
                if isinstance(last_des, dict) and last_des.get("destination"):
                    info["destination"] = last_des["destination"]
                arr_str = last_des.get("arrival") if isinstance(last_des, dict) else last_seg.get("arrivalTime")
                if arr_str:
                    parsed = self._parse_dt(str(arr_str), dep_date)
                    if parsed:
                        info["arrival"] = parsed
        
        return info
    
    def _extract_price(self, fare: dict, fares_available: dict) -> float:
        """Extract price from fare object, potentially looking up in faresAvailable."""
        price = None
        
        # First check if fare has a key to lookup in faresAvailable
        fare_key = fare.get("fareAvailabilityKey") or fare.get("fareKey")
        if fare_key and fare_key in fares_available:
            fare_data = fares_available[fare_key]
            if isinstance(fare_data, dict):
                # Try totals.fareTotal (DotRez NSK v1 structure)
                totals = fare_data.get("totals", {})
                if isinstance(totals, dict):
                    fare_total = totals.get("fareTotal") or totals.get("total") or totals.get("passengerTotals", {}).get("ADT", {}).get("fareTotal")
                    if fare_total:
                        try:
                            price = float(str(fare_total).replace(",", ""))
                            if price > 0:
                                return price
                        except (ValueError, TypeError):
                            pass
                
                # Try passengerFares array
                pf_list = fare_data.get("passengerFares", [])
                for pf in pf_list:
                    if isinstance(pf, dict):
                        for pk in ["fareAmount", "amount", "total", "price", "totalAmount"]:
                            if pk in pf:
                                try:
                                    price = float(str(pf[pk]).replace(",", ""))
                                    if price > 0:
                                        return price
                                except (ValueError, TypeError):
                                    continue
        
        # Try direct price keys on fare object
        for key in [
            "totalPrice", "price", "fare", "amount",
            "adultFare", "displayPrice", "fareAmount",
            "total", "adultPrice", "lowestFare", "totalAmount",
        ]:
            val = fare.get(key)
            if val is not None:
                try:
                    price = float(str(val).replace(",", ""))
                    if price > 0:
                        return price
                except (ValueError, TypeError):
                    continue
        
        # Try nested price structures in fare object
        for nested_key in ["passengerFares", "fareBreakdown", "priceBreakdown"]:
            nested = fare.get(nested_key, [])
            if nested and isinstance(nested, list):
                for pf in nested:
                    if isinstance(pf, dict):
                        for pk in ["fareAmount", "amount", "total", "price", "totalAmount"]:
                            if pk in pf:
                                try:
                                    price = float(str(pf[pk]).replace(",", ""))
                                    if price > 0:
                                        return price
                                except (ValueError, TypeError):
                                    continue
        
        return price or 0
    
    def _extract_price_from_fare_available(self, fare_data: dict) -> float:
        """Extract price from a faresAvailable entry (DotRez format)."""
        price = None
        
        # Try totals.passengerTotals.ADT.fareTotal
        totals = fare_data.get("totals", {})
        if isinstance(totals, dict):
            pt = totals.get("passengerTotals", {})
            if isinstance(pt, dict):
                for pax_type in ["ADT", "adult", "ADULT"]:
                    if pax_type in pt and isinstance(pt[pax_type], dict):
                        for pk in ["fareTotal", "total", "totalAmount", "amount"]:
                            if pk in pt[pax_type]:
                                try:
                                    price = float(str(pt[pax_type][pk]).replace(",", ""))
                                    if price > 0:
                                        return price
                                except (ValueError, TypeError):
                                    continue
        
        # Try totals direct keys
        if isinstance(totals, dict):
            for pk in ["fareTotal", "total", "totalAmount", "grandTotal"]:
                if pk in totals:
                    try:
                        price = float(str(totals[pk]).replace(",", ""))
                        if price > 0:
                            return price
                    except (ValueError, TypeError):
                        continue
        
        # Try fares array inside fare_data
        fares = fare_data.get("fares", [])
        if isinstance(fares, list):
            for f in fares:
                if isinstance(f, dict):
                    # Try passengerFares
                    pf_list = f.get("passengerFares", [])
                    for pf in pf_list:
                        if isinstance(pf, dict):
                            for pk in ["fareAmount", "amount", "total", "price", "totalAmount", "serviceCharges"]:
                                if pk in pf:
                                    val = pf[pk]
                                    # serviceCharges is typically a list
                                    if pk == "serviceCharges" and isinstance(val, list):
                                        for sc in val:
                                            if isinstance(sc, dict) and "amount" in sc:
                                                try:
                                                    price = float(str(sc["amount"]).replace(",", ""))
                                                    if price > 0:
                                                        return price
                                                except (ValueError, TypeError):
                                                    continue
                                    else:
                                        try:
                                            price = float(str(val).replace(",", ""))
                                            if price > 0:
                                                return price
                                        except (ValueError, TypeError):
                                            continue
        
        # Try passengerFares directly on fare_data
        pf_list = fare_data.get("passengerFares", [])
        for pf in pf_list:
            if isinstance(pf, dict):
                for pk in ["fareAmount", "amount", "total", "price", "totalAmount"]:
                    if pk in pf:
                        try:
                            price = float(str(pf[pk]).replace(",", ""))
                            if price > 0:
                                return price
                        except (ValueError, TypeError):
                            continue
        
        return price or 0

    def _parse_html(self, html: str, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse Navitaire rendered HTML for flight results."""
        offers = []
        dep_date = datetime.strptime(str(req.date_from), "%Y-%m-%d")

        # Navitaire HTML patterns: fare-row, flight-strip, availability row
        cards = re.findall(
            r'<(?:div|tr|li)[^>]*class="[^"]*(?:fare|flight|avail|journey|schedule)[^"]*"[^>]*>(.*?)</(?:div|tr|li)>',
            html, re.S | re.I,
        )

        for card in cards:
            # Price (IDR format)
            price_m = re.search(
                r'(?:Rp\.?\s*|IDR\s*)?(\d[\d.,]+)',
                card, re.I,
            )
            if not price_m:
                continue
            try:
                ps = price_m.group(1).replace(".", "").replace(",", "")
                price = float(ps)
            except (ValueError, TypeError):
                continue
            if price < 50000:  # Minimum IDR for a flight
                continue

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
            fn_m = re.search(r'\b(QG\s*\d+)\b', card)
            flight_no = fn_m.group(1).replace(" ", "") if fn_m else ""

            _qg_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
            segment = FlightSegment(
                airline="QG",
                airline_name="Citilink",
                flight_no=flight_no,
                origin=req.origin,
                destination=req.destination,
                departure=dep_dt,
                arrival=arr_dt,
                duration_seconds=dur,
                cabin_class=_qg_cabin,
            )
            route = FlightRoute(segments=[segment], total_duration_seconds=dur, stopovers=0)
            fid = hashlib.md5(f"qg_{flight_no}_{price}_{req.date_from}".encode()).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"qg_{fid}",
                price=round(price, 2),
                currency="IDR",
                price_formatted=f"IDR {price:,.0f}",
                outbound=route,
                inbound=None,
                airlines=["Citilink"],
                owner_airline="QG",
                booking_url=_BOOK_URL,
                is_locked=False,
                source="citilink_direct",
                source_tier="free",
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
    def _combine_rt(ob: list, ib: list, req) -> list:
        combos = []
        for o in sorted(ob, key=lambda x: x.price)[:15]:
            for i in sorted(ib, key=lambda x: x.price)[:10]:
                combos.append(FlightOffer(
                    id=f"qg_rt_{o.id}_{i.id}",
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

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"citilink{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="IDR",
            offers=[],
            total_results=0,
        )
