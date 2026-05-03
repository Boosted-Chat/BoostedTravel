"""
Saudia (SV) CDP Chrome connector — Angular Material form fill + DOM scraping.

Saudia's website at saudia.com is an Angular 16 app with Material Design components
and ngx-daterangepicker-material for the calendar. Imperva WAF blocks headless/curl
requests; headed CDP Chrome works.

Strategy (CDP Chrome + form fill + DOM scrape):
1. Launch headed Chrome via CDP (off-screen, stealth).
2. Navigate to saudia.com/en-SA → Angular SPA loads with search widget.
3. Click One Way radio → fill origin/dest via mat-autocomplete dropdown
   → open date picker (table.table-condensed) → select date → close calendar.
4. Click "Search Flights" → dismiss "Terminal Changes" modal → wait for
   Angular to navigate to /en-SA/booking results page.
5. Inject XHR monkey-patch to capture air-bounds API response; fall back
   to DOM scraping on the results page.

Key selectors (Angular Material / ngx-daterangepicker):
  - Origin:      input[placeholder='From']  → mat-option dropdown
  - Destination: input[placeholder='To']    → mat-option dropdown
  - Date picker: mat-form-field with mat-label 'Departing'
  - Calendar:    div.calendar-table > table.table-condensed > td.available:not(.off) > span
  - Modal:       .cdk-overlay-container button matching /continue/i
  - Search:      button matching /search.?flight/i
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

_DEBUG_PORT = 9481
_USER_DATA_DIR = os.path.join(
    os.environ.get("TEMP", os.environ.get("TMPDIR", "/tmp")), ".saudia_chrome_data"
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
    """Persistent Chrome via CDP (headed — Imperva blocks headless)."""
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
            logger.info("Saudia: connected to existing Chrome on port %d", _DEBUG_PORT)
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
            logger.info("Saudia: Chrome launched on CDP port %d", _DEBUG_PORT)

        contexts = _browser.contexts
        _context = contexts[0] if contexts else await _browser.new_context()
        return _context


async def _reset_profile():
    """Wipe Chrome profile on WAF block."""
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
        except Exception:
            pass


async def _dismiss_overlays(page) -> None:
    """Accept cookie consent and remove blocking overlays."""
    try:
        await page.evaluate("""() => {
            for (const b of document.querySelectorAll('button'))
                if (b.textContent.includes('Yes, I accept') || b.textContent.includes('doneYes'))
                    { b.click(); return; }
        }""")
    except Exception:
        pass
    try:
        await page.evaluate("""() => {
            document.querySelectorAll('.custom-overlay, .custom-overlay--visible').forEach(el => {
                el.style.display = 'none'; el.style.pointerEvents = 'none';
            });
        }""")
    except Exception:
        pass


async def _dismiss_modal(page) -> bool:
    """Click 'Continue' in the CDK overlay modal (e.g. 'Terminal Changes for RUH')."""
    try:
        result = await page.evaluate("""() => {
            const c = document.querySelector('.cdk-overlay-container');
            if (!c) return 'no-overlay';
            for (const b of c.querySelectorAll('button')) {
                if (b.offsetHeight > 0 && /^continue$/i.test(b.textContent.trim())) {
                    b.click(); return 'clicked-continue';
                }
            }
            // Force-remove blocking overlays if no Continue button
            c.querySelectorAll('.cdk-overlay-backdrop').forEach(el => el.remove());
            c.querySelectorAll('mat-dialog-container').forEach(d => {
                const pane = d.closest('.cdk-overlay-pane');
                if (pane) pane.remove();
            });
            return 'removed-overlay';
        }""")
        return result == "clicked-continue"
    except Exception:
        return False


# XHR monkey-patch injected into the page to capture air-bounds responses.
_XHR_CAPTURE_SCRIPT = """() => {
    if (window.__svCapture) return 'already';
    window.__svCapture = [];
    const origOpen = XMLHttpRequest.prototype.open;
    const origSend = XMLHttpRequest.prototype.send;
    XMLHttpRequest.prototype.open = function(method, url) {
        this.__svUrl = url; this.__svMethod = method;
        return origOpen.apply(this, arguments);
    };
    XMLHttpRequest.prototype.send = function(body) {
        const self = this, url = self.__svUrl || '';
        if (/air-bound|airsearch/i.test(url)) {
            self.addEventListener('load', function() {
                if (self.status === 200 && self.responseText)
                    window.__svCapture.push({url, status: self.status, body: self.responseText});
            });
        }
        return origSend.apply(this, arguments);
    };
    return 'injected';
}"""


class SaudiaConnectorClient:
    """Saudia (SV) CDP Chrome connector — Angular form fill + DOM scraping."""

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
                logger.debug("Ancillary fetch timed out for %s→%s", anc_origin, anc_dest)
            except Exception as _anc_err:
                logger.debug("Ancillary fetch error for %s→%s: %s", anc_origin, anc_dest, _anc_err)

        return ob_result

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        context = await _get_context()
        page = await context.new_page()
        await auto_block_if_proxied(page)

        # Interception state
        search_data: dict = {}
        api_event = asyncio.Event()

        async def _on_response(response):
            url = response.url.lower()
            if response.status not in (200, 201):
                return
            try:
                if any(k in url for k in ["/air-bound", "/airsearch", "/availability",
                                           "/flightresult", "dapi.saudia.com"]):
                    ct = response.headers.get("content-type", "")
                    if "json" not in ct and "text" not in ct:
                        return
                    body = await response.text()
                    if len(body) < 50:
                        return
                    try:
                        data = json.loads(body)
                    except json.JSONDecodeError:
                        return
                    if not isinstance(data, dict):
                        return
                    keys_lower = " ".join(str(k).lower() for k in data.keys())
                    if any(k in keys_lower for k in ["flight", "itiner", "offer", "fare",
                                                       "bound", "trip", "result", "segment",
                                                       "avail", "journey"]):
                        search_data.update(data)
                        api_event.set()
                        logger.info("Saudia: captured search response from %s (%d keys)", url[:80], len(data))
            except Exception:
                pass

        page.on("response", _on_response)

        try:
            # Step 1: Load homepage
            logger.info("Saudia: loading homepage for %s→%s", req.origin, req.destination)
            await page.goto("https://www.saudia.com/en-SA", wait_until="domcontentloaded", timeout=40000)
            await asyncio.sleep(2.5)

            # Inject XHR capture + dismiss overlays
            await page.evaluate(_XHR_CAPTURE_SCRIPT)
            await _dismiss_overlays(page)

            content_len = await page.evaluate("() => document.body.innerHTML.length")
            if content_len < 10000:
                logger.warning("Saudia: page too small (%d), possibly WAF blocked", content_len)
                return self._empty(req)

            # Step 2: Click trip type radio
            if not req.return_from:
                await page.evaluate("""() => {
                    for (const r of document.querySelectorAll('mat-radio-button'))
                        if (/one.?way/i.test(r.textContent) && r.offsetHeight > 0)
                            { r.querySelector('.mat-radio-outer-circle, label')?.click(); return; }
                }""")
            else:
                await page.evaluate("""() => {
                    for (const r of document.querySelectorAll('mat-radio-button'))
                        if (/round.?trip|return/i.test(r.textContent) && r.offsetHeight > 0)
                            { r.querySelector('.mat-radio-outer-circle, label')?.click(); return; }
                }""")
            await asyncio.sleep(0.5)

            # Step 3: Fill origin
            ok = await self._fill_airport(page, req.origin, "From")
            if not ok:
                logger.warning("Saudia: origin fill failed for %s", req.origin)
                return self._empty(req)
            await asyncio.sleep(0.8)

            # Step 4: Fill destination
            ok = await self._fill_airport(page, req.destination, "To")
            if not ok:
                logger.warning("Saudia: destination fill failed for %s", req.destination)
                return self._empty(req)
            await asyncio.sleep(0.8)

            # Step 5: Fill date
            ok = await self._fill_date(page, req)
            if not ok:
                logger.warning("Saudia: date fill failed")
                return self._empty(req)

            # Step 6: Click Search Flights via mouse (Angular requires real click)
            search_pos = await page.evaluate("""() => {
                for (const b of document.querySelectorAll('button')) {
                    if (b.offsetHeight > 0 && /search.?flight/i.test(b.textContent)) {
                        const r = b.getBoundingClientRect();
                        return {x: r.x + r.width / 2, y: r.y + r.height / 2};
                    }
                }
                return null;
            }""")
            if search_pos:
                await page.mouse.click(search_pos["x"], search_pos["y"])
            else:
                # Fallback to aria-label selector
                await page.evaluate("""() => {
                    const btn = document.querySelector('button[aria-label="Search Flights"]');
                    if (btn) btn.click();
                }""")
            logger.info("Saudia: search clicked")
            await asyncio.sleep(1.5)

            # Step 7: Dismiss "Terminal Changes" modal (click Continue)
            for _ in range(3):
                dismissed = await _dismiss_modal(page)
                if not dismissed:
                    break
                await asyncio.sleep(0.5)
            logger.info("Saudia: modal dismissed, waiting for results…")

            # Step 8: Wait for results page
            remaining = max(self.timeout - (time.monotonic() - t0), 15)
            deadline = time.monotonic() + remaining

            while time.monotonic() < deadline:
                if api_event.is_set():
                    break
                url = page.url
                if "/booking" in url.lower():
                    # Check if results rendered in DOM
                    dom_ready = await page.evaluate("""() => {
                        const text = document.body.innerText || '';
                        return /\\d{1,2}:\\d{2}/.test(text) || /no flights found/i.test(text);
                    }""")
                    if dom_ready:
                        break
                await asyncio.sleep(1.0)

            # Extra wait for API interception
            if not api_event.is_set():
                try:
                    await asyncio.wait_for(api_event.wait(), timeout=6.0)
                except asyncio.TimeoutError:
                    pass

            # Check XHR monkey-patch for captured data
            if not search_data:
                try:
                    xhr_captures = await page.evaluate(
                        "() => (window.__svCapture || []).filter(c => c.body && c.status === 200)"
                    )
                    for cap in (xhr_captures or []):
                        body = cap.get("body", "")
                        if body:
                            try:
                                data = json.loads(body)
                                if isinstance(data, dict):
                                    search_data.update(data)
                                    api_event.set()
                                    logger.info("Saudia: got data from XHR capture (%d chars)", len(body))
                            except json.JSONDecodeError:
                                pass
                except Exception:
                    pass

            # Step 9: Parse results
            offers = []
            if search_data:
                offers = self._parse_api_response(search_data, req)
                logger.info("Saudia: parsed %d offers from API", len(offers))

            if not offers:
                offers = await self._scrape_dom(page, req)
                if offers:
                    logger.info("Saudia: scraped %d offers from DOM", len(offers))

            offers.sort(key=lambda o: o.price)

            elapsed = time.monotonic() - t0
            logger.info("Saudia %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

            search_hash = hashlib.md5(
                f"saudia{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
            ).hexdigest()[:12]

            currency = offers[0].currency if offers else "SAR"
            return FlightSearchResponse(
                search_id=f"fs_{search_hash}",
                origin=req.origin,
                destination=req.destination,
                currency=currency,
                offers=offers,
                total_results=len(offers),
            )

        except Exception as e:
            logger.error("Saudia error: %s", e)
            return self._empty(req)
        finally:
            try:
                page.remove_listener("response", _on_response)
            except Exception:
                pass
            try:
                await page.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Form interaction
    # ------------------------------------------------------------------

    async def _fill_airport(self, page, iata: str, placeholder: str) -> bool:
        """Fill Angular Material autocomplete using placeholder-based selector.

        Args:
            page: Playwright page
            iata: 3-letter IATA code (e.g. "JED")
            placeholder: input placeholder text – "From" or "To"
        """
        try:
            # Clear existing value and focus via JS
            await page.evaluate("""(ph) => {
                const inp = document.querySelector(`input[placeholder='${ph}']`);
                if (!inp) return;
                inp.value = '';
                inp.dispatchEvent(new Event('input', {bubbles: true}));
                inp.focus();
                inp.click();
            }""", placeholder)
            await asyncio.sleep(0.5)

            # Type IATA with keyboard (triggers Angular ngModel)
            await page.keyboard.press("Home")
            await page.keyboard.press("Shift+End")
            await page.keyboard.press("Backspace")
            await page.keyboard.type(iata, delay=80)
            await asyncio.sleep(2.0)

            # Click matching mat-option
            selected = await page.evaluate("""(iata) => {
                const opts = document.querySelectorAll('mat-option, [role="option"]');
                for (const opt of opts) {
                    if (opt.offsetHeight > 0 && opt.textContent.includes(iata)) {
                        opt.click();
                        return opt.textContent.trim().slice(0, 80);
                    }
                }
                return null;
            }""", iata)

            if not selected:
                # Fallback: select first visible option
                await page.keyboard.press("ArrowDown")
                await asyncio.sleep(0.2)
                await page.keyboard.press("Enter")

            await asyncio.sleep(0.5)
            logger.info("Saudia: airport %s (%s) → %s", placeholder, iata, selected or "(keyboard)")
            return True

        except Exception as e:
            logger.warning("Saudia: airport fill error for %s: %s", iata, e)
            return False

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        """Open ngx-daterangepicker calendar and click target date.

        Uses ``table.table-condensed`` inside ``div.calendar-table``, navigates
        months via ``th.next``, verifies month via ``th.month`` text.
        The range picker needs two clicks (start = end for one-way).
        Retries up to 3 times.
        """
        for attempt in range(3):
            ok = await self._fill_date_attempt(page, req)
            if ok:
                return True
            logger.info("Saudia: date attempt %d failed, retrying...", attempt + 1)
            await page.keyboard.press("Escape")
            await asyncio.sleep(1.0)
        return False

    async def _fill_date_attempt(self, page, req: FlightSearchRequest) -> bool:
        """Single attempt at filling the date via the calendar picker."""
        try:
            dt = req.date_from if isinstance(req.date_from, (datetime, date)) else datetime.strptime(str(req.date_from), "%Y-%m-%d")
        except (ValueError, TypeError):
            return False

        target_month = dt.strftime("%B")   # e.g. "April"
        target_year = str(dt.year)
        target_day = str(dt.day)

        try:
            # Click Depart date field (mat-form-field with label "Depart")
            await page.evaluate("""() => {
                const ffs = document.querySelectorAll('mat-form-field');
                for (const f of ffs) {
                    const lbl = f.querySelector('mat-label');
                    if (lbl && /depart/i.test(lbl.textContent)) {
                        const inp = f.querySelector('input');
                        if (inp) { inp.click(); return; }
                    }
                }
                // Fallback: click any date-looking input
                const inp = document.querySelector("input[formcontrolname='hijriCal']")
                    || document.querySelector("input[placeholder*='Depart']");
                if (inp) inp.click();
            }""")
            logger.info("Saudia: clicked depart field")
            await asyncio.sleep(3.0)

            # Dismiss any modal that appeared (terminal changes dialog)
            await _dismiss_modal(page)
            await asyncio.sleep(1.0)

            # Verify calendar is visible (re-click if needed)
            cal_visible = await page.evaluate("""() => {
                const p = document.querySelector('.md-drppicker');
                return p && p.offsetHeight > 100;
            }""")
            if not cal_visible:
                # Re-click the depart field
                await page.evaluate("""() => {
                    const ffs = document.querySelectorAll('mat-form-field');
                    for (const f of ffs) {
                        const lbl = f.querySelector('mat-label');
                        if (lbl && /depart/i.test(lbl.textContent)) {
                            const inp = f.querySelector('input');
                            if (inp) { inp.click(); return; }
                        }
                    }
                }""")
                await asyncio.sleep(2.0)

            # Find the VISIBLE calendar panel
            vis_idx = await page.evaluate("""() => {
                const panels = document.querySelectorAll('.md-drppicker');
                for (let i = 0; i < panels.length; i++)
                    if (panels[i].offsetHeight > 100) return i;
                return -1;
            }""")
            if vis_idx < 0:
                logger.warning("Saudia: no visible calendar panel")
                return False

            # Navigate to target month/year
            for _ in range(14):
                month_text = await page.evaluate("""(idx) => {
                    const panel = document.querySelectorAll('.md-drppicker')[idx];
                    if (!panel) return '';
                    const hdr = panel.querySelector('th.month');
                    return hdr ? hdr.textContent.trim() : '';
                }""", vis_idx)

                if target_month.lower() in month_text.lower() and target_year in month_text:
                    break

                # Click next-month arrow
                await page.evaluate("""(idx) => {
                    const panel = document.querySelectorAll('.md-drppicker')[idx];
                    if (panel) {
                        const n = panel.querySelector('th.next');
                        if (n) n.click();
                    }
                }""", vis_idx)
                await asyncio.sleep(0.5)

            # Click target day in table.table-condensed
            day_clicked = await page.evaluate("""(args) => {
                const [idx, day, month] = args;
                const panel = document.querySelectorAll('.md-drppicker')[idx];
                if (!panel) return false;
                // DOM: div.calendar-table > table.table-condensed > tbody > tr > td.available
                const tables = panel.querySelectorAll('table.table-condensed');
                for (const table of tables) {
                    const hdr = table.querySelector('th.month');
                    if (hdr && !hdr.textContent.toLowerCase().includes(month.toLowerCase())) continue;
                    for (const td of table.querySelectorAll('td.available:not(.off)')) {
                        if (td.offsetHeight === 0) continue;
                        const span = td.querySelector('span');
                        if (!span) continue;
                        // span text is just the day number (e.g. "26")
                        if (span.textContent.trim() === day) {
                            td.click();
                            return true;
                        }
                    }
                }
                // Fallback: broaden search to any visible td in the panel
                for (const td of panel.querySelectorAll('td')) {
                    if (td.offsetHeight === 0 || td.classList.contains('off')) continue;
                    const span = td.querySelector('span');
                    if (span && span.textContent.trim() === day) {
                        td.click();
                        return true;
                    }
                }
                return false;
            }""", [vis_idx, target_day, target_month])

            if not day_clicked:
                logger.warning("Saudia: day %s %s not found in panel", target_day, target_month)
                return False
            logger.info("Saudia: clicked day %s %s %s", target_day, target_month, target_year)
            await asyncio.sleep(1.5)

            # Range picker: second click = end date
            # For round-trip, navigate to return month and click return day
            # For one-way, click same departure day again
            if req.return_from:
                ret_dt = req.return_from if isinstance(req.return_from, (datetime, date)) else datetime.strptime(str(req.return_from), "%Y-%m-%d")
                ret_month = ret_dt.strftime("%B")
                ret_year = str(ret_dt.year)
                ret_day = str(ret_dt.day)
                # Navigate to return month if different
                for _ in range(14):
                    month_text = await page.evaluate("""(idx) => {
                        const panel = document.querySelectorAll('.md-drppicker')[idx];
                        if (!panel) return '';
                        const hdr = panel.querySelector('th.month');
                        return hdr ? hdr.textContent.trim() : '';
                    }""", vis_idx)
                    if ret_month.lower() in month_text.lower() and ret_year in month_text:
                        break
                    await page.evaluate("""(idx) => {
                        const panel = document.querySelectorAll('.md-drppicker')[idx];
                        if (panel) { const n = panel.querySelector('th.next'); if (n) n.click(); }
                    }""", vis_idx)
                    await asyncio.sleep(0.5)
                await page.evaluate("""(args) => {
                    const [idx, day, month] = args;
                    const panel = document.querySelectorAll('.md-drppicker')[idx];
                    if (!panel) return;
                    for (const td of panel.querySelectorAll('td:not(.off)')) {
                        if (td.offsetHeight === 0) continue;
                        const span = td.querySelector('span');
                        if (span && span.textContent.trim() === day) {
                            td.click(); return;
                        }
                    }
                }""", [vis_idx, ret_day, ret_month])
            else:
                await page.evaluate("""(args) => {
                    const [idx, day, month] = args;
                    const panel = document.querySelectorAll('.md-drppicker')[idx];
                    if (!panel) return;
                    for (const td of panel.querySelectorAll('td:not(.off)')) {
                        if (td.offsetHeight === 0) continue;
                        const span = td.querySelector('span');
                        if (span && span.textContent.trim() === day) {
                            td.click(); return;
                        }
                    }
                }""", [vis_idx, target_day, target_month])
            await asyncio.sleep(1.0)

            # Close calendar
            await page.keyboard.press("Escape")
            await asyncio.sleep(0.5)

            logger.info("Saudia: date set to %s %s, %s", target_day, target_month, target_year)
            return True

        except Exception as e:
            logger.warning("Saudia: date error: %s", e)
            return False

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    def _parse_api_response(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse Saudia search API response."""
        raw_offers = []

        # Try various response shapes
        flights = (
            data.get("flightAvailability") or data.get("flights") or
            data.get("journeys") or data.get("results") or
            data.get("itineraries") or data.get("offers") or
            data.get("outbound") or data.get("schedules") or []
        )

        if isinstance(flights, dict):
            for key in ("flights", "journeys", "results", "options", "recommendations"):
                if key in flights:
                    flights = flights[key]
                    break
            else:
                flights = [flights]

        if not isinstance(flights, list):
            flights = self._find_flights(data)

        for flight in flights:
            offer = self._build_offer(flight, req)
            if offer:
                raw_offers.append(offer)

        if not req.return_from or not raw_offers:
            return raw_offers

        # Classify OB/IB by first segment origin
        ob_offers = []
        ib_offers = []
        for o in raw_offers:
            first_org = o.outbound.segments[0].origin if o.outbound and o.outbound.segments else ""
            if first_org == req.destination:
                ib_offers.append(o)
            else:
                ob_offers.append(o)

        if not ib_offers:
            return raw_offers

        # Find cheapest inbound
        best_ib = min(ib_offers, key=lambda o: o.price)
        ib_route = best_ib.outbound  # the "outbound" of the IB offer is actually the inbound route
        ib_price = best_ib.price

        # Combine OB offers with cheapest IB
        combined = []
        for o in ob_offers:
            cp = round(o.price + ib_price, 2)
            oid = hashlib.md5(f"sv_rt_{o.id}_{best_ib.id}".encode()).hexdigest()[:12]
            combined.append(FlightOffer(
                id=f"sv_rt_{oid}",
                price=cp,
                currency=o.currency,
                price_formatted=f"{o.currency} {cp:,.0f}",
                outbound=o.outbound,
                inbound=ib_route,
                airlines=o.airlines,
                owner_airline="SV",
                booking_url=self._booking_url(req),
                is_locked=False,
                source="saudia_direct",
                source_tier="free",
            ))
        return combined

    def _find_flights(self, data, depth=0) -> list:
        """Recursively find flight arrays in nested data."""
        if depth > 4 or not isinstance(data, dict):
            return []
        for key, val in data.items():
            if isinstance(val, list) and len(val) > 0 and isinstance(val[0], dict):
                sample_keys = {str(k).lower() for k in val[0].keys()}
                if sample_keys & {"price", "fare", "flight", "departure", "segment", "leg", "journey"}:
                    return val
            elif isinstance(val, dict):
                result = self._find_flights(val, depth + 1)
                if result:
                    return result
        return []

    def _build_offer(self, flight: dict, req: FlightSearchRequest) -> Optional[FlightOffer]:
        """Build FlightOffer from API flight data."""
        try:
            price = (
                flight.get("price") or flight.get("totalPrice") or
                flight.get("fare") or flight.get("lowest") or
                flight.get("amount") or 0
            )
            if isinstance(price, dict):
                price = price.get("amount") or price.get("total") or price.get("value") or 0
            price = float(price) if price else 0
            if price <= 0:
                return None

            currency = self._extract_currency(flight) or "SAR"

            segments_data = flight.get("segments") or flight.get("legs") or flight.get("flights") or []
            if not isinstance(segments_data, list):
                segments_data = [flight]

            segments = []
            for seg in segments_data:
                dep_str = seg.get("departure") or seg.get("departureTime") or seg.get("depTime") or ""
                arr_str = seg.get("arrival") or seg.get("arrivalTime") or seg.get("arrTime") or ""
                dep_dt = self._parse_dt(dep_str, req.date_from)
                arr_dt = self._parse_dt(arr_str, req.date_from)

                airline_code = seg.get("airline") or seg.get("carrierCode") or seg.get("operatingCarrier") or "SV"
                flight_no = seg.get("flightNumber") or seg.get("flightNo") or ""
                if flight_no and not flight_no.startswith(airline_code):
                    flight_no = f"{airline_code}{flight_no}"

                _sv_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
                segments.append(FlightSegment(
                    airline=airline_code[:2],
                    airline_name="Saudia" if airline_code == "SV" else airline_code,
                    flight_no=flight_no or "SV",
                    origin=seg.get("origin") or seg.get("departureAirport") or req.origin,
                    destination=seg.get("destination") or seg.get("arrivalAirport") or req.destination,
                    departure=dep_dt,
                    arrival=arr_dt,
                    cabin_class=_sv_cabin,
                ))

            if not segments:
                return None

            duration = flight.get("duration") or flight.get("totalDuration") or 0
            if isinstance(duration, str):
                m = re.search(r"(\d+)[hH].*?(\d+)?", duration)
                if m:
                    duration = int(m.group(1)) * 60 + int(m.group(2) or 0)

            route = FlightRoute(
                segments=segments,
                total_duration_seconds=int(duration) * 60 if duration else 0,
                stopovers=max(0, len(segments) - 1),
            )

            offer_id = hashlib.md5(
                f"sv_{req.origin}_{req.destination}_{req.date_from}_{price}_{segments[0].flight_no}".encode()
            ).hexdigest()[:12]

            return FlightOffer(
                id=f"sv_{offer_id}",
                price=round(price, 2),
                currency=currency,
                price_formatted=f"{currency} {price:,.0f}",
                outbound=route,
                inbound=None,
                airlines=list({s.airline for s in segments}),
                owner_airline="SV",
                booking_url=self._booking_url(req),
                is_locked=False,
                source="saudia_direct",
                source_tier="free",
            )
        except Exception as e:
            logger.debug("Saudia: offer parse error: %s", e)
            return None

    @staticmethod
    def _extract_currency(d: dict) -> str:
        for key in ("currency", "currencyCode"):
            val = d.get(key)
            if isinstance(val, str) and len(val) == 3:
                return val.upper()
            if isinstance(val, dict):
                return val.get("code", "SAR")
        if isinstance(d.get("price"), dict):
            return d["price"].get("currency", "SAR")
        return "SAR"

    @staticmethod
    def _parse_dt(s, fallback_date) -> datetime:
        if not s:
            try:
                dt = fallback_date if isinstance(fallback_date, (datetime, date)) else datetime.strptime(str(fallback_date), "%Y-%m-%d")
                return datetime(dt.year, dt.month, dt.day) if isinstance(dt, date) and not isinstance(dt, datetime) else dt
            except Exception:
                return datetime.now()
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M"):
            try:
                return datetime.strptime(s[:19], fmt)
            except (ValueError, TypeError):
                continue
        m = re.search(r"(\d{1,2}):(\d{2})", str(s))
        if m:
            try:
                dt = fallback_date if isinstance(fallback_date, (datetime, date)) else datetime.strptime(str(fallback_date), "%Y-%m-%d")
                d = dt.date() if isinstance(dt, datetime) else dt if isinstance(dt, date) else dt
                return datetime(d.year, d.month, d.day, int(m.group(1)), int(m.group(2)))
            except Exception:
                pass
        return datetime.now()

    # ------------------------------------------------------------------
    # DOM scraping fallback
    # ------------------------------------------------------------------

    async def _scrape_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Scrape flight results from DOM (Angular components on booking page)."""
        await asyncio.sleep(2)

        # Check for "No flights found" message first
        no_flights = await page.evaluate("""() => {
            const body = document.body?.innerText || '';
            return /no flights? found|no results|no available/i.test(body);
        }""")
        if no_flights:
            logger.info("Saudia: page says no flights found")
            return []

        flights = await page.evaluate(r"""(params) => {
            const [origin, destination] = params;
            const results = [];

            // Saudia Angular uses flight bound cards inside the booking page.
            // Look for flight cards, bound-selection components, or fare-family cards.
            const selectors = [
                'app-jss-flight-data',
                'app-jss-flight-list [class*="flight"]',
                'app-bound-selection [class*="flight"]',
                '[class*="flight-card"]',
                '[class*="bound-card"]',
                '[class*="itinerary-card"]',
                'mat-card[class*="flight"]',
                '[data-flight]',
            ];
            const seen = new Set();
            const cards = [];
            for (const sel of selectors) {
                for (const el of document.querySelectorAll(sel)) {
                    if (!seen.has(el) && el.offsetHeight > 0) { seen.add(el); cards.push(el); }
                }
            }
            // Fallback: any mat-card with time patterns
            if (cards.length === 0) {
                for (const card of document.querySelectorAll('mat-card, [class*="result"]')) {
                    if (card.offsetHeight > 0 && /\d{1,2}:\d{2}/.test(card.innerText)) cards.push(card);
                }
            }

            for (const card of cards) {
                const text = card.innerText || '';
                if (text.length < 20) continue;

                const times = text.match(/\b(\d{1,2}:\d{2})\b/g) || [];
                if (times.length < 2) continue;

                // Extract price — SAR is most common
                const priceMatch = text.match(/(SAR|USD|EUR|GBP|\$)\s*[\d,]+\.?\d*/i) ||
                                   text.match(/[\d,]+\.?\d*\s*(SAR|USD|EUR|GBP)/i);
                if (!priceMatch) continue;

                const priceStr = priceMatch[0].replace(/[^0-9.]/g, '');
                const price = parseFloat(priceStr);
                if (!price || price <= 0) continue;

                let currency = 'SAR';
                if (/USD|\$/i.test(priceMatch[0])) currency = 'USD';
                else if (/EUR|€/i.test(priceMatch[0])) currency = 'EUR';

                const fnMatch = text.match(/\b(SV\s*\d{2,4})\b/i);
                const flightNo = fnMatch ? fnMatch[1].replace(/\s/g, '') : 'SV';

                let durationMin = 0;
                const durMatch = text.match(/(\d+)\s*h(?:rs?)?\s*(\d+)?\s*m/i);
                if (durMatch) durationMin = parseInt(durMatch[1]) * 60 + parseInt(durMatch[2] || 0);

                const nonstop = /non.?stop|direct/i.test(text);
                const stopsMatch = text.match(/(\d+)\s*stop/i);

                results.push({
                    depTime: times[0], arrTime: times[1],
                    price, currency, flightNo, durationMin,
                    stops: nonstop ? 0 : (stopsMatch ? parseInt(stopsMatch[1]) : 0),
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

    def _build_dom_offer(self, flight: dict, req: FlightSearchRequest) -> Optional[FlightOffer]:
        """Build FlightOffer from DOM-scraped data."""
        price = flight.get("price", 0)
        if price <= 0:
            return None

        try:
            dt = req.date_from if isinstance(req.date_from, (datetime, date)) else datetime.strptime(str(req.date_from), "%Y-%m-%d")
            dep_date = dt.date() if isinstance(dt, datetime) else dt if isinstance(dt, date) else date.today()
        except (ValueError, TypeError):
            dep_date = date.today()

        dep_time = flight.get("depTime", "00:00")
        arr_time = flight.get("arrTime", "00:00")

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

        flight_no = flight.get("flightNo", "SV")
        currency = flight.get("currency", "SAR")

        offer_id = hashlib.md5(
            f"sv_{req.origin}_{req.destination}_{dep_date}_{flight_no}_{price}".encode()
        ).hexdigest()[:12]
        _sv_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")

        segment = FlightSegment(
            airline="SV",
            airline_name="Saudia",
            flight_no=flight_no,
            origin=req.origin,
            destination=req.destination,
            departure=dep_dt,
            arrival=arr_dt,
            duration_seconds=flight.get("durationMin", 0) * 60,
            cabin_class=_sv_cabin,
        )

        route = FlightRoute(
            segments=[segment],
            total_duration_seconds=flight.get("durationMin", 0) * 60,
            stopovers=flight.get("stops", 0),
        )

        return FlightOffer(
            id=f"sv_{offer_id}",
            price=round(price, 2),
            currency=currency,
            price_formatted=f"{currency} {price:,.0f}",
            outbound=route,
            inbound=None,
            airlines=["Saudia"],
            owner_airline="SV",
            booking_url=self._booking_url(req),
            is_locked=False,
            source="saudia_direct",
            source_tier="free",
        )

    @staticmethod
    def _booking_url(req: FlightSearchRequest) -> str:
        """Build Saudia deep-link booking URL.

        The URL points to saudia.com's Angular search page which auto-fills
        origin/destination/date so users can pick a fare and complete checkout.
        """
        try:
            dt = req.date_from
            date_str = dt.strftime("%Y-%m-%d") if hasattr(dt, "strftime") else str(dt)
        except Exception:
            date_str = ""
        adults = req.adults or 1
        children = getattr(req, "children", 0) or 0
        infants = getattr(req, "infants", 0) or 0
        url = (
            f"https://www.saudia.com/en-SA/booking?"
            f"origin={req.origin}&destination={req.destination}"
            f"&departureDate={date_str}"
            f"&adults={adults}&children={children}&infants={infants}"
            f"&tripType={'RoundTrip' if req.return_from else 'OneWay'}&lang=en-SA"
        )
        if req.return_from:
            try:
                ret_str = req.return_from.strftime("%Y-%m-%d") if hasattr(req.return_from, "strftime") else str(req.return_from)
            except Exception:
                ret_str = ""
            url += f"&returnDate={ret_str}"
        return url

    async def _fetch_ancillaries(
        self, origin: str, dest: str, date_str: str, adults: int, currency: str
    ) -> dict | None:
        # Saudia SV — Economy: 23 kg checked bag included; cabin bag + personal item included
        return {
            "checked_bag_note": "23 kg checked bag included",
            "bags_note": "cabin bag + personal item included",
            "seat_note": "seat selection add-on from ~30 SAR",
            "currency": "SAR",
        }

    def _apply_ancillaries(self, offers: list, ancillary: dict) -> None:
        checked_bag_note = ancillary.get("checked_bag_note")
        bags_note = ancillary.get("bags_note")
        seat_note = ancillary.get("seat_note")
        bags_from = ancillary.get("bags_from")
        anc_currency = ancillary.get("currency", "EUR")
        for offer in offers:
            if checked_bag_note:
                offer.conditions["checked_bag"] = checked_bag_note
            if bags_note:
                offer.conditions["carry_on"] = bags_note
            if seat_note:
                offer.conditions["seat"] = seat_note
            if bags_from == 0.0:
                offer.bags_price["carry_on"] = 0.0

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"saudia{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency="SAR",
            offers=[],
            total_results=0,
        )


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
                    id=f"rt_saud_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
