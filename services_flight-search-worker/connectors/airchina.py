"""
Air China (CA) — CDP Chrome connector — form fill + API intercept.

Air China's website at airchina.com.cn uses a search widget with autocomplete
airport fields and calendar date picker. Direct API calls are blocked;
headed CDP Chrome with form fill + API interception is required.

IMPORTANT: Air China uses visual object-click captcha for bot detection.
For headless/backend operation, set OPENAI_API_KEY to enable automatic
captcha solving via GPT-4o-mini vision (~$0.0001/solve).

Strategy (CDP Chrome + API interception):
1. Launch headed Chrome via CDP (off-screen, stealth).
2. Navigate to airchina.com.cn → SPA loads with search widget.
3. Accept cookies → set one-way → fill origin/dest → select date → search.
4. Intercept the search API response (flight availability JSON).
5. If API not captured or blocked, fall back to DOM scraping on results page.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
import random
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

_DEBUG_PORT = 9491
_USER_DATA_DIR = os.path.join(
    os.environ.get("TEMP", os.environ.get("TMPDIR", "/tmp")), ".airchina_chrome_data"
)


# ── GPT-4o-mini Vision Captcha Solver ──
async def _solve_captcha_via_backend(img_b64: str, captcha_text: str, api_key: str, ref_b64: str = None) -> Optional[tuple]:
    """Call LetsFG captcha solver service. Returns (x_pct, y_pct) or None."""
    import httpx
    base_url = os.environ.get("LETSFG_CAPTCHA_URL", "https://captcha.letsfg.co")
    try:
        payload = {"image_b64": img_b64, "instruction": captcha_text}
        if ref_b64:
            payload["reference_b64"] = ref_b64
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"{base_url}/api/v1/captcha/solve",
                headers={"X-API-Key": api_key, "Content-Type": "application/json"},
                json=payload
            )
            if resp.status_code != 200:
                logger.debug("Backend captcha solve returned %s: %s", resp.status_code, resp.text[:200])
                return None
            data = resp.json()
            return (float(data["x"]), float(data["y"]))
    except Exception as e:
        logger.debug("Backend captcha solve failed: %s", e)
        return None


async def _solve_captcha_via_gemini(img_b64: str, prompt_text: str, gemini_key: str, ref_b64: str = None) -> Optional[tuple]:
    """Call Gemini 3.1 Flash-Lite directly. Returns (x_pct, y_pct) or None."""
    import httpx
    try:
        parts = [
            {"text": prompt_text},
            {"inline_data": {"mime_type": "image/jpeg", "data": img_b64}},
        ]
        if ref_b64:
            parts.append({"inline_data": {"mime_type": "image/jpeg", "data": ref_b64}})
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/gemini-3.1-flash-lite-preview:generateContent?key={gemini_key}",
                headers={"Content-Type": "application/json"},
                json={
                    "contents": [{"parts": parts}],
                    "generationConfig": {"maxOutputTokens": 50}
                }
            )
            if resp.status_code != 200:
                return None
            content = (resp.json().get("candidates", [{}])[0]
                       .get("content", {}).get("parts", [{}])[0].get("text", ""))
            m = re.search(r'\{[^}]*"x"\s*:\s*([\d.]+)[^}]*"y"\s*:\s*([\d.]+)[^}]*\}', content)
            return (float(m.group(1)), float(m.group(2))) if m else None
    except Exception as e:
        logger.debug("Gemini captcha solve failed: %s", e)
        return None


async def _solve_captcha_via_openai(img_b64: str, prompt_text: str, openai_key: str, ref_b64: str = None) -> Optional[tuple]:
    """Call GPT-4o-mini directly. Returns (x_pct, y_pct) or None."""
    import httpx
    try:
        content_parts = [
            {"type": "text", "text": prompt_text},
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}},
        ]
        if ref_b64:
            content_parts.append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{ref_b64}"}})
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {openai_key}", "Content-Type": "application/json"},
                json={
                    "model": "gpt-4o-mini",
                    "messages": [{"role": "user", "content": content_parts}],
                    "max_tokens": 50
                }
            )
            if resp.status_code != 200:
                return None
            content = resp.json().get("choices", [{}])[0].get("message", {}).get("content", "")
            m = re.search(r'\{[^}]*"x"\s*:\s*([\d.]+)[^}]*"y"\s*:\s*([\d.]+)[^}]*\}', content)
            return (float(m.group(1)), float(m.group(2))) if m else None
    except Exception as e:
        logger.debug("OpenAI captcha solve failed: %s", e)
        return None


async def _solve_captcha_with_llm(page) -> bool:
    """
    Solve visual click captcha using LLM vision.
    Priority: 1) LetsFG backend (zero config) → 2) Gemini direct → 3) OpenAI direct.
    Users need NO extra API keys — backend proxies the LLM call using our Gemini key.
    """
    letsfg_key = os.environ.get("LETSFG_API_KEY")
    gemini_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    openai_key = os.environ.get("OPENAI_API_KEY")

    if not letsfg_key and not gemini_key and not openai_key:
        logger.warning("No LETSFG_API_KEY, GEMINI_API_KEY, or OPENAI_API_KEY — cannot auto-solve captcha")
        return False

    try:
        import httpx  # noqa: F811
    except ImportError:
        logger.warning("httpx not installed — cannot solve captcha")
        return False

    for attempt in range(5):
        try:
            # Air China uses a specific captcha modal with known structure:
            #   div.captcha_drop (full-page overlay, position:fixed)
            #   div#captcha_modal (the visible dialog, position:absolute)
            #     div.captcha_header ("安全验证")
            #     div.captcha_body > img (the captcha image, base64 JPG)
            #     div.captcha_footer ("请点击上图中的：" + reference img)

            # Wait for captcha image to fully load (it loads async)
            await asyncio.sleep(2.0)

            # Get captcha image directly from DOM — select the LARGEST image 
            # (there are small icons/spinners in the modal too)
            captcha_info = await page.evaluate("""() => {
                // Find all images in the captcha area
                const imgs = document.querySelectorAll('.captcha_body img, #captcha_modal img, .captcha_drop img');
                let bestImg = null;
                let bestArea = 0;
                for (const img of imgs) {
                    const area = img.naturalWidth * img.naturalHeight;
                    if (area > bestArea && img.src) {
                        bestArea = area;
                        bestImg = img;
                    }
                }
                if (!bestImg || bestArea < 5000) return null;  // Must be at least ~70x70
                
                const r = bestImg.getBoundingClientRect();
                // Get instruction text and reference image from footer
                const footer = document.querySelector('.captcha_footer');
                let instruction = '';
                let refSrc = '';
                if (footer) {
                    instruction = footer.textContent.trim();
                    const refImg = footer.querySelector('img');
                    if (refImg) refSrc = refImg.src || '';
                }
                const src = bestImg.src || '';
                return {
                    src: src,
                    srcLen: src.length,
                    x: r.x, y: r.y, w: r.width, h: r.height,
                    instruction: instruction,
                    refSrc: refSrc,
                    refSrcLen: (refSrc || '').length,
                    naturalWidth: bestImg.naturalWidth,
                    naturalHeight: bestImg.naturalHeight,
                    complete: bestImg.complete,
                };
            }""")

            if not captcha_info:
                logger.warning("AirChina: captcha image element not found")
                return False

            logger.info("AirChina: captcha image src length=%d, natural=%dx%d, complete=%s, refLen=%d",
                        captcha_info.get('srcLen', 0),
                        captcha_info.get('naturalWidth', 0), captcha_info.get('naturalHeight', 0),
                        captcha_info.get('complete'), captcha_info.get('refSrcLen', 0))

            # If image not loaded yet, wait and retry
            if not captcha_info.get('complete') or captcha_info.get('srcLen', 0) < 1000:
                logger.info("AirChina: captcha image not fully loaded, waiting...")
                await asyncio.sleep(3.0)
                captcha_info = await page.evaluate("""() => {
                    const imgs = document.querySelectorAll('.captcha_body img, #captcha_modal img, .captcha_drop img');
                    let bestImg = null;
                    let bestArea = 0;
                    for (const img of imgs) {
                        const area = img.naturalWidth * img.naturalHeight;
                        if (area > bestArea && img.src) { bestArea = area; bestImg = img; }
                    }
                    if (!bestImg || bestArea < 5000) return null;
                    const r = bestImg.getBoundingClientRect();
                    const footer = document.querySelector('.captcha_footer');
                    let instruction = '';
                    let refSrc = '';
                    if (footer) {
                        instruction = footer.textContent.trim();
                        const refImg = footer.querySelector('img');
                        if (refImg) refSrc = refImg.src || '';
                    }
                    return {
                        src: bestImg.src || '',
                        srcLen: (bestImg.src || '').length,
                        x: r.x, y: r.y, w: r.width, h: r.height,
                        instruction: instruction,
                        refSrc: refSrc,
                        refSrcLen: (refSrc || '').length,
                        naturalWidth: bestImg.naturalWidth,
                        naturalHeight: bestImg.naturalHeight,
                        complete: bestImg.complete,
                    };
                }""")
                if not captcha_info:
                    return False
                logger.info("AirChina: retry - src length=%d, complete=%s",
                            captcha_info.get('srcLen', 0), captcha_info.get('complete'))

            # Extract base64 image data from the src attribute
            img_src = captcha_info["src"]
            if img_src.startswith("data:image"):
                # data:image/jpg;base64,/9j/4AAQ...
                img_b64 = img_src.split(",", 1)[1]
            else:
                logger.warning("AirChina: captcha image is not base64: %s", img_src[:60])
                return False

            # Extract reference image (the small target image showing WHICH object to click)
            ref_b64 = None
            ref_src = captcha_info.get("refSrc", "")
            if ref_src and ref_src.startswith("data:image"):
                ref_b64 = ref_src.split(",", 1)[1]

            logger.info("AirChina: captcha image b64 size=%d, ref b64 size=%d",
                        len(img_b64), len(ref_b64) if ref_b64 else 0)

            # Build instruction text including reference image
            captcha_text = captcha_info.get("instruction", "")
            if not captcha_text:
                captcha_text = "请点击图中的目标物体"

            if ref_b64:
                prompt_text = (
                    f'This is a visual click captcha from Air China.\n'
                    f'IMAGE 1 (first image): The main captcha scene showing multiple objects.\n'
                    f'IMAGE 2 (second image): A small reference image showing the TARGET object to click.\n\n'
                    f'The instruction says: "{captcha_text}"\n\n'
                    f'Find the object in IMAGE 1 that matches IMAGE 2 (the reference/target).\n'
                    f'Return the click coordinates within IMAGE 1 as a JSON object:\n'
                    f'{{"x": 0.XX, "y": 0.YY}}\n'
                    f'where x,y are percentages of IMAGE 1 width/height (0.0 to 1.0).\n'
                    f'Example: center = {{"x": 0.5, "y": 0.5}}'
                )
            else:
                prompt_text = (
                    f'This is a visual captcha from Air China\'s website.\n'
                    f'The instruction says: "{captcha_text}"\n\n'
                    f'Look at the image and identify where to click to solve this captcha.\n'
                    f'Return ONLY a JSON object with the x,y coordinates '
                    f'(as percentage of image width/height from top-left):\n'
                    f'{{"x": 0.XX, "y": 0.YY}}\n\n'
                    f'For example, if the target is in the center, return: {{"x": 0.5, "y": 0.5}}'
                )

            # Try solvers in priority order
            coords = None
            if letsfg_key and not coords:
                logger.info("AirChina: Solving captcha via LetsFG backend (attempt %d): %s", attempt + 1, captcha_text)
                coords = await _solve_captcha_via_backend(img_b64, captcha_text, letsfg_key, ref_b64)
            if gemini_key and not coords:
                logger.info("AirChina: Solving captcha via Gemini (attempt %d): %s", attempt + 1, captcha_text)
                coords = await _solve_captcha_via_gemini(img_b64, prompt_text, gemini_key, ref_b64)
            if openai_key and not coords:
                logger.info("AirChina: Solving captcha via OpenAI (attempt %d): %s", attempt + 1, captcha_text)
                coords = await _solve_captcha_via_openai(img_b64, prompt_text, openai_key, ref_b64)

            if not coords:
                logger.warning("AirChina: All captcha solvers failed (attempt %d), retrying...", attempt + 1)
                await asyncio.sleep(2.0)
                continue

            x_pct, y_pct = coords

            # Map coordinates to the captcha IMAGE bounds (not modal, not page)
            img_box = captcha_info
            click_x = img_box['x'] + img_box['w'] * x_pct
            click_y = img_box['y'] + img_box['h'] * y_pct

            logger.info("AirChina: clicking captcha at (%.0f, %.0f) — image at (%d,%d %dx%d), pct=(%.2f,%.2f)",
                        click_x, click_y, int(img_box['x']), int(img_box['y']),
                        int(img_box['w']), int(img_box['h']), x_pct, y_pct)
            await page.mouse.click(click_x, click_y)
            await asyncio.sleep(2.5)

            still_captcha = await page.evaluate("""() => document.body.innerText.includes('安全验证')""")
            if not still_captcha:
                logger.info("AirChina: Captcha solved on attempt %d!", attempt + 1)
                return True
            else:
                logger.warning("AirChina: Captcha click missed (attempt %d), retrying...", attempt + 1)
                # Captcha refreshes after wrong click, wait for new image
                await asyncio.sleep(1.5)

        except Exception as e:
            logger.warning("AirChina: LLM captcha solve error (attempt %d): %s", attempt + 1, e)

    logger.warning("AirChina: Captcha not solved after 5 attempts")
    return False

_browser = None
_context = None
_pw_instance = None
_chrome_proc = None
_browser_lock: Optional[asyncio.Lock] = None


# ── Human-like behavior helpers ──
def _bezier_curve(p0: tuple, p1: tuple, p2: tuple, p3: tuple, steps: int = 30) -> list:
    """Generate points along cubic bezier curve for human-like mouse movement."""
    pts = []
    for i in range(steps + 1):
        t = i / steps
        s = 1 - t
        x = s**3 * p0[0] + 3*s**2*t * p1[0] + 3*s*t**2 * p2[0] + t**3 * p3[0]
        y = s**3 * p0[1] + 3*s**2*t * p1[1] + 3*s*t**2 * p2[1] + t**3 * p3[1]
        pts.append((x, y))
    return pts


async def _human_mouse_move(page, start_x: float, start_y: float, end_x: float, end_y: float):
    """Move mouse from start to end using bezier curve with micro-variations."""
    dx = end_x - start_x
    dy = end_y - start_y
    ctrl1 = (start_x + dx * random.uniform(0.2, 0.4), start_y + dy * random.uniform(-0.3, 0.3))
    ctrl2 = (start_x + dx * random.uniform(0.6, 0.8), end_y + random.uniform(-15, 15))
    pts = _bezier_curve((start_x, start_y), ctrl1, ctrl2, (end_x, end_y), steps=random.randint(20, 35))
    for px, py in pts:
        px += random.uniform(-1.5, 1.5)
        py += random.uniform(-1.5, 1.5)
        await page.mouse.move(px, py)
        await asyncio.sleep(random.uniform(0.003, 0.010))


async def _human_click(page, x: float, y: float, start_x: float = 100, start_y: float = 100):
    """Human-like click: move mouse with bezier curve, then click."""
    await _human_mouse_move(page, start_x, start_y, x, y)
    await asyncio.sleep(random.uniform(0.05, 0.15))
    await page.mouse.click(x, y)
    await asyncio.sleep(random.uniform(0.1, 0.3))


async def _human_type(page, text: str, slow: bool = False):
    """Type text with human-like delays between keys."""
    for char in text:
        await page.keyboard.type(char, delay=0)
        delay = random.uniform(0.08, 0.18) if slow else random.uniform(0.05, 0.12)
        await asyncio.sleep(delay)


async def _random_scroll(page):
    """Random small scroll to simulate human behavior."""
    await page.mouse.wheel(0, random.randint(-50, 150))
    await asyncio.sleep(random.uniform(0.2, 0.5))


async def _click_calendar_date(page, target_day: int) -> bool:
    """Click on a specific day in the calendar to change date without URL navigation."""
    try:
        # Find and click the date picker to open calendar
        date_input = await page.query_selector('input[name="DepartureDate"], input#DepartureDate, .date-picker input')
        if not date_input:
            # Try clicking on visible date element
            date_el = await page.query_selector('.travel-date, .departure-date, [class*="date"]')
            if date_el:
                box = await date_el.bounding_box()
                if box:
                    await _human_click(page, box['x'] + box['width'] / 2, box['y'] + box['height'] / 2)
                    await asyncio.sleep(0.5)
        else:
            await date_input.click()
            await asyncio.sleep(0.5)
        
        # Look for day cells in calendar
        # Air China uses various calendar implementations
        day_selectors = [
            f'td[data-day="{target_day}"]',
            f'.calendar-day:has-text("{target_day}")',
            f'.ant-calendar-cell:has-text("{target_day}")',
            f'.day-cell:has-text("{target_day}")',
        ]
        
        for selector in day_selectors:
            try:
                day_el = await page.query_selector(selector)
                if day_el:
                    box = await day_el.bounding_box()
                    if box:
                        logger.info(f"AirChina: clicking calendar day {target_day}")
                        await _human_click(page, box['x'] + box['width'] / 2, box['y'] + box['height'] / 2)
                        await asyncio.sleep(1.0)
                        return True
            except Exception:
                continue
        
        # Fallback: find any clickable element containing the day number
        all_cells = await page.query_selector_all('td, div.day, span.day, button')
        for cell in all_cells:
            try:
                text = await cell.inner_text()
                if text.strip() == str(target_day):
                    box = await cell.bounding_box()
                    if box and box['width'] > 10 and box['height'] > 10:
                        logger.info(f"AirChina: found day {target_day} cell, clicking")
                        await _human_click(page, box['x'] + box['width'] / 2, box['y'] + box['height'] / 2)
                        await asyncio.sleep(1.0)
                        return True
            except Exception:
                continue
        
        logger.warning(f"AirChina: could not find calendar day {target_day}")
        return False
        
    except Exception as e:
        logger.warning(f"AirChina: calendar click failed: {e}")
        return False


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
            logger.info("AirChina: connected to existing Chrome on port %d", _DEBUG_PORT)
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
            logger.info("AirChina: Chrome launched on CDP port %d", _DEBUG_PORT)

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
        await page.evaluate("""() => {
            document.querySelectorAll(
                '#onetrust-consent-sdk, .cookie-banner, [class*="cookie"], [class*="consent"]'
            ).forEach(el => el.remove());
            const btns = document.querySelectorAll('button');
            for (const b of btns) {
                const t = b.textContent.trim().toLowerCase();
                if (t.includes('accept') || t.includes('agree') || t.includes('got it') || t.includes('ok')) {
                    if (b.offsetHeight > 0) { b.click(); break; }
                }
            }
        }""")
    except Exception:
        pass


class AirChinaConnectorClient:
    """Air China (CA) CDP Chrome connector."""

    IATA = "CA"
    AIRLINE_NAME = "Air China"
    SOURCE = "airchina_direct"
    HOMEPAGE = "https://www.airchina.com.cn"
    HOMEPAGE_FALLBACKS = [
        "http://www.airchina.com",
        "https://www.airchina.com",
    ]
    DEFAULT_CURRENCY = "CNY"

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
        
        # Reuse existing page if available (preserves session/cookies, avoids captcha)
        existing_pages = context.pages
        if existing_pages:
            page = existing_pages[0]
            logger.debug("AirChina: reusing existing page")
        else:
            page = await context.new_page()
            logger.debug("AirChina: created new page")
        
        await auto_block_if_proxied(page)

        # Apply stealth to evade bot detection
        try:
            from playwright_stealth import Stealth
            stealth = Stealth()
            await stealth.apply_stealth_async(page)
            logger.debug("AirChina: stealth applied")
        except ImportError:
            logger.debug("AirChina: playwright_stealth not available")
        except Exception as e:
            logger.debug("AirChina: stealth error: %s", e)

        search_data: dict = {}
        api_event = asyncio.Event()

        async def _on_response(response):
            url = response.url.lower()
            if response.status not in (200, 201):
                return
            try:
                # Air China uses /gateway/api/flight/list for search results
                # Also capture other common flight API patterns
                if any(k in url for k in ["/gateway/api/flight/list", "/flight/list", "/search",
                                           "/availability", "/offer", "/fare", "/lowprice", "/schedule"]):
                    ct = response.headers.get("content-type", "")
                    if "json" not in ct and "text" not in ct:
                        return
                    body = await response.text()
                    if len(body) < 50:
                        return
                    data = json.loads(body)
                    if not isinstance(data, dict):
                        return
                    # Air China response has Data.Journeys structure
                    if "Data" in data and isinstance(data.get("Data"), dict):
                        if "Journeys" in data["Data"]:
                            search_data.update(data)
                            api_event.set()
                            logger.info("AirChina: captured flight/list API → %d journeys", len(data["Data"]["Journeys"]))
                            return
                    # Fallback: check for common flight data keys
                    keys_str = " ".join(str(k).lower() for k in data.keys())
                    if any(k in keys_str for k in ["flight", "itiner", "offer", "fare",
                                                     "bound", "trip", "result", "segment",
                                                     "avail", "journey", "price"]):
                        search_data.update(data)
                        api_event.set()
                        logger.info("AirChina: captured API → %s (%d keys)", url[:80], len(data))
            except Exception:
                pass

        page.on("response", _on_response)
        
        needs_form_fill = True  # Flag to track if we need form fill

        try:
            # Format search date
            try:
                dt = req.date_from if isinstance(req.date_from, (datetime, date)) else datetime.strptime(str(req.date_from), "%Y-%m-%d")
                date_str = dt.strftime("%Y-%m-%d") if isinstance(dt, datetime) else str(dt)
                target_day = dt.day if isinstance(dt, datetime) else int(str(dt).split("-")[-1])
            except (ValueError, TypeError):
                date_str = str(req.date_from)
                target_day = int(date_str.split("-")[-1])
            
            current_url = page.url
            target_route = f"{req.origin.lower()}-{req.destination.lower()}"
            
            # Check if we're already on a search results page (avoids navigation = avoids captcha)
            if "airchina.com.cn/flight" in current_url and not current_url.endswith("about:blank"):
                logger.info("AirChina: already on search page, checking if we can use calendar click")
                
                # Check page state without navigation
                page_state = await page.evaluate("""() => {
                    const text = document.body ? document.body.innerText : '';
                    const hasCaptcha = text.includes('安全验证') || text.includes('请点击');
                    const hasFlightNumbers = /CA\\d{3,4}/.test(text);
                    const hasPrices = /[￥¥]\\d{3,}/.test(text);
                    return { hasCaptcha, hasFlights: hasFlightNumbers && hasPrices };
                }""")
                
                if not page_state['hasCaptcha'] and page_state['hasFlights']:
                    # We have a working session! Check if route matches
                    if target_route in current_url.lower():
                        logger.info("AirChina: session active, using calendar click for date change")
                        # Click on calendar date instead of navigating
                        clicked = await _click_calendar_date(page, target_day)
                        if clicked:
                            needs_form_fill = False
                            await asyncio.sleep(3.0)  # Wait for results to load
                            
                            # Scrape results
                            offers = await self._scrape_dom(page, req)
                            offers.sort(key=lambda o: o.price)
                            elapsed = time.monotonic() - t0
                            logger.info("AirChina %s→%s: %d offers in %.1fs (calendar click)", 
                                       req.origin, req.destination, len(offers), elapsed)
                            
                            search_hash = hashlib.md5(
                                f"airchina{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
                            ).hexdigest()[:12]
                            currency = offers[0].currency if offers else self.DEFAULT_CURRENCY
                            return FlightSearchResponse(
                                search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
                                currency=currency, offers=offers, total_results=len(offers),
                            )
                    else:
                        logger.info("AirChina: different route requested, need form fill")
            
            # Form fill approach - more reliable than direct URL (avoids captcha triggers)
            if needs_form_fill:
                loaded = False
                for url in [self.HOMEPAGE] + self.HOMEPAGE_FALLBACKS:
                    try:
                        await page.goto(url, wait_until="domcontentloaded", timeout=18000)
                        loaded = True
                        logger.info("AirChina: loaded %s", url)
                        break
                    except Exception:
                        logger.info("AirChina: %s unreachable, trying next", url)
                        try:
                            await page.goto("about:blank", wait_until="load", timeout=5000)
                        except Exception:
                            pass
                        await asyncio.sleep(0.5)
                if not loaded:
                    logger.warning("AirChina: all URLs unreachable")
                    return self._empty(req)
                await asyncio.sleep(random.uniform(2.0, 3.0))
                
                # Check for captcha on homepage
                has_captcha = await page.evaluate(
                    """() => document.body.innerText.includes('安全验证') || document.body.innerText.includes('请点击')"""
                )
                if has_captcha:
                    logger.warning("AirChina: CAPTCHA detected on homepage - attempting auto-solve...")
                    solved = await _solve_captcha_with_llm(page)
                    if not solved:
                        print("\n" + "="*60)
                        print("AIR CHINA CAPTCHA DETECTED")
                        print("Please solve the image captcha in the browser window.")
                        print("(Set OPENAI_API_KEY for auto-solve)")
                        print("="*60 + "\n")
                        
                        captcha_start = time.monotonic()
                        while time.monotonic() - captcha_start < 60:
                            still_has = await page.evaluate("""() => document.body.innerText.includes('安全验证')""")
                            if not still_has:
                                logger.info("AirChina: Captcha solved! Continuing...")
                                await asyncio.sleep(2.0)
                                break
                            await asyncio.sleep(2.0)
                        else:
                            logger.warning("AirChina: Captcha timeout after 60s")
                            return self._empty(req)
                
                # Human-like initial page interaction - simulate reading the page
                for _ in range(random.randint(1, 2)):
                    await _random_scroll(page)
                    await asyncio.sleep(random.uniform(0.3, 0.6))
            
            # Move mouse around randomly
            for _ in range(random.randint(3, 6)):
                x = random.randint(150, 800)
                y = random.randint(150, 600)
                await page.mouse.move(x, y, steps=random.randint(8, 15))
                await asyncio.sleep(random.uniform(0.2, 0.6))
            
            await _dismiss_overlays(page)
            await asyncio.sleep(random.uniform(0.5, 1.0))

            # One-way toggle
            await page.evaluate("""() => {
                const cssEls = document.querySelectorAll(
                    '[class*="one-way"], [class*="oneway"], input[value="OW"], ' +
                    'div[class*="trip-type"] label:nth-child(2), [data-value="OW"]'
                );
                for (const el of cssEls) {
                    if (el.offsetHeight > 0) { el.click(); return; }
                }
                const textEls = document.querySelectorAll('label, li, a, button, span, div, mat-radio-button');
                for (const el of textEls) {
                    const t = (el.textContent || '').trim();
                    const tl = t.toLowerCase();
                    if ((t === '单程' || tl === 'one way' || tl === 'one-way') && el.offsetHeight > 0) {
                        el.click(); return;
                    }
                }
            }""")
            await asyncio.sleep(0.5)

            ok = await self._fill_airport(page, "origin", req.origin)
            if not ok:
                return self._empty(req)
            await asyncio.sleep(random.uniform(0.4, 0.6))
            
            # Close any open dropdown before filling destination
            await page.keyboard.press("Escape")
            await asyncio.sleep(0.2)

            ok = await self._fill_airport(page, "destination", req.destination)
            if not ok:
                return self._empty(req)
            await asyncio.sleep(random.uniform(0.4, 0.6))
            
            # Close any open dropdown before clicking search
            await page.keyboard.press("Escape")
            await asyncio.sleep(0.2)
            
            # Verify airports are correctly set (debug)
            form_state = await page.evaluate("""() => {
                const inputs = [...document.querySelectorAll('input')].filter(i => i.offsetHeight > 0);
                const airportInputs = inputs.filter(i => {
                    const p = i.placeholder || '';
                    return (p.includes('机场') || p.includes('城市') || p.includes('三字码'));
                });
                return {
                    origin: airportInputs[0]?.value || '',
                    dest: airportInputs.length > 1 ? airportInputs[1]?.value : (airportInputs[0]?.value || ''),
                };
            }""")
            logger.info("AirChina: form verification - origin=%s dest=%s", 
                       form_state.get('origin', '?'), form_state.get('dest', '?'))
            await asyncio.sleep(random.uniform(0.3, 0.5))

            ok = await self._fill_date(page, req)
            if not ok:
                return self._empty(req)

            # Human-like pause before search
            await asyncio.sleep(random.uniform(0.5, 1.0))
            await _random_scroll(page)

            # Click search - try to use human mouse movement to the button
            search_btn_box = await page.evaluate("""() => {
                const acBtn = document.querySelector('.searchCore_searchButton__sxBzy, [class*="searchButton"]');
                if (acBtn && acBtn.offsetHeight > 0) {
                    const r = acBtn.getBoundingClientRect();
                    return {x: r.x + r.width/2, y: r.y + r.height/2};
                }
                const btns = document.querySelectorAll('button, input[type="submit"], a');
                for (const b of btns) {
                    const t = (b.textContent || b.value || '').trim().toLowerCase();
                    if ((t.includes('search') || t.includes('find') || t.includes('查询') || t.includes('搜索'))
                        && b.offsetHeight > 0) {
                        const r = b.getBoundingClientRect();
                        return {x: r.x + r.width/2, y: r.y + r.height/2};
                    }
                }
                return null;
            }""")
            
            # Click search - try Playwright locator first (most reliable), then human click, then JS
            search_clicked = False
            try:
                btn = page.locator('.searchCore_searchButton__sxBzy, [class*="searchButton"]').first
                if await btn.is_visible(timeout=3000):
                    await btn.click(timeout=5000)
                    search_clicked = True
                    logger.info("AirChina: search clicked via locator")
            except Exception as e:
                logger.debug("AirChina: locator click failed: %s", e)
            
            if not search_clicked and search_btn_box:
                # Use human mouse movement to click
                await _human_click(page, search_btn_box['x'], search_btn_box['y'], 
                                   random.randint(300, 600), random.randint(200, 400))
                search_clicked = True
            if not search_clicked:
                # Fallback to JS click
                await page.evaluate("""() => {
                    const acBtn = document.querySelector('.searchCore_searchButton__sxBzy, [class*="searchButton"]');
                    if (acBtn && acBtn.offsetHeight > 0) { acBtn.click(); return; }
                    const btns = document.querySelectorAll('button, input[type="submit"], a');
                    for (const b of btns) {
                        const t = (b.textContent || b.value || '').trim().toLowerCase();
                        if ((t.includes('search') || t.includes('find') || t.includes('查询') || t.includes('搜索'))
                            && b.offsetHeight > 0) { b.click(); return; }
                    }
                }""")
                
            logger.info("AirChina: search clicked")
            await asyncio.sleep(random.uniform(2.0, 3.0))  # Wait for page navigation

            # Check for error dialog
            error = await page.evaluate("""() => {
                const dialog = document.querySelector('[class*="dialog_content"], [class*="dialogContent"]');
                if (dialog && dialog.offsetWidth > 0) {
                    const text = dialog.textContent || '';
                    // Close the dialog
                    const close = document.querySelector('[class*="dialog_closeIcon"], [class*="closeIcon"]');
                    if (close) close.click();
                    const ok = document.querySelector('[class*="dialog_primaryBtn"], [class*="confirm"]');
                    if (ok) ok.click();
                    return text;
                }
                return null;
            }""")
            if error:
                logger.warning("AirChina: form validation error: %s", error[:100])
                return self._empty(req)

            remaining = max(self.timeout - (time.monotonic() - t0), 15)
            deadline = time.monotonic() + remaining
            while time.monotonic() < deadline:
                if api_event.is_set():
                    break
                url = page.url
                if any(k in url.lower() for k in ["result", "search", "flight", "availability"]):
                    await asyncio.sleep(4.0)
                    break
                await asyncio.sleep(1.0)

            if not api_event.is_set():
                try:
                    await asyncio.wait_for(api_event.wait(), timeout=8.0)
                except asyncio.TimeoutError:
                    pass

            # Check for captcha AFTER page has loaded (captcha appears after navigation)
            has_captcha = await page.evaluate("""() => {
                const text = (document.body && document.body.innerText) || '';
                return text.includes('安全验证') || text.includes('请点击');
            }""")
            if has_captcha:
                logger.warning("AirChina: CAPTCHA detected on results page — attempting auto-solve...")
                solved = await _solve_captcha_with_llm(page)
                if solved:
                    logger.info("AirChina: Captcha auto-solved! Waiting for results...")
                    # After captcha solve, wait for results to load.
                    # First try: just wait — the SPA may already have results behind captcha.
                    await asyncio.sleep(4.0)
                    has_data = await page.evaluate(r"""() => {
                        const text = document.body.innerText || '';
                        return /CA\d{3,4}/.test(text) && /[￥¥]\d{3,}/.test(text);
                    }""")
                    if has_data:
                        logger.info("AirChina: Flight data ready after captcha solve")
                    elif api_event.is_set():
                        logger.info("AirChina: API data captured after captcha solve")
                    else:
                        # If no data, try clicking the re-search button or reload
                        logger.info("AirChina: No data yet, clicking re-search button...")
                        try:
                            btn = page.locator("text=重新查询")
                            if await btn.count() > 0:
                                await btn.first.click()
                                logger.info("AirChina: Clicked re-search button")
                            else:
                                logger.info("AirChina: No re-search button, reloading...")
                                await page.reload(wait_until="domcontentloaded", timeout=30000)
                        except Exception:
                            await page.reload(wait_until="domcontentloaded", timeout=30000)
                        await asyncio.sleep(3.0)
                        # Wait for flight data after re-search/reload
                        for wait_round in range(6):  # Up to 18 seconds
                            await asyncio.sleep(3.0)
                            # Check if flight data appeared
                            has_data = await page.evaluate(r"""() => {
                                const text = document.body.innerText || '';
                                return /CA\d{3,4}/.test(text) && /[￥¥]\d{3,}/.test(text);
                            }""")
                            if has_data:
                                logger.info("AirChina: Flight data loaded after re-search (%ds)", (wait_round + 1) * 5)
                                break
                            # Check for second captcha
                            still_captcha = await page.evaluate("""() => {
                                return (document.body.innerText || '').includes('安全验证');
                            }""")
                            if still_captcha:
                                logger.warning("AirChina: Second captcha appeared, attempting solve...")
                                solved2 = await _solve_captcha_with_llm(page)
                                if not solved2:
                                    break
                            if api_event.is_set():
                                logger.info("AirChina: API data captured after re-search")
                                break
                else:
                    logger.warning("AirChina: Captcha auto-solve failed, falling back to manual wait...")
                    # Wait for manual solve
                    manual_start = time.monotonic()
                    while time.monotonic() - manual_start < 45:
                        still = await page.evaluate("""() => (document.body.innerText || '').includes('安全验证')""")
                        if not still:
                            logger.info("AirChina: Captcha resolved (manual), continuing...")
                            await asyncio.sleep(2.0)
                            break
                        await asyncio.sleep(2.0)

            offers = []
            if search_data:
                offers = self._parse_api_response(search_data, req)
            if not offers:
                offers = await self._scrape_dom(page, req)
            
            # Debug: save screenshot if no offers found
            if not offers:
                try:
                    debug_path = os.path.join(os.path.dirname(__file__), "_airchina_debug.png")
                    await page.screenshot(path=debug_path)
                    logger.info("AirChina: debug screenshot saved to %s", debug_path)
                    # Also log the page URL and title
                    logger.info("AirChina: final URL=%s title=%s", page.url, await page.title())
                except Exception as e:
                    logger.debug("AirChina: screenshot error: %s", e)

            offers.sort(key=lambda o: o.price)
            elapsed = time.monotonic() - t0
            logger.info("AirChina %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

            search_hash = hashlib.md5(
                f"airchina{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
            ).hexdigest()[:12]
            currency = offers[0].currency if offers else self.DEFAULT_CURRENCY
            return FlightSearchResponse(
                search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
                currency=currency, offers=offers, total_results=len(offers),
            )
        except Exception as e:
            logger.error("AirChina error: %s", e)
            return self._empty(req)
        finally:
            try:
                page.remove_listener("response", _on_response)
            except Exception:
                pass
            # Don't close page - preserve session for subsequent searches
            # This avoids re-triggering captcha on each search

    async def _fill_airport(self, page, direction: str, iata: str) -> bool:
        logger.debug("AirChina: _fill_airport called - direction=%s iata=%s", direction, iata)
        try:
            # Close any existing dropdowns first
            await page.evaluate("() => document.body.click()")
            await asyncio.sleep(0.3)
            
            # Air China has 2 airport inputs: origin (placeholder contains 出发机场)
            # and destination (placeholder 到达机场 — but changes to 输入城市或机场三字码 when active).
            # After origin selection, destination auto-focuses and placeholder changes.
            # Strategy: for origin, find by 出发机场; for destination, find by 到达 or use 2nd airport input.
            focused = await page.evaluate("""(args) => {
                const [direction, iata] = args;
                const inputs = [...document.querySelectorAll('input')].filter(i => i.offsetHeight > 0);
                
                // Find airport inputs (exclude date input which has 出发日期/日期 in placeholder)
                const airportInputs = inputs.filter(i => {
                    const p = i.placeholder || '';
                    return (p.includes('机场') || p.includes('城市') || p.includes('三字码'));
                });
                
                let target = null;
                if (direction === 'origin') {
                    // Origin: first airport input (usually placeholder=出发机场)
                    target = airportInputs.find(i => i.placeholder.includes('出发')) || airportInputs[0];
                } else {
                    // Destination: second airport input or one with 到达/城市/三字码
                    target = airportInputs.find(i => i.placeholder.includes('到达'));
                    if (!target) {
                        // After origin fill, dest has placeholder 输入城市或机场三字码
                        target = airportInputs.find(i => i.placeholder.includes('三字码') || i.placeholder.includes('城市'));
                    }
                    if (!target && airportInputs.length > 1) {
                        target = airportInputs[1];
                    }
                }
                
                if (!target) return {focused: false, count: airportInputs.length};
                
                // Check if already has correct value
                if (target.value && target.value.toUpperCase().includes(iata)) {
                    return {alreadyFilled: true, value: target.value};
                }
                target.focus();
                target.value = '';
                target.dispatchEvent(new Event('input', {bubbles: true}));
                return {focused: true, placeholder: target.placeholder};
            }""", [direction, iata])
            
            if focused.get('alreadyFilled'):
                logger.info("AirChina: %s already has %s", direction, iata)
                return True
            
            if not focused.get('focused'):
                logger.warning("AirChina: could not focus %s field (airport inputs: %d)", direction, focused.get('count', 0))
                return False
            
            logger.debug("AirChina: focused %s field (placeholder=%s)", direction, focused.get('placeholder'))
            await asyncio.sleep(random.uniform(0.3, 0.5))
            
            # Type the IATA code using keyboard (JS focus + keyboard type works with React)
            await page.keyboard.type(iata, delay=80)
            await asyncio.sleep(random.uniform(1.2, 1.8))

            # Select from dropdown - Air China uses airportItem class
            selected = await page.evaluate("""(iata) => {
                const items = document.querySelectorAll('[class*="airportItem"]');
                for (const item of items) {
                    if (item.textContent.includes(iata) && item.offsetHeight > 0) {
                        item.click();
                        return {selected: true, text: item.textContent.substring(0, 30)};
                    }
                }
                // Fallback: click first item
                if (items.length > 0 && items[0].offsetHeight > 0) {
                    items[0].click();
                    return {selected: true, text: items[0].textContent.substring(0, 30), fallback: true};
                }
                return {selected: false};
            }""", iata)
            
            if not selected.get('selected'):
                # Use keyboard navigation as fallback
                await asyncio.sleep(random.uniform(0.2, 0.4))
                await page.keyboard.press("ArrowDown")
                await asyncio.sleep(random.uniform(0.15, 0.3))
                await page.keyboard.press("Enter")

            await asyncio.sleep(random.uniform(0.3, 0.7))
            logger.info("AirChina: airport %s → %s (%s)", direction, iata, selected.get('text', '?'))
            return True
        except Exception as e:
            logger.warning("AirChina: airport fill error for %s: %s", iata, e)
            return False

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        try:
            dt = req.date_from if isinstance(req.date_from, (datetime, date)) else datetime.strptime(str(req.date_from), "%Y-%m-%d")
        except (ValueError, TypeError):
            return False
        target_day = str(dt.day)
        target_year = dt.year
        target_month = dt.month
        target_date_str = f"{target_year}-{target_month:02d}-{dt.day:02d}"
        
        try:
            # First check if date is already correct
            current_date = await page.evaluate("""() => {
                const inputs = document.querySelectorAll('input');
                for (const i of inputs) {
                    if (i.placeholder && i.placeholder.includes('日期') && i.offsetHeight > 0) {
                        return i.value || '';
                    }
                }
                return '';
            }""")
            if current_date == target_date_str:
                logger.info("AirChina: date already set to %s", target_date_str)
                return True
            
            # Click date input to open calendar (using JS focus to bypass overlays)
            await page.evaluate("""() => {
                const inputs = [...document.querySelectorAll('input')].filter(i => i.offsetHeight > 0);
                const dateInput = inputs.find(i => i.placeholder && i.placeholder.includes('日期'));
                if (dateInput) {
                    dateInput.click();
                    dateInput.focus();
                }
            }""")
            await asyncio.sleep(random.uniform(1.0, 1.5))

            # Calculate how many months to navigate forward
            # Air China calendar uses carousel-next-btn for month navigation
            now = datetime.now()
            current_month_val = now.year * 12 + now.month
            target_month_val = target_year * 12 + target_month
            months_to_advance = target_month_val - current_month_val
            
            logger.debug("AirChina: navigating %d months forward", months_to_advance)
            for _ in range(max(0, months_to_advance)):
                await page.evaluate("""() => {
                    const btn = document.querySelector('[class*="carousel-next-btn"]');
                    if (btn) btn.click();
                }""")
                await asyncio.sleep(random.uniform(0.35, 0.55))

            # Click the target day - find enabled (not disabled) td cells
            clicked = await page.evaluate("""(targetDay) => {
                const cells = [...document.querySelectorAll('td[class*="calendar"]')];
                const enabled = cells.filter(c => {
                    const cls = c.className || '';
                    return !cls.includes('Disabled') && c.offsetHeight > 0;
                });
                
                // Find cell with target day text
                const target = enabled.find(c => c.textContent.trim() === targetDay);
                if (target) {
                    target.click();
                    target.dispatchEvent(new MouseEvent('click', {bubbles: true}));
                    return {clicked: true, day: targetDay};
                }
                return {clicked: false, enabledCount: enabled.length};
            }""", target_day)
            
            if clicked.get('clicked'):
                logger.info("AirChina: date selected %s", dt.strftime("%Y-%m-%d"))
            else:
                logger.warning("AirChina: could not click day %s (enabled cells: %d)", 
                              target_day, clicked.get('enabledCount', 0))
            await asyncio.sleep(1.0)
            return True
        except Exception as e:
            logger.warning("AirChina: date error: %s", e)
            return False

    def _parse_api_response(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        offers = []
        
        # Air China uses Data.Journeys format from /gateway/api/flight/list
        if "Data" in data and isinstance(data["Data"], dict):
            journeys = data["Data"].get("Journeys", [])
            if journeys:
                logger.info("AirChina: parsing %d journeys from API", len(journeys))
                offers = self._parse_journeys(journeys, req)
                if offers:
                    return offers
        
        # Fallback to generic parsing
        flights = (
            data.get("flights") or data.get("results") or data.get("itineraries") or
            data.get("flightInfos") or data.get("offers") or data.get("journeys") or
            data.get("routeList") or data.get("flightList") or []
        )
        if isinstance(flights, dict):
            for key in ("flights", "results", "itineraries", "options", "list"):
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
                offers.append(offer)
        return offers

    def _parse_journeys(self, journeys: list, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse Air China's Journeys format from /gateway/api/flight/list."""
        offers = []
        for journey in journeys:
            try:
                # Extract segments
                segments_data = journey.get("Segments", [])
                if not segments_data:
                    continue
                
                segments = []
                for seg in segments_data:
                    dep_info = seg.get("Dep", {})
                    arr_info = seg.get("Arrival", {})
                    
                    dep_dt = self._parse_dt(dep_info.get("AircraftScheduledDateTime", ""), req.date_from)
                    arr_dt = self._parse_dt(arr_info.get("AircraftScheduledDateTime", ""), req.date_from)
                    
                    carrier = seg.get("MarketingCarrier", "") or seg.get("OperatingCarrier", "")
                    # Extract flight number from carrier string like "CA1507"
                    flight_no = carrier if carrier else self.IATA
                    airline_code = carrier[:2] if len(carrier) >= 2 else self.IATA
                    
                    segments.append(FlightSegment(
                        airline=airline_code,
                        airline_name=self.AIRLINE_NAME if airline_code == self.IATA else airline_code,
                        flight_no=flight_no,
                        origin=dep_info.get("IATA_LocationCode", req.origin),
                        destination=arr_info.get("IATA_LocationCode", req.destination),
                        departure=dep_dt,
                        arrival=arr_dt,
                        cabin_class="economy",
                    ))
                
                if not segments:
                    continue
                
                # Extract best price from Fares
                # Fares is a dict keyed by cabin type, each with Fare list
                fares_data = journey.get("Fares", {})
                best_price = None
                best_offer_id = None
                
                for cabin_key, cabin_data in fares_data.items():
                    if isinstance(cabin_data, dict):
                        # Check cabin-level Price (lowest in that cabin)
                        cabin_price = cabin_data.get("Price", 0)
                        if cabin_price and cabin_price > 0 and (best_price is None or cabin_price < best_price):
                            best_price = cabin_price
                        # Also check individual fares
                        fare_list = cabin_data.get("Fare", [])
                        for fare in fare_list:
                            if isinstance(fare, dict):
                                fare_price = fare.get("Price", 0)
                                if fare_price and fare_price > 0 and (best_price is None or fare_price < best_price):
                                    best_price = fare_price
                                    best_offer_id = fare.get("OfferID", "")
                
                if not best_price or best_price <= 0:
                    continue
                
                logger.debug("AirChina: journey %s → best_price=%s (cabins: %s)",
                            journey.get("JourneyRefID", "?"), best_price,
                            {k: v.get("Price") for k, v in fares_data.items() if isinstance(v, dict)})
                
                route = FlightRoute(
                    segments=segments,
                    total_duration_seconds=segments_data[0].get("Duration", 0) if segments_data else 0,
                    stopovers=max(0, len(segments) - 1),
                )
                
                offer_id = hashlib.md5(
                    f"{self.IATA.lower()}_{journey.get('JourneyRefID', '')}_{best_price}_{segments[0].flight_no}".encode()
                ).hexdigest()[:12]
                
                offers.append(FlightOffer(
                    id=f"{self.IATA.lower()}_{offer_id}",
                    price=round(float(best_price), 2),
                    currency=self.DEFAULT_CURRENCY,
                    price_formatted=f"CNY {best_price:,.0f}",
                    outbound=route,
                    inbound=None,
                    airlines=list({s.airline for s in segments}),
                    owner_airline=self.IATA,
                    booking_url=self._booking_url(req),
                    is_locked=False,
                    source=self.SOURCE,
                    source_tier="free",
                ))
            except Exception as e:
                logger.debug("AirChina: journey parse error: %s", e)
                continue
        
        return offers

    def _find_flights(self, data, depth=0) -> list:
        if depth > 4 or not isinstance(data, dict):
            return []
        for key, val in data.items():
            if isinstance(val, list) and len(val) > 0 and isinstance(val[0], dict):
                sample_keys = {str(k).lower() for k in val[0].keys()}
                if sample_keys & {"price", "fare", "flight", "departure", "segment", "leg"}:
                    return val
            elif isinstance(val, dict):
                result = self._find_flights(val, depth + 1)
                if result:
                    return result
        return []

    def _build_offer(self, flight: dict, req: FlightSearchRequest) -> Optional[FlightOffer]:
        try:
            price = (
                flight.get("price") or flight.get("totalPrice") or
                flight.get("fare") or flight.get("amount") or
                flight.get("adultPrice") or 0
            )
            if isinstance(price, dict):
                price = price.get("amount") or price.get("total") or price.get("value") or 0
            price = float(price) if price else 0
            if price <= 0:
                return None

            currency = self._extract_currency(flight)

            segments_data = flight.get("segments") or flight.get("legs") or flight.get("flights") or []
            if not isinstance(segments_data, list):
                segments_data = [flight]

            segments = []
            for seg in segments_data:
                dep_str = seg.get("departure") or seg.get("departureTime") or seg.get("depTime") or ""
                arr_str = seg.get("arrival") or seg.get("arrivalTime") or seg.get("arrTime") or ""
                dep_dt = self._parse_dt(dep_str, req.date_from)
                arr_dt = self._parse_dt(arr_str, req.date_from)
                airline_code = seg.get("airline") or seg.get("carrierCode") or seg.get("operatingCarrier") or self.IATA
                flight_no = seg.get("flightNumber") or seg.get("flightNo") or ""
                if flight_no and not flight_no.startswith(airline_code):
                    flight_no = f"{airline_code}{flight_no}"

                segments.append(FlightSegment(
                    airline=airline_code[:2], airline_name=self.AIRLINE_NAME if airline_code == self.IATA else airline_code,
                    flight_no=flight_no or self.IATA, origin=seg.get("origin") or seg.get("departureAirport") or req.origin,
                    destination=seg.get("destination") or seg.get("arrivalAirport") or req.destination,
                    departure=dep_dt, arrival=arr_dt, cabin_class="economy",
                ))

            if not segments:
                return None

            route = FlightRoute(segments=segments, total_duration_seconds=0, stopovers=max(0, len(segments) - 1))
            offer_id = hashlib.md5(
                f"{self.IATA.lower()}_{req.origin}_{req.destination}_{req.date_from}_{price}_{segments[0].flight_no}".encode()
            ).hexdigest()[:12]

            return FlightOffer(
                id=f"{self.IATA.lower()}_{offer_id}", price=round(price, 2), currency=currency,
                price_formatted=f"{currency} {price:,.0f}", outbound=route, inbound=None,
                airlines=list({s.airline for s in segments}), owner_airline=self.IATA,
                booking_url=self._booking_url(req), is_locked=False,
                source=self.SOURCE, source_tier="free",
            )
        except Exception as e:
            logger.debug("AirChina: offer parse error: %s", e)
            return None

    def _extract_currency(self, d: dict) -> str:
        for key in ("currency", "currencyCode"):
            val = d.get(key)
            if isinstance(val, str) and len(val) == 3:
                return val.upper()
        if isinstance(d.get("price"), dict):
            return d["price"].get("currency", self.DEFAULT_CURRENCY)
        return self.DEFAULT_CURRENCY

    @staticmethod
    def _parse_dt(s, fallback_date) -> datetime:
        if not s:
            try:
                dt = fallback_date if isinstance(fallback_date, (datetime, date)) else datetime.strptime(str(fallback_date), "%Y-%m-%d")
                return datetime(dt.year, dt.month, dt.day) if isinstance(dt, date) and not isinstance(dt, datetime) else dt
            except Exception:
                return datetime.now()
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                return datetime.strptime(s[:19], fmt)
            except (ValueError, TypeError):
                continue
        m = re.search(r"(\d{1,2}):(\d{2})", str(s))
        if m:
            try:
                dt = fallback_date if isinstance(fallback_date, (datetime, date)) else datetime.strptime(str(fallback_date), "%Y-%m-%d")
                d = dt if isinstance(dt, date) and not isinstance(dt, datetime) else dt.date() if isinstance(dt, datetime) else dt
                return datetime(d.year, d.month, d.day, int(m.group(1)), int(m.group(2)))
            except Exception:
                pass
        return datetime.now()

    async def _scrape_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        await asyncio.sleep(2)
        
        # Air China renders flights in a table/card format
        # Parse the visible flight data using body text parsing
        flights = await page.evaluate(r"""(params) => {
            const [origin, destination] = params;
            const results = [];
            const bodyText = document.body.innerText || '';
            
            // Air China flight pattern: CA1359\n33A\n07:00\n10:25\nPEK T3\nCAN T1\n3h25m\n...
            // Prices appear as multiple fare classes, with economy being the LAST price shown
            const lines = bodyText.split('\n').map(l => l.trim()).filter(l => l.length > 0);
            
            let i = 0;
            while (i < lines.length) {
                const line = lines[i];
                
                // Look for CA flight number
                const flightMatch = line.match(/^(CA\d{3,4})$/);
                if (flightMatch) {
                    const flightNo = flightMatch[1];
                    
                    // Scan next 25 lines for flight details
                    let depTime = null, arrTime = null, duration = null;
                    let prices = [];  // Collect ALL prices, economy is last
                    
                    for (let j = i + 1; j < Math.min(i + 25, lines.length); j++) {
                        const nextLine = lines[j];
                        
                        // Look for time pattern HH:MM
                        const timeMatch = nextLine.match(/^(\d{1,2}:\d{2})$/);
                        if (timeMatch) {
                            if (!depTime) {
                                depTime = timeMatch[1];
                            } else if (!arrTime) {
                                arrTime = timeMatch[1];
                            }
                        }
                        
                        // Look for duration pattern XhYm
                        const durMatch = nextLine.match(/^(\d+)h(\d+)m$/);
                        if (durMatch) {
                            duration = parseInt(durMatch[1]) * 60 + parseInt(durMatch[2]);
                        }
                        
                        // Collect ALL prices ￥XXX or ¥XXX (economy comes last)
                        const priceMatch = nextLine.match(/^[￥¥](\d+)$/);
                        if (priceMatch) {
                            prices.push(parseInt(priceMatch[1]));
                        }
                        
                        // Stop if we hit next flight number
                        if (nextLine.match(/^CA\d{3,4}$/)) break;
                    }
                    
                    // Use LAST price (economy class) as the main price
                    const economyPrice = prices.length > 0 ? prices[prices.length - 1] : 0;
                    
                    if (depTime && arrTime && economyPrice && economyPrice > 0) {
                        results.push({
                            flightNo: flightNo,
                            depTime: depTime,
                            arrTime: arrTime,
                            duration: duration || 0,
                            price: economyPrice,
                            currency: 'CNY'
                        });
                    }
                }
                i++;
            }
            
            return results;
        }""", [req.origin, req.destination])
        
        logger.info("AirChina DOM scraper found %d flights", len(flights or []))

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
        offer_id = hashlib.md5(f"{self.IATA.lower()}_{req.origin}_{req.destination}_{dep_date}_{flight_no}_{price}".encode()).hexdigest()[:12]

        segment = FlightSegment(
            airline=self.IATA, airline_name=self.AIRLINE_NAME, flight_no=flight_no,
            origin=req.origin, destination=req.destination, departure=dep_dt, arrival=arr_dt, cabin_class="economy",
        )
        route = FlightRoute(segments=[segment], total_duration_seconds=0, stopovers=0)
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
        return f"https://www.airchina.com/en/booking?from={req.origin}&to={req.destination}&date={date_str}"

    @staticmethod
    def _combine_rt(ob: list, ib: list, req) -> list:
        combos = []
        for o in sorted(ob, key=lambda x: x.price)[:15]:
            for i in sorted(ib, key=lambda x: x.price)[:10]:
                combos.append(FlightOffer(
                    id=f"ca_rt_{o.id}_{i.id}",
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
        search_hash = hashlib.md5(f"airchina{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
            currency=self.DEFAULT_CURRENCY, offers=[], total_results=0,
        )
