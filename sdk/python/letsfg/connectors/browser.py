"""
Shared browser launcher for LetsFG connectors.

Provides Chrome discovery, stealth CDP launch (off-screen, no-focus),
and Playwright helpers. All CDP Chrome connectors should use these
instead of rolling their own launch logic.

Environment variables:
    CHROME_PATH                — Override Chrome executable path.
    BOOSTED_BROWSER_VISIBLE    — Set to "1" to show browser windows (debugging).
    LETSFG_MAX_BROWSERS — Max concurrent browser processes (default: auto-detect).
    LETSFG_PROXY               — Global residential proxy URL for all connectors.
                                 Format: http://user:pass@host:port
                                 When set, all browser launches and HTTP clients
                                 route through this proxy. Per-connector env vars
                                 (e.g. ALLEGIANT_PROXY) override this.
    LETSFG_PROXY_PORT_RANGE    — Optional port range for round-robin rotation.
                                 Format: "10001-10010"
                                 Each proxy call picks the next port in sequence,
                                 spreading load across multiple proxy endpoints.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
import os
import platform
import shutil
import subprocess
from typing import Optional
from urllib.parse import urlparse, urlunparse

logger = logging.getLogger(__name__)

# ── Concurrency gate — limits how many browsers can run at once ──────────────
# Without this, 20+ Chrome processes spawn simultaneously and crash the machine.
# Value is set by configure_max_browsers() or auto-detected on first use.
_max_concurrent_browsers: int | None = None
_browser_semaphore: Optional[asyncio.Semaphore] = None


def _resolve_max_browsers() -> int:
    """Determine max concurrent browsers: env var > explicit config > auto-detect."""
    global _max_concurrent_browsers

    # 1. Env var override (highest priority)
    env_val = os.environ.get("LETSFG_MAX_BROWSERS")
    if env_val:
        try:
            n = int(env_val)
            if 1 <= n <= 32:
                return n
        except ValueError:
            pass

    # 2. Explicitly configured via configure_max_browsers()
    if _max_concurrent_browsers is not None:
        return _max_concurrent_browsers

    # 3. Auto-detect from system resources
    try:
        from letsfg.system_info import get_system_profile
        profile = get_system_profile()
        recommended = profile["recommended_max_browsers"]
        logger.info(
            "Auto-detected system: %.1f GB RAM available, %s tier → max %d browsers",
            profile.get("ram_available_gb") or profile.get("ram_total_gb") or 0,
            profile["tier"],
            recommended,
        )
        return recommended
    except Exception:
        return 8  # safe fallback


def configure_max_browsers(n: int) -> None:
    """Set the maximum number of concurrent browser processes.

    Call this BEFORE starting a search to override auto-detection.
    Values are clamped to 1–32.

    Args:
        n: Max concurrent browsers (1=sequential, 16=aggressive).
    """
    global _max_concurrent_browsers, _browser_semaphore
    _max_concurrent_browsers = max(1, min(32, n))
    # Reset semaphore so next acquire picks up the new value
    _browser_semaphore = None
    logger.info("Browser concurrency set to %d", _max_concurrent_browsers)


def get_max_browsers() -> int:
    """Return the current max concurrent browsers setting."""
    return _resolve_max_browsers()


async def _get_browser_semaphore() -> asyncio.Semaphore:
    """Get or create the global browser concurrency semaphore (lazy init)."""
    global _browser_semaphore
    if _browser_semaphore is None:
        _browser_semaphore = asyncio.Semaphore(_resolve_max_browsers())
    return _browser_semaphore


async def acquire_browser_slot():
    """Acquire a browser slot — blocks if too many browsers are running."""
    sem = await _get_browser_semaphore()
    max_val = _resolve_max_browsers()
    # _value is the internal counter: how many more can acquire without blocking
    available = getattr(sem, '_value', '?')
    logger.debug("Browser slot ACQUIRE requested (available=%s/%d)", available, max_val)
    await sem.acquire()
    available = getattr(sem, '_value', '?')
    logger.debug("Browser slot ACQUIRED (available now=%s/%d)", available, max_val)


def release_browser_slot():
    """Release a browser slot after a connector finishes with its browser."""
    if _browser_semaphore is not None:
        _browser_semaphore.release()
        available = getattr(_browser_semaphore, '_value', '?')
        max_val = _max_concurrent_browsers or '?'
        logger.debug("Browser slot RELEASED (available now=%s/%s)", available, max_val)


# ── Cleanup registry — tracks resources launched by connectors ───────────────
_launched_procs: list[subprocess.Popen] = []
_launched_pw_instances: list = []

# ── Chrome discovery ────────────────────────────────────────────────────────────

_WIN_CANDIDATES = [
    os.path.expandvars(r"%ProgramFiles%\Google\Chrome\Application\chrome.exe"),
    os.path.expandvars(r"%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe"),
    os.path.expandvars(r"%LocalAppData%\Google\Chrome\Application\chrome.exe"),
]

_LINUX_CANDIDATES = [
    "/usr/bin/google-chrome",
    "/usr/bin/google-chrome-stable",
    "/usr/bin/chromium-browser",
    "/usr/bin/chromium",
]

_MAC_CANDIDATES = [
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
]


def is_browser_available() -> bool:
    """Return True if a usable Chrome binary exists on this machine."""
    try:
        find_chrome()
        return True
    except RuntimeError:
        return False


def find_chrome() -> str:
    """Find Chrome executable on the system. Raises RuntimeError if not found."""
    env = os.environ.get("CHROME_PATH", "")
    if env and os.path.isfile(env):
        return env

    system = platform.system()
    if system == "Windows":
        candidates = _WIN_CANDIDATES
    elif system == "Darwin":
        candidates = _MAC_CANDIDATES
    else:
        candidates = _LINUX_CANDIDATES

    # Also try PATH
    which = shutil.which("google-chrome") or shutil.which("chrome") or shutil.which("chromium")
    if which:
        candidates = [which] + candidates

    for c in candidates:
        if c and os.path.isfile(c):
            return c

    raise RuntimeError(
        "Chrome not found. Install Google Chrome or set CHROME_PATH env var."
    )


# ── Stealth window args ────────────────────────────────────────────────────────

def get_proxy(env_var: str) -> Optional[dict]:
    """Read a Playwright proxy dict from an environment variable.

    Set the env var to an HTTP proxy URL, e.g.
        KAYAK_PROXY="http://user:pass@resi-proxy.example.com:10001"

    Falls back to ``LETSFG_PROXY`` if *env_var* is unset/empty, so a single
    global proxy covers all connectors without per-airline configuration.

    Returns a dict suitable for ``pw.chromium.launch(proxy=...)``,
    or *None* when no proxy is configured.
    """
    raw = os.environ.get(env_var, "").strip()
    if not raw:
        raw = os.environ.get("LETSFG_PROXY", "").strip()
    if not raw:
        return None
    return _parse_proxy_url(raw)


def _parse_proxy_url(raw: str) -> Optional[dict]:
    """Parse a proxy URL into a Playwright proxy dict."""
    if not raw:
        return None
    from urllib.parse import urlparse

    p = urlparse(raw)
    result: dict[str, str] = {"server": f"{p.scheme}://{p.hostname}:{p.port}"}
    if p.username:
        result["username"] = p.username
    if p.password:
        result["password"] = p.password
    return result


# ── Port rotation for multi-endpoint proxies (e.g. 10001-10010) ──────────────

_port_cycle: Optional[itertools.cycle] = None


def _get_port_cycle() -> Optional[itertools.cycle]:
    """Lazily build a round-robin port iterator from LETSFG_PROXY_PORT_RANGE."""
    global _port_cycle
    if _port_cycle is not None:
        return _port_cycle
    raw = os.environ.get("LETSFG_PROXY_PORT_RANGE", "").strip()
    if not raw:
        return None
    try:
        lo, hi = raw.split("-", 1)
        ports = list(range(int(lo), int(hi) + 1))
        if not ports:
            return None
        _port_cycle = itertools.cycle(ports)
        return _port_cycle
    except (ValueError, TypeError):
        logger.warning("Invalid LETSFG_PROXY_PORT_RANGE=%r — ignoring", raw)
        return None


def _rotating_proxy_url() -> str:
    """Return LETSFG_PROXY with port rotated if LETSFG_PROXY_PORT_RANGE is set."""
    base = os.environ.get("LETSFG_PROXY", "").strip()
    if not base:
        return ""
    cycle = _get_port_cycle()
    if cycle is None:
        return base
    p = urlparse(base)
    port = next(cycle)
    rotated = p._replace(netloc=f"{p.username}:{p.password}@{p.hostname}:{port}" if p.username else f"{p.hostname}:{port}")
    return urlunparse(rotated)


def get_default_proxy() -> Optional[dict]:
    """Return the global Playwright proxy dict from ``LETSFG_PROXY``, or None."""
    return _parse_proxy_url(_rotating_proxy_url())


def get_default_proxy_url() -> str:
    """Return the raw ``LETSFG_PROXY`` URL string, or empty string."""
    return _rotating_proxy_url()


def get_httpx_proxy_url() -> Optional[str]:
    """Return proxy URL for httpx clients, or None.

    Reads ``LETSFG_PROXY``.  Automatically rotates port when
    ``LETSFG_PROXY_PORT_RANGE`` is set.  Use as::

        httpx.AsyncClient(proxy=get_httpx_proxy_url())
    """
    url = _rotating_proxy_url()
    return url or None


def get_curl_cffi_proxies() -> Optional[dict]:
    """Return proxy dict for curl_cffi sessions, or None.

    Reads ``LETSFG_PROXY``.  Automatically rotates port when
    ``LETSFG_PROXY_PORT_RANGE`` is set.  Use as::

        cffi_requests.Session(proxies=get_curl_cffi_proxies())
    """
    url = _rotating_proxy_url()
    if not url:
        return None
    return {"http": url, "https": url}


def proxy_chrome_args() -> list[str]:
    """Return Chrome CLI args to route traffic through the global proxy.

    For CDP Chrome subprocess launches, spread into your arg list::

        args = [chrome, ..., *proxy_chrome_args(), ...]

    Returns empty list when ``LETSFG_PROXY`` is not set.
    Rotates port when ``LETSFG_PROXY_PORT_RANGE`` is set.
    """
    raw = _rotating_proxy_url()
    if not raw:
        return []
    p = urlparse(raw)
    server = f"{p.scheme}://{p.hostname}:{p.port}"
    args = [f"--proxy-server={server}"]
    return args


def proxy_is_configured() -> bool:
    """Return True when a global proxy (``LETSFG_PROXY``) is set."""
    return bool(os.environ.get("LETSFG_PROXY", "").strip())


# ── Resource blocking — saves bandwidth when routing through residential proxy ──

# Resource types to always block (saves ~80% bandwidth)
_BLOCKED_RESOURCE_TYPES = frozenset({
    "image",       # jpgs, pngs, webp, svg, etc. - huge bandwidth hog
    "media",       # videos, audio files
    "font",        # web fonts (woff2, ttf, etc.)
    "websocket",   # usually telemetry/tracking sockets
    "manifest",    # PWA manifests - not needed for scraping
})

# URL patterns to block (analytics, tracking, ads, social widgets)
# These are regex-like globs matched against full URL
_BLOCKED_URL_PATTERNS = (
    # Google
    "*google-analytics.com*",
    "*googletagmanager.com*",
    "*googlesyndication.com*",
    "*googleadservices.com*",
    "*doubleclick.net*",
    "*google.com/pagead*",
    # Facebook
    "*facebook.com/tr*",
    "*facebook.net/en_US/fbevents*",
    "*connect.facebook.net*",
    # Other analytics
    "*hotjar.com*",
    "*clarity.ms*",
    "*fullstory.com*",
    "*mixpanel.com*",
    "*segment.com*",
    "*amplitude.com*",
    "*heapanalytics.com*",
    "*intercom.io*",
    "*zendesk.com/embeddable*",
    "*appsflyer.com*",
    "*branch.io*",
    # Ads
    "*adsrvr.org*",
    "*adnxs.com*",
    "*criteo.com*",
    "*taboola.com*",
    "*outbrain.com*",
    "*amazon-adsystem.com*",
    "*ads.linkedin.com*",
    "*ads.twitter.com*",
    # Tracking pixels & beacons
    "*pixel.wp.com*",
    "*bat.bing.com*",
    "*tr.snapchat.com*",
    "*tiktok.com/i18n*",
    "*cdn.mxpnl.com*",
    # Social widgets
    "*platform.twitter.com/widgets*",
    "*buttons.github.io*",
    "*addthis.com*",
    "*sharethis.com*",
    # Error tracking (not needed for scraping)
    "*sentry.io*",
    "*bugsnag.com*",
    "*rollbar.com*",
    # Chat widgets
    "*drift.com*",
    "*crisp.chat*",
    "*tawk.to*",
    "*livechatinc.com*",
)


async def _block_handler(route):
    """Abort blocked resource types, continue others."""
    req = route.request
    if req.resource_type in _BLOCKED_RESOURCE_TYPES:
        await route.abort()
    else:
        await route.continue_()


async def _aggressive_block_handler(route):
    """Block by resource type AND URL pattern for maximum bandwidth savings."""
    req = route.request
    url = req.url.lower()
    
    # Block by resource type
    if req.resource_type in _BLOCKED_RESOURCE_TYPES:
        await route.abort()
        return
    
    # Block by URL pattern (analytics, tracking, ads)
    for pattern in _BLOCKED_URL_PATTERNS:
        # Convert glob to simple check (fnmatch-style)
        check = pattern.replace("*", "")
        if check in url:
            await route.abort()
            return
    
    await route.continue_()


async def block_heavy_resources(page) -> None:
    """Block images, video, and fonts to save proxy bandwidth.

    Call right after ``ctx.new_page()``, before navigation.
    Keeps scripts & stylesheets intact (anti-bot systems may check them).
    """
    await page.route("**/*", _block_handler)


async def block_all_heavy_resources(page) -> None:
    """Aggressively block images, video, fonts, AND tracking/analytics.
    
    Use this when bandwidth is critical (expensive residential proxies).
    Blocks ~90% of typical page weight while keeping core functionality.
    """
    await page.route("**/*", _aggressive_block_handler)


async def auto_block_if_proxied(page) -> None:
    """Aggressively block heavy resources when a global proxy is configured.

    No-op when ``LETSFG_PROXY`` is not set (local mode — bandwidth is free).
    Use this in connectors instead of ``block_heavy_resources`` to auto-detect.
    
    Blocks: images, video, fonts, websockets, analytics, tracking, ads, social widgets.
    """
    if proxy_is_configured():
        await page.route("**/*", _aggressive_block_handler)


# ── Anti-bot stealth injection ──────────────────────────────────────────────────

_STEALTH_INIT_SCRIPT = """\
// Hide navigator.webdriver (set by CDP/Playwright)
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});

// Ensure window.chrome exists (missing in headless/automation)
if (!window.chrome) {
  window.chrome = {runtime: {}, loadTimes: function(){}, csi: function(){}};
}

// Fake plugins array (headless has empty plugins)
Object.defineProperty(navigator, 'plugins', {
  get: () => {
    const arr = [
      {name:'Chrome PDF Plugin', filename:'internal-pdf-viewer'},
      {name:'Chrome PDF Viewer', filename:'mhjfbmdgcfjbbpaeojofohoefgiehjai'},
      {name:'Native Client', filename:'internal-nacl-plugin'},
    ];
    arr.item = i => arr[i];
    arr.namedItem = n => arr.find(p => p.name === n);
    arr.refresh = () => {};
    return arr;
  }
});

// Fix languages (automation sometimes misses this)
Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});

// Remove Playwright/CDP tell-tales from Error stack traces
const _Error = Error;
Object.defineProperty(globalThis, 'Error', {value: _Error, configurable: true, writable: true});

// Mask permissions query (Notification permission check used by PX/Akamai)
const origQuery = window.navigator.permissions?.query;
if (origQuery) {
  window.navigator.permissions.query = (params) =>
    params.name === 'notifications'
      ? Promise.resolve({state: Notification.permission})
      : origQuery.call(window.navigator.permissions, params);
}
"""


async def inject_stealth_js(page) -> None:
    """Inject anti-detection JavaScript into a page.

    Must be called BEFORE any navigation (``page.goto(...)``).
    Works with both CDP-connected real Chrome and Playwright browsers.
    Patches navigator.webdriver, plugins, chrome runtime, and permissions
    to reduce bot detection score on PerimeterX, Akamai, and Cloudflare.

    Usage::

        page = await context.new_page()
        await inject_stealth_js(page)
        await page.goto("https://protected-site.com")
    """
    await page.add_init_script(_STEALTH_INIT_SCRIPT)


def _is_visible() -> bool:
    """Check if browsers should be visible (for debugging)."""
    return os.environ.get("BOOSTED_BROWSER_VISIBLE", "").strip() in ("1", "true", "yes")


def is_headless() -> bool:
    """Returns True when browsers should run headless (default).

    Set BOOSTED_BROWSER_VISIBLE=1 to force headed mode for debugging.
    """
    return not _is_visible()


def stealth_args() -> list[str]:
    """
    Chrome CLI args for invisible operation.

    Default: --headless=new (Chrome's modern undetectable headless).
    When BOOSTED_BROWSER_VISIBLE=1, returns empty list (normal headed window).
    """
    if _is_visible():
        return []
    return [
        "--headless=new",
        "--disable-http2",
        "--window-position=-2400,-2400",
        "--window-size=800,600",
    ]


def stealth_position_arg() -> list[str]:
    """Headless + off-screen position (for connectors that set their own --window-size)."""
    if _is_visible():
        return []
    return ["--headless=new", "--disable-http2", "--window-position=-2400,-2400"]


def stealth_popen_kwargs() -> dict:
    """
    Extra kwargs for subprocess.Popen to suppress the Chrome window on Windows.

    Uses STARTUPINFO with SW_SHOWMINNOACTIVE so the window starts minimized
    without stealing focus. On Linux/Mac, suppresses stdout/stderr.
    """
    kwargs: dict = {
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
    }

    if _is_visible():
        return kwargs

    if platform.system() == "Windows":
        si = subprocess.STARTUPINFO()
        si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        si.wShowWindow = 7  # SW_SHOWMINNOACTIVE — minimized, no focus steal
        kwargs["startupinfo"] = si
        kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW
    return kwargs


# ── CDP Chrome launch ──────────────────────────────────────────────────────────

async def launch_cdp_chrome(
    port: int,
    user_data_dir: str,
    *,
    extra_args: Optional[list[str]] = None,
    start_url: str = "about:blank",
    startup_wait: float = 2.0,
) -> subprocess.Popen:
    """
    Launch a real Chrome instance for CDP connection.

    The window is pushed off-screen and minimized so it doesn't
    disturb the user. Set BOOSTED_BROWSER_VISIBLE=1 to override.

    Args:
        port: CDP debugging port (e.g. 9460).
        user_data_dir: Path to Chrome user data directory.
        extra_args: Additional Chrome CLI flags.
        start_url: Initial URL to load (default: about:blank).
        startup_wait: Seconds to wait after launch for Chrome to be ready.

    Returns:
        The subprocess.Popen handle.
    """
    chrome = find_chrome()
    os.makedirs(user_data_dir, exist_ok=True)

    args = [
        chrome,
        f"--remote-debugging-port={port}",
        f"--user-data-dir={user_data_dir}",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-blink-features=AutomationControlled",
        *stealth_args(),
        *proxy_chrome_args(),
        *(extra_args or []),
        start_url,
    ]

    proc = subprocess.Popen(args, **stealth_popen_kwargs())
    _launched_procs.append(proc)
    await asyncio.sleep(startup_wait)
    logger.info("Chrome launched on CDP port %d (pid %d, visible=%s)",
                port, proc.pid, _is_visible())
    return proc


async def connect_cdp(port: int):
    """
    Connect Playwright to an existing Chrome via CDP.

    Returns the Playwright Browser object.
    """
    from playwright.async_api import async_playwright
    pw = await async_playwright().start()
    _launched_pw_instances.append(pw)
    browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{port}")
    return browser


async def get_or_launch_cdp(
    port: int,
    user_data_dir: str,
    *,
    extra_args: Optional[list[str]] = None,
    start_url: str = "about:blank",
    startup_wait: float = 2.0,
):
    """
    Try connecting to existing Chrome on port, or launch a new one.

    Returns (browser, proc_or_None).  Playwright instances are tracked
    in ``_launched_pw_instances`` so ``cleanup_all_browsers()`` can stop
    them later.
    """
    from playwright.async_api import async_playwright

    # Try connecting to already-running Chrome
    pw = None
    try:
        pw = await async_playwright().start()
        browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{port}")
        _launched_pw_instances.append(pw)
        logger.info("Connected to existing Chrome on port %d", port)
        return browser, None
    except Exception:
        if pw:
            try:
                await pw.stop()
            except Exception:
                pass

    # Launch new Chrome
    proc = await launch_cdp_chrome(
        port, user_data_dir,
        extra_args=extra_args,
        start_url=start_url,
        startup_wait=startup_wait,
    )
    pw = await async_playwright().start()
    _launched_pw_instances.append(pw)
    browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{port}")
    return browser, proc


# ── Playwright headed launch ───────────────────────────────────────────────────

async def launch_headed_browser(
    *,
    channel: str = "chrome",
    extra_args: Optional[list[str]] = None,
    proxy: Optional[dict] = None,
):
    """
    Launch a headed Playwright browser with stealth window positioning.

    Used by connectors that need Playwright.launch(headless=False) instead of CDP.
    Window is pushed off-screen unless BOOSTED_BROWSER_VISIBLE=1.

    When *proxy* is not provided, automatically uses ``LETSFG_PROXY`` if set.

    Returns the Playwright Browser object.
    """
    from playwright.async_api import async_playwright

    args = [*stealth_args(), *(extra_args or [])]

    # Auto-resolve proxy from LETSFG_PROXY when not explicitly provided
    if proxy is None:
        proxy = get_default_proxy()

    headless = is_headless()
    pw = await async_playwright().start()
    _launched_pw_instances.append(pw)

    launch_kw: dict = {
        "headless": headless,
        "channel": channel,
        "args": args if args else None,
    }
    if proxy:
        launch_kw["proxy"] = proxy

    try:
        browser = await pw.chromium.launch(**launch_kw)
    except Exception:
        # Fallback: no channel (use bundled Chromium)
        launch_kw.pop("channel", None)
        browser = await pw.chromium.launch(**launch_kw)
    logger.info("Browser launched (channel=%s, headless=%s, proxy=%s)",
                channel, headless, bool(proxy))
    return browser


# ── Cleanup ─────────────────────────────────────────────────────────────────────

async def cleanup_module_browsers(*modules) -> int:
    """
    Clean up browser resources stored in module-level globals.

    Introspects each module for known global names and closes/terminates them.
    Returns number of resources closed.
    """
    closed = 0
    for mod in modules:
        # Close Playwright browser contexts
        for attr in ('_context', '_pw_context'):
            ctx = getattr(mod, attr, None)
            if ctx:
                try:
                    await ctx.close()
                except Exception:
                    pass
                setattr(mod, attr, None)
                closed += 1

        # Close pages (warm pages, persistent pages, leaked singleton pages)
        for attr in ('_warm_page', '_persistent_page', '_page', '_api_page'):
            pg = getattr(mod, attr, None)
            if pg:
                try:
                    await pg.close()
                except Exception:
                    pass
                setattr(mod, attr, None)
                closed += 1

        # Close primary Playwright browser
        for attr in ('_browser', '_cdp_browser', '_pw_browser'):
            browser = getattr(mod, attr, None)
            if browser:
                try:
                    await browser.close()
                except Exception:
                    pass
                setattr(mod, attr, None)
                closed += 1

        # Quit nodriver browser (twayair)
        nd = getattr(mod, '_nd_browser', None)
        if nd:
            try:
                nd.stop()
            except Exception:
                pass
            mod._nd_browser = None
            closed += 1

        # Stop Playwright instances
        pw = getattr(mod, '_pw_instance', None)
        if pw:
            try:
                await pw.stop()
            except Exception:
                pass
            mod._pw_instance = None
            # Remove from global tracking list so cleanup_all_browsers()
            # won't stop other modules' Playwright instances.
            try:
                _launched_pw_instances.remove(pw)
            except ValueError:
                pass
            closed += 1

        # Terminate Chrome subprocess
        proc = getattr(mod, '_chrome_proc', None)
        if proc:
            try:
                if proc.poll() is None:
                    proc.terminate()
                    proc.wait(timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
            mod._chrome_proc = None
            # Remove from global tracking list so cleanup_all_browsers()
            # won't try to kill it again (or kill other modules' procs).
            try:
                _launched_procs.remove(proc)
            except ValueError:
                pass
            closed += 1

        # Reset the browser init lock so _get_browser() re-creates after cleanup
        if getattr(mod, '_browser_lock', None) is not None:
            mod._browser_lock = None

    return closed


async def cleanup_all_browsers():
    """
    Terminate all Chrome processes and Playwright instances launched via
    browser.py helper functions (launch_cdp_chrome, launch_headed_browser).

    Only affects processes this module created — never touches the user's
    own Chrome windows.
    """
    closed = 0

    # Stop Playwright instances launched by launch_headed_browser / connect_cdp
    for pw in _launched_pw_instances:
        try:
            await pw.stop()
            closed += 1
        except Exception:
            pass
    _launched_pw_instances.clear()

    # Terminate Chrome subprocesses launched by launch_cdp_chrome
    for proc in _launched_procs:
        try:
            if proc.poll() is None:
                proc.terminate()
                proc.wait(timeout=5)
                closed += 1
        except Exception:
            try:
                proc.kill()
                closed += 1
            except Exception:
                pass
    _launched_procs.clear()

    # Reset the semaphore so it's fresh for next search (picks up any config changes)
    global _browser_semaphore
    _browser_semaphore = None

    if closed:
        logger.info("browser.py cleanup: terminated %d browser resources", closed)

    return closed
