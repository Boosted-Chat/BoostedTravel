"""
Shared browser launcher for LetsFG connectors.

Provides Chrome discovery, stealth CDP launch (off-screen, no-focus),
and Playwright helpers. All CDP Chrome connectors should use these
instead of rolling their own launch logic.

Environment variables:
    CHROME_PATH             — Override Chrome executable path.
    BOOSTED_BROWSER_VISIBLE — Set to "1" to show browser windows (debugging).
    LETSFG_BROWSER_WS      — WebSocket URL for remote browser (Browserbase, etc.).
                              When set, connectors connect to this instead of local Chrome.
"""

from __future__ import annotations

import asyncio
import glob
import logging
import os
import platform
import shutil
import subprocess
import time
from typing import Optional

logger = logging.getLogger(__name__)

# ── Concurrency gate — limits how many browsers can run at once ──────────────
# Dynamically tuned based on system resources. On weak cloud VMs (2 vCPU, 4GB),
# running 8 Chrome instances simultaneously will OOM-kill everything.

def _detect_max_browsers() -> int:
    """Detect optimal browser concurrency based on system resources.

    Chrome ~300-500MB RAM per instance + ~0.5 CPU. Tuning:
      - 16GB+ RAM, 8+ cores  → 8 browsers (desktop/workstation)
      - 8GB RAM, 4 cores     → 4 browsers (standard laptop)
      - 4GB RAM, 2 cores     → 2 browsers (cloud VM, Mac Mini base)
      - 2GB RAM, 1 core      → 1 browser  (minimal container)

    Override with LETSFG_MAX_BROWSERS env var.
    """
    override = os.environ.get("LETSFG_MAX_BROWSERS", "").strip()
    if override.isdigit() and int(override) > 0:
        return int(override)

    try:
        cpu_count = os.cpu_count() or 2
    except Exception:
        cpu_count = 2

    # Get available memory in GB
    mem_gb = _get_available_memory_gb()

    # Each Chrome needs ~400MB RAM and benefits from ~0.5 CPU
    by_cpu = max(1, cpu_count // 2)
    by_mem = max(1, int(mem_gb / 0.5)) if mem_gb > 0 else 4  # 500MB reserved per browser

    limit = min(by_cpu, by_mem, 8)  # Cap at 8 regardless
    logger.info("Browser concurrency: %d (cpus=%d, mem=%.1fGB, by_cpu=%d, by_mem=%d)",
                limit, cpu_count, mem_gb, by_cpu, by_mem)
    return limit


def _get_available_memory_gb() -> float:
    """Get available system memory in GB. Returns 0 if unknown."""
    # Try psutil first (most reliable)
    try:
        import psutil
        return psutil.virtual_memory().available / (1024 ** 3)
    except ImportError:
        pass

    system = platform.system()

    if system == "Linux":
        try:
            with open("/proc/meminfo") as f:
                for line in f:
                    if line.startswith("MemAvailable:"):
                        return int(line.split()[1]) / (1024 * 1024)  # kB → GB
        except Exception:
            pass

    if system == "Darwin":
        try:
            # macOS: vm_stat gives free + inactive pages
            result = subprocess.run(
                ["vm_stat"], capture_output=True, text=True, timeout=5,
            )
            pages_free = 0
            page_size = 16384  # default on Apple Silicon
            for line in result.stdout.splitlines():
                if "page size" in line.lower():
                    nums = [int(s) for s in line.split() if s.isdigit()]
                    if nums:
                        page_size = nums[0]
                if "Pages free:" in line or "Pages inactive:" in line:
                    nums = [int(s.rstrip(".")) for s in line.split() if s.rstrip(".").isdigit()]
                    if nums:
                        pages_free += nums[0]
            return (pages_free * page_size) / (1024 ** 3)
        except Exception:
            pass

    if system == "Windows":
        try:
            result = subprocess.run(
                ["wmic", "OS", "get", "FreePhysicalMemory", "/value"],
                capture_output=True, text=True, timeout=5,
            )
            for line in result.stdout.splitlines():
                if "FreePhysicalMemory" in line:
                    val = line.split("=")[1].strip()
                    return int(val) / (1024 * 1024)  # kB → GB
        except Exception:
            pass

    return 0.0


_MAX_CONCURRENT_BROWSERS: int = _detect_max_browsers()
_browser_semaphore: Optional[asyncio.Semaphore] = None


async def _get_browser_semaphore() -> asyncio.Semaphore:
    """Get or create the global browser concurrency semaphore (lazy init)."""
    global _browser_semaphore
    if _browser_semaphore is None:
        _browser_semaphore = asyncio.Semaphore(_MAX_CONCURRENT_BROWSERS)
    return _browser_semaphore


async def acquire_browser_slot():
    """Acquire a browser slot — blocks if too many browsers are running."""
    sem = await _get_browser_semaphore()
    await sem.acquire()


def release_browser_slot():
    """Release a browser slot after a connector finishes with its browser."""
    if _browser_semaphore is not None:
        _browser_semaphore.release()


# ── Cleanup registry — tracks resources launched by connectors ───────────────
_launched_procs: list[subprocess.Popen] = []
_launched_pw_instances: list = []
_launched_browsers: list = []  # browsers from launch_headed_browser(), closed before pw.stop()

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
    # Homebrew Chromium (common on Mac Mini / developer setups)
    "/opt/homebrew/bin/chromium",
    "/usr/local/bin/chromium",
    # Brave (Chromium-based, supports CDP)
    "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
    # Arc (Chromium-based)
    "/Applications/Arc.app/Contents/MacOS/Arc",
    # Microsoft Edge (Chromium-based)
    "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
    # Chromium.app (standalone or Homebrew cask)
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
]


def _playwright_chromium_candidates() -> list[str]:
    """Find Playwright's bundled Chromium (works even when Chrome isn't installed).

    Checks standard Playwright cache dirs on all platforms including Apple Silicon Macs.
    On macOS, also attempts to remove Gatekeeper quarantine attribute from Chromium
    (Playwright's unsigned binary gets blocked by Gatekeeper on first run).
    """
    candidates = []
    home = os.path.expanduser("~")
    cache_dirs = [
        os.path.join(home, ".cache", "ms-playwright"),           # Linux default
        os.path.join(home, "AppData", "Local", "ms-playwright"), # Windows
        os.path.join(home, "Library", "Caches", "ms-playwright"),# macOS
    ]
    for cache_dir in cache_dirs:
        if not os.path.isdir(cache_dir):
            continue
        # Chromium dirs are like chromium-1208/
        for chrome_dir in sorted(glob.glob(os.path.join(cache_dir, "chromium-*")), reverse=True):
            for subpath in [
                # Linux
                os.path.join(chrome_dir, "chrome-linux64", "chrome"),
                os.path.join(chrome_dir, "chrome-linux", "chrome"),
                # Windows
                os.path.join(chrome_dir, "chrome-win", "chrome.exe"),
                # macOS Intel
                os.path.join(chrome_dir, "chrome-mac", "Chromium.app", "Contents", "MacOS", "Chromium"),
                # macOS Apple Silicon (arm64)
                os.path.join(chrome_dir, "chrome-mac-arm64", "Chromium.app", "Contents", "MacOS", "Chromium"),
            ]:
                if os.path.isfile(subpath):
                    # On macOS, remove Gatekeeper quarantine (Playwright's Chromium is unsigned)
                    if platform.system() == "Darwin" and "Chromium" in subpath:
                        _remove_macos_quarantine(subpath)
                    candidates.append(subpath)
    return candidates


def _remove_macos_quarantine(binary_path: str) -> None:
    """Remove macOS Gatekeeper quarantine attribute from a binary.

    Playwright's bundled Chromium is unsigned, so macOS Gatekeeper blocks it
    on first run with "Chromium.app is damaged and can't be opened". Removing
    the com.apple.quarantine xattr fixes this. This is the same as running:
        xattr -cr /path/to/Chromium.app

    Only runs if the quarantine attribute is actually present.
    """
    try:
        # Find the .app bundle (go up from the binary)
        app_path = binary_path
        while app_path and not app_path.endswith(".app"):
            app_path = os.path.dirname(app_path)
        if not app_path:
            return

        # Check if quarantine attribute exists
        result = subprocess.run(
            ["xattr", "-l", app_path],
            capture_output=True, text=True, timeout=5,
        )
        if "com.apple.quarantine" not in result.stdout:
            return

        # Remove it recursively
        subprocess.run(
            ["xattr", "-cr", app_path],
            capture_output=True, timeout=10,
        )
        logger.info("Removed macOS Gatekeeper quarantine from %s", app_path)
    except Exception as e:
        logger.debug("Could not remove quarantine from %s: %s", binary_path, e)


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
    if system == "Darwin":
        # Homebrew on Mac often puts chromium in a non-standard location
        which = which or shutil.which("chromium", path="/opt/homebrew/bin:/usr/local/bin")
    if which:
        candidates = [which] + candidates

    # Fallback: Playwright's bundled Chromium
    candidates.extend(_playwright_chromium_candidates())

    for c in candidates:
        if c and os.path.isfile(c):
            return c

    raise RuntimeError(
        "Chrome not found. Install Google Chrome or set CHROME_PATH env var."
    )


def is_browser_available() -> bool:
    """Check if a Chrome/Chromium binary can be found. Non-throwing."""
    try:
        find_chrome()
        return True
    except RuntimeError:
        return False


# ── Virtual display (Xvfb) auto-start for headless Linux ────────────────────

_xvfb_proc: Optional[subprocess.Popen] = None


def _ensure_display() -> bool:
    """Ensure a display server is available on Linux.

    On Windows/macOS, always returns True (no X server needed).
    On Linux with DISPLAY already set, returns True.
    On Linux without DISPLAY, tries to auto-start Xvfb if installed.

    This solves the cloud agent scenario (Perplexity Computer, Replit, etc.)
    where Xvfb is installed but no display is active.
    """
    global _xvfb_proc

    if platform.system() != "Linux":
        return True

    if os.environ.get("DISPLAY"):
        return True

    # Already started by us
    if _xvfb_proc and _xvfb_proc.poll() is None:
        return True

    xvfb = shutil.which("Xvfb")
    if not xvfb:
        logger.debug("No Xvfb found — headless Chrome should still work with --headless=new")
        return True  # --headless=new doesn't strictly need X, but some ops may

    # Find a free display number
    for display_num in range(99, 110):
        lock_file = f"/tmp/.X{display_num}-lock"
        socket_file = f"/tmp/.X11-unix/X{display_num}"
        if os.path.exists(lock_file) or os.path.exists(socket_file):
            continue
        try:
            _xvfb_proc = subprocess.Popen(
                [xvfb, f":{display_num}", "-screen", "0", "1280x720x16",
                 "-ac", "-nolisten", "tcp"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            time.sleep(1)
            if _xvfb_proc.poll() is None:
                os.environ["DISPLAY"] = f":{display_num}"
                logger.info("Auto-started Xvfb on display :%d (pid %d)",
                            display_num, _xvfb_proc.pid)
                return True
        except Exception as e:
            logger.debug("Failed to start Xvfb on :%d: %s", display_num, e)

    logger.debug("Could not start Xvfb — continuing without display")
    return True  # --headless=new may still work


def cleanup_xvfb():
    """Stop the auto-started Xvfb process."""
    global _xvfb_proc
    if _xvfb_proc and _xvfb_proc.poll() is None:
        _xvfb_proc.terminate()
        try:
            _xvfb_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            _xvfb_proc.kill()
        logger.info("Stopped auto-started Xvfb (pid %d)", _xvfb_proc.pid)
    _xvfb_proc = None


# ── Remote browser support (LETSFG_BROWSER_WS) ──────────────────────────────

def get_remote_browser_ws() -> str:
    """Get remote browser WebSocket URL if configured.

    Supports:
    - Browserbase: wss://connect.browserbase.com/?apiKey=...
    - Apify: wss://...
    - Any CDP-compatible WebSocket endpoint

    Returns empty string if not configured.
    """
    return os.environ.get("LETSFG_BROWSER_WS", "").strip()


async def connect_remote_browser():
    """Connect to a remote browser via WebSocket (Browserbase, Apify, etc.).

    Returns (browser, pw_instance) or raises if connection fails.
    The caller is responsible for cleanup.
    """
    ws_url = get_remote_browser_ws()
    if not ws_url:
        raise RuntimeError("LETSFG_BROWSER_WS not set")

    from playwright.async_api import async_playwright
    pw = await async_playwright().start()
    _launched_pw_instances.append(pw)
    try:
        browser = await pw.chromium.connect_over_cdp(ws_url)
        logger.info("Connected to remote browser: %s...", ws_url[:50])
        return browser, pw
    except Exception:
        try:
            await pw.stop()
        except Exception:
            pass
        _launched_pw_instances.remove(pw)
        raise


# Auto-start display early so Chrome discovery + launch works
_ensure_display()


# ── Stealth window args ────────────────────────────────────────────────────────

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
    Try connecting to a browser, in order:
    1. Remote browser (LETSFG_BROWSER_WS) — cloud/agent environments
    2. Existing local Chrome on the given port
    3. Launch new local Chrome

    Returns (browser, proc_or_None).  Playwright instances are tracked
    in ``_launched_pw_instances`` so ``cleanup_all_browsers()`` can stop
    them later.
    """
    from playwright.async_api import async_playwright

    # 1. Try remote browser (Browserbase, Apify, etc.)
    ws_url = get_remote_browser_ws()
    if ws_url:
        try:
            browser, _pw = await connect_remote_browser()
            return browser, None
        except Exception as e:
            logger.warning("Remote browser connection failed: %s — falling back to local", e)

    # 2. Try connecting to already-running Chrome
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

    # 3. Launch new Chrome
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
):
    """
    Launch a Playwright browser, preferring remote if available.

    Order:
    1. Remote browser (LETSFG_BROWSER_WS) — cloud/agent environments
    2. Local Playwright launch with stealth window positioning

    Used by connectors that need Playwright.launch(headless=False) instead of CDP.
    Window is pushed off-screen unless BOOSTED_BROWSER_VISIBLE=1.

    Returns the Playwright Browser object.
    """
    from playwright.async_api import async_playwright

    # Try remote browser first
    ws_url = get_remote_browser_ws()
    if ws_url:
        try:
            browser, _pw = await connect_remote_browser()
            return browser
        except Exception as e:
            logger.warning("Remote browser failed: %s — launching local", e)

    args = [*stealth_args(), *(extra_args or [])]

    headless = is_headless()
    pw = await async_playwright().start()
    _launched_pw_instances.append(pw)
    try:
        browser = await pw.chromium.launch(
            headless=headless,
            channel=channel,
            args=args if args else None,
        )
    except Exception:
        # Fallback: no channel (use bundled Chromium)
        browser = await pw.chromium.launch(
            headless=headless,
            args=args if args else None,
        )
    _launched_browsers.append(browser)  # track so cleanup_all_browsers() can close it
    logger.info("Browser launched (channel=%s, headless=%s)", channel, headless)
    return browser


# ── Cleanup ─────────────────────────────────────────────────────────────────────

async def cleanup_module_browsers(*modules) -> int:
    """
    Clean up browser resources stored in module-level globals.

    Introspects each module for known global names (_browser, _chrome_proc,
    _pw_instance, _nd_browser, _cdp_browser, _context, _warm_page) and
    closes/terminates them.  Returns number of resources closed.
    """
    closed = 0
    for mod in modules:
        # Close Playwright browser context (easyjet)
        ctx = getattr(mod, '_context', None)
        if ctx:
            try:
                await ctx.close()
            except Exception:
                pass
            mod._context = None
            closed += 1

        # Close warm page (flynas keepalive)
        wp = getattr(mod, '_warm_page', None)
        if wp:
            try:
                await wp.close()
            except Exception:
                pass
            mod._warm_page = None
            closed += 1

        # Close primary Playwright browser
        for attr in ('_browser', '_cdp_browser'):
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
            closed += 1

    return closed


async def cleanup_all_browsers():
    """
    Terminate all Chrome processes and Playwright instances launched via
    browser.py helper functions (launch_cdp_chrome, launch_headed_browser).

    Only affects processes this module created — never touches the user's
    own Chrome windows.
    """
    closed = 0

    # Close browsers first so Playwright deletes their scoped_dir* temp profiles.
    # pw.stop() alone does not reliably clean up the temp dirs.
    for browser in _launched_browsers:
        try:
            await browser.close()
            closed += 1
        except Exception:
            pass
    _launched_browsers.clear()

    # Stop Playwright instances launched by launch_headed_browser / connect_cdp
    for pw in _launched_pw_instances:
        try:
            await pw.stop()
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

    # Reset the semaphore so it's fresh for next search
    global _browser_semaphore
    _browser_semaphore = None

    # Stop auto-started Xvfb (will be re-created on next search if needed)
    cleanup_xvfb()

    if closed:
        logger.info("browser.py cleanup: terminated %d browser resources", closed)

    return closed


# ── Proxy helpers ───────────────────────────────────────────────────────────────
#
# Reads from environment:
#   LETSFG_PROXY               — Global residential proxy URL for all connectors.
#                                 Format: http://user:pass@host:port
#   LETSFG_PROXY_PORT_RANGE    — Optional port range for round-robin rotation.
#                                 Format: "10001-10100" (rotates on each request)

from urllib.parse import urlparse, urlunparse
from itertools import cycle

_port_cycle: Optional[cycle] = None
_port_cycle_lock: Optional[asyncio.Lock] = None


def _get_port_cycle_lock() -> asyncio.Lock:
    global _port_cycle_lock
    if _port_cycle_lock is None:
        _port_cycle_lock = asyncio.Lock()
    return _port_cycle_lock


def _parse_proxy_url(raw: str) -> Optional[dict]:
    """Parse proxy URL into dict suitable for Playwright context."""
    if not raw:
        return None
    p = urlparse(raw)
    proxy = {"server": f"{p.scheme}://{p.hostname}:{p.port}"}
    if p.username:
        proxy["username"] = p.username
    if p.password:
        proxy["password"] = p.password
    return proxy


def _port_range_cycle() -> Optional[cycle]:
    """Lazily build a round-robin port iterator from LETSFG_PROXY_PORT_RANGE."""
    global _port_cycle
    if _port_cycle is not None:
        return _port_cycle
    raw = os.environ.get("LETSFG_PROXY_PORT_RANGE", "").strip()
    if not raw:
        return None
    try:
        start, end = raw.split("-")
        ports = list(range(int(start), int(end) + 1))
        _port_cycle = cycle(ports)
        return _port_cycle
    except Exception:
        logger.warning("Invalid LETSFG_PROXY_PORT_RANGE=%r — ignoring", raw)
        return None


def _rotating_proxy_url() -> str:
    """Return LETSFG_PROXY with port rotated if LETSFG_PROXY_PORT_RANGE is set."""
    base = os.environ.get("LETSFG_PROXY", "").strip()
    if not base:
        return ""
    port_cycle = _port_range_cycle()
    if not port_cycle:
        return base
    p = urlparse(base)
    port = next(port_cycle)
    rotated = p._replace(netloc=f"{p.username}:{p.password}@{p.hostname}:{port}" if p.username else f"{p.hostname}:{port}")
    return urlunparse(rotated)


def get_default_proxy() -> Optional[dict]:
    """Return the global Playwright proxy dict from ``LETSFG_PROXY``, or None."""
    return _parse_proxy_url(_rotating_proxy_url())


def get_default_proxy_url() -> str:
    """Return the raw ``LETSFG_PROXY`` URL string, or empty string."""
    return _rotating_proxy_url()


def get_httpx_proxy_url() -> Optional[str]:
    """Return proxy URL for httpx clients, or None."""
    url = _rotating_proxy_url()
    return url or None


def get_curl_cffi_proxies() -> Optional[dict]:
    """Return proxy dict for curl_cffi sessions, or None."""
    url = _rotating_proxy_url()
    if not url:
        return None
    return {"http": url, "https": url}


def proxy_chrome_args() -> list[str]:
    """Return Chrome CLI args to route traffic through the global proxy."""
    raw = _rotating_proxy_url()
    if not raw:
        return []
    p = urlparse(raw)
    server = f"{p.scheme}://{p.hostname}:{p.port}"
    return [f"--proxy-server={server}"]


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
        check = pattern.replace("*", "")
        if check in url:
            await route.abort()
            return
    
    await route.continue_()


async def block_heavy_resources(page) -> None:
    """Block images, video, and fonts to save proxy bandwidth."""
    await page.route("**/*", _block_handler)


async def block_all_heavy_resources(page) -> None:
    """Aggressively block images, video, fonts, AND tracking/analytics."""
    await page.route("**/*", _aggressive_block_handler)


async def auto_block_if_proxied(page) -> None:
    """Aggressively block heavy resources when a global proxy is configured.
    
    No-op when ``LETSFG_PROXY`` is not set (local mode — bandwidth is free).
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

// Fix languages
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
    """
    await page.add_init_script(_STEALTH_INIT_SCRIPT)
