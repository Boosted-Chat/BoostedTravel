"""
System resource detection for adaptive concurrency tuning.

Safely detects RAM, CPU cores, and available memory so agents and the
engine can pick optimal concurrency settings automatically.

    from letsfg.system_info import get_system_profile
    profile = get_system_profile()
    # {'ram_total_gb': 16.0, 'ram_available_gb': 9.2, 'cpu_cores': 8,
    #  'recommended_max_browsers': 8, 'tier': 'standard'}
"""

from __future__ import annotations

import logging
import os
import platform

logger = logging.getLogger(__name__)

# ── Tier thresholds (RAM in GB) ──────────────────────────────────────────────
# Each tier maps to a recommended max concurrent browsers.
# Each headless Chrome instance uses ~150-300 MB RAM.
_TIERS = [
    # (min_ram_gb, max_browsers, tier_name)
    (0,   2, "minimal"),     # <2 GB — barely usable
    (2,   3, "low"),         # 2–4 GB — budget laptops
    (4,   5, "moderate"),    # 4–8 GB — older machines
    (8,   8, "standard"),    # 8–16 GB — typical dev machine
    (16, 12, "high"),        # 16–32 GB — power user
    (32, 12, "maximum"),     # 32+ GB — workstation
]


def _get_ram_total_gb() -> float | None:
    """Total physical RAM in GB. Returns None if detection fails."""
    try:
        import psutil
        return round(psutil.virtual_memory().total / (1024 ** 3), 1)
    except ImportError:
        pass

    # Fallback: platform-specific without psutil
    system = platform.system()
    try:
        if system == "Windows":
            import ctypes
            kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
            c_ulong = ctypes.c_ulonglong
            class MEMSTAT(ctypes.Structure):
                _fields_ = [
                    ("dwLength", ctypes.c_ulong),
                    ("dwMemoryLoad", ctypes.c_ulong),
                    ("ullTotalPhys", c_ulong),
                    ("ullAvailPhys", c_ulong),
                    ("ullTotalPageFile", c_ulong),
                    ("ullAvailPageFile", c_ulong),
                    ("ullTotalVirtual", c_ulong),
                    ("ullAvailVirtual", c_ulong),
                    ("ullAvailExtendedVirtual", c_ulong),
                ]
            stat = MEMSTAT()
            stat.dwLength = ctypes.sizeof(stat)
            kernel32.GlobalMemoryStatusEx(ctypes.byref(stat))
            return round(stat.ullTotalPhys / (1024 ** 3), 1)
        elif system == "Linux":
            with open("/proc/meminfo") as f:
                for line in f:
                    if line.startswith("MemTotal:"):
                        kb = int(line.split()[1])
                        return round(kb / (1024 ** 2), 1)
        elif system == "Darwin":
            import subprocess
            out = subprocess.check_output(["sysctl", "-n", "hw.memsize"], text=True)
            return round(int(out.strip()) / (1024 ** 3), 1)
    except Exception as exc:
        logger.debug("RAM detection fallback failed: %s", exc)

    return None


def _get_ram_available_gb() -> float | None:
    """Available (free + cached) RAM in GB. Returns None if detection fails."""
    try:
        import psutil
        return round(psutil.virtual_memory().available / (1024 ** 3), 1)
    except ImportError:
        pass

    system = platform.system()
    try:
        if system == "Windows":
            import ctypes
            kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
            c_ulong = ctypes.c_ulonglong
            class MEMSTAT(ctypes.Structure):
                _fields_ = [
                    ("dwLength", ctypes.c_ulong),
                    ("dwMemoryLoad", ctypes.c_ulong),
                    ("ullTotalPhys", c_ulong),
                    ("ullAvailPhys", c_ulong),
                    ("ullTotalPageFile", c_ulong),
                    ("ullAvailPageFile", c_ulong),
                    ("ullTotalVirtual", c_ulong),
                    ("ullAvailVirtual", c_ulong),
                    ("ullAvailExtendedVirtual", c_ulong),
                ]
            stat = MEMSTAT()
            stat.dwLength = ctypes.sizeof(stat)
            kernel32.GlobalMemoryStatusEx(ctypes.byref(stat))
            return round(stat.ullAvailPhys / (1024 ** 3), 1)
        elif system == "Linux":
            with open("/proc/meminfo") as f:
                for line in f:
                    if line.startswith("MemAvailable:"):
                        kb = int(line.split()[1])
                        return round(kb / (1024 ** 2), 1)
    except Exception as exc:
        logger.debug("Available RAM detection failed: %s", exc)

    return None


def _get_cpu_cores() -> int:
    """Logical CPU core count."""
    return os.cpu_count() or 1


def _recommend_max_browsers(ram_gb: float | None) -> int:
    """Pick max concurrent browsers based on available RAM."""
    if ram_gb is None:
        return 6  # safe middle ground when detection fails

    for min_ram, max_browsers, _ in reversed(_TIERS):
        if ram_gb >= min_ram:
            return max_browsers
    return 2


def _get_tier_name(ram_gb: float | None) -> str:
    """Human-readable tier name."""
    if ram_gb is None:
        return "unknown"
    for min_ram, _, name in reversed(_TIERS):
        if ram_gb >= min_ram:
            return name
    return "minimal"


def get_system_profile() -> dict:
    """
    Detect system resources and return a profile dict.

    Returns:
        {
            "ram_total_gb": 16.0,          # Total physical RAM
            "ram_available_gb": 9.2,        # Currently available RAM
            "cpu_cores": 8,                 # Logical cores
            "recommended_max_browsers": 8,  # Auto-detected optimal concurrency
            "tier": "standard",             # Human-readable tier name
            "platform": "Windows",          # OS name
        }

    All values are best-effort. If detection fails for a field, it returns
    None for that field (except recommended_max_browsers which always
    returns a safe default).
    """
    ram_total = _get_ram_total_gb()
    ram_available = _get_ram_available_gb()

    # Use available RAM if we have it (more accurate for current load),
    # otherwise fall back to total RAM
    ram_for_decision = ram_available if ram_available is not None else ram_total

    return {
        "ram_total_gb": ram_total,
        "ram_available_gb": ram_available,
        "cpu_cores": _get_cpu_cores(),
        "recommended_max_browsers": _recommend_max_browsers(ram_for_decision),
        "tier": _get_tier_name(ram_for_decision),
        "platform": platform.system(),
    }
