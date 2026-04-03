"""
Lightweight currency conversion for normalizing multi-provider flight prices.

Uses exchangerate.host (free, no API key) with a simple in-memory cache.
Fallback to hardcoded rates if the API is unreachable.
"""

from __future__ import annotations

import logging
import time

import httpx

logger = logging.getLogger(__name__)

# Cache: {base_currency: {target: rate, ...}, ...} + timestamp
_cache: dict[str, dict[str, float]] = {}
_cache_ts: float = 0.0
_CACHE_TTL = 3600  # 1 hour

# Hardcoded fallback rates (vs EUR) — updated March 2026
_FALLBACK_VS_EUR: dict[str, float] = {
    "EUR": 1.0,
    "USD": 1.08,
    "GBP": 0.86,
    "PLN": 4.28,
    "CZK": 25.3,
    "HUF": 395.0,
    "SEK": 11.2,
    "NOK": 11.5,
    "DKK": 7.46,
    "CHF": 0.96,
    "RON": 4.97,
    "BGN": 1.96,
    "TRY": 39.5,
    "CAD": 1.47,
    "AUD": 1.66,
    "JPY": 162.0,
    "CNY": 7.85,
    "INR": 91.0,
    "BRL": 6.2,
    "THB": 37.5,
    "ZAR": 20.5,
    "AED": 3.97,
    "SAR": 4.05,
    "KES": 140.0,
    "NGN": 1760.0,
    "EGP": 53.0,
    "MYR": 5.05,
    "SGD": 1.45,
    "HKD": 8.42,
    "NZD": 1.82,
    "MXN": 21.5,
    "ARS": 1085.0,
    "KRW": 1480.0,
    "IDR": 17500.0,
    "PHP": 63.0,
    "VND": 27500.0,
}


async def fetch_rates(base: str = "EUR") -> dict[str, float]:
    """Fetch live exchange rates. Returns {currency: rate_vs_base}."""
    global _cache, _cache_ts

    now = time.monotonic()
    if base in _cache and (now - _cache_ts) < _CACHE_TTL:
        return _cache[base]

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(
                f"https://api.exchangerate.host/latest?base={base}"
            )
            resp.raise_for_status()
            data = resp.json()
            rates = data.get("rates", {})
            if rates:
                _cache[base] = {k: float(v) for k, v in rates.items()}
                _cache_ts = now
                return _cache[base]
    except Exception as e:
        logger.debug("Exchange rate API unavailable: %s — using fallback", e)

    return {}


def _fallback_convert(amount: float, from_cur: str, to_cur: str) -> float:
    """Convert using hardcoded fallback rates."""
    from_cur = from_cur.upper()
    to_cur = to_cur.upper()
    if from_cur == to_cur:
        return amount

    from_rate = _FALLBACK_VS_EUR.get(from_cur)
    to_rate = _FALLBACK_VS_EUR.get(to_cur)

    if from_rate is None or to_rate is None:
        return amount  # Can't convert — return as-is

    # from_cur → EUR → to_cur
    eur_amount = amount / from_rate
    return eur_amount * to_rate
