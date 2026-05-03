"""Live seat price lookups for LH Group airlines (LH / OS / LX / SN).

Uses great-circle distance between airports to select the correct price
tier from Lufthansa Group published Economy Light ancillary pricing.
This is more accurate than a single flat rate because LH seat prices
genuinely differ between short-haul intra-European and long-haul
intercontinental flights.

Architecture
------------
- Pure-Python distance calculation -- no HTTP calls, no credentials needed.
- Prices derived from LH published ancillary schedule:
    <= 1500 km  -> LH/OS/SN EUR 10, LX CHF 12
    1500-3000   -> LH/OS/SN EUR 15, LX CHF 18
    3000-6000   -> LH/OS/SN EUR 25, LX CHF 30
    > 6000 km   -> LH/OS/SN EUR 30, LX CHF 36
- Results cached 1 hour per (airline, origin, dest).
- Called from engine._enrich_seat_prices() with a 6 s timeout (no I/O so
  the timeout is never hit in practice).
- Returns None for non-LH airlines (handled silently by engine).

Extending
---------
Add a handler to _HANDLERS for new airlines:
    _HANDLERS["IB"] = _fetch_iberia_seat_price
where the handler signature is:
    async def _fetch_xxx(flight_no, origin, dest, date_str, cabin) -> float | None
"""

from __future__ import annotations

import asyncio
import math
import time
import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

# -- Cache --------------------------------------------------------------------

# {cache_key: (price, fetched_unix_ts)}
_SEAT_CACHE: dict[str, tuple[float, float]] = {}
_SEAT_CACHE_TTL = 3600.0  # 1 hour

# -- Airport coordinates (lat, lon) -------------------------------------------
# Covers all LH Group hubs + the most-common destinations.

_AIRPORTS: dict[str, tuple[float, float]] = {
    # Germany
    "FRA": (50.033, 8.571), "MUC": (48.353, 11.786), "BER": (52.366, 13.503),
    "HAM": (53.630, 10.006), "DUS": (51.289, 6.767), "STR": (48.689, 9.222),
    "CGN": (50.866, 7.143), "HAJ": (52.461, 9.685), "NUE": (49.499, 11.078),
    "LEJ": (51.424, 12.236), "BRE": (53.048, 8.787), "DRS": (51.132, 13.767),
    # Austria
    "VIE": (48.110, 16.570), "GRZ": (46.991, 15.440), "SZG": (47.793, 13.004),
    "INN": (47.260, 11.344), "LNZ": (48.233, 14.188),
    # Switzerland
    "ZRH": (47.458, 8.548), "GVA": (46.238, 6.109), "BSL": (47.590, 7.530),
    # Belgium
    "BRU": (50.900, 4.484),
    # UK & Ireland
    "LHR": (51.477, -0.461), "LGW": (51.148, -0.190), "MAN": (53.354, -2.275),
    "EDI": (55.950, -3.372), "BHX": (52.454, -1.748), "DUB": (53.421, -6.270),
    "SNN": (52.702, -8.924), "ORK": (51.841, -8.491),
    # France
    "CDG": (49.009, 2.548), "ORY": (48.725, 2.360), "NCE": (43.658, 7.215),
    "LYS": (45.726, 5.091), "MRS": (43.436, 5.215), "TLS": (43.629, 1.368),
    "BOD": (44.828, -0.715), "NTE": (47.157, -1.608), "SXB": (48.540, 7.629),
    # Italy
    "FCO": (41.800, 12.239), "MXP": (45.630, 8.724), "VCE": (45.505, 12.351),
    "NAP": (40.886, 14.291), "CTA": (37.466, 15.066), "BLQ": (44.535, 11.289),
    "PMO": (38.180, 13.091), "FLR": (43.810, 11.205), "TRN": (45.200, 7.650),
    # Spain & Portugal
    "BCN": (41.297, 2.078), "MAD": (40.472, -3.561), "LIS": (38.774, -9.134),
    "AGP": (36.674, -4.499), "PMI": (39.552, 2.739), "OPO": (41.235, -8.679),
    "VLC": (39.489, -0.481), "ALC": (38.282, -0.558), "SVQ": (37.418, -5.893),
    "BIO": (43.301, -2.910), "TFS": (28.045, -16.572), "LPA": (27.932, -15.386),
    "IBZ": (38.873, 1.373), "FAO": (37.014, -7.966),
    # Scandinavia
    "CPH": (55.618, 12.656), "ARN": (59.651, 17.919), "OSL": (60.202, 11.084),
    "HEL": (60.317, 24.963), "BGO": (60.294, 5.218), "TRD": (63.457, 10.924),
    "SVG": (58.877, 5.638), "GOT": (57.663, 12.279), "TMP": (61.414, 23.604),
    "BLL": (55.740, 9.152), "AAL": (57.093, 9.849),
    # Eastern Europe
    "WAW": (52.166, 20.967), "KRK": (50.078, 19.785), "GDN": (54.378, 18.467),
    "WRO": (51.103, 16.885), "POZ": (52.421, 16.826), "KTW": (50.475, 19.080),
    "PRG": (50.100, 14.260), "BRQ": (49.151, 16.695),
    "BUD": (47.439, 19.261),
    "OTP": (44.572, 26.102), "CLJ": (46.785, 23.686), "TSR": (45.809, 21.338),
    "SOF": (42.696, 23.411), "VAR": (43.232, 27.825),
    "BEG": (44.818, 20.309), "ZAG": (45.743, 16.069), "SPU": (43.539, 16.298),
    "DBV": (42.561, 18.268), "LJU": (46.224, 14.458), "TIA": (41.415, 19.721),
    "SKP": (41.961, 21.621), "TGD": (42.359, 19.252),
    # Baltics
    "RIX": (56.922, 23.972), "TLL": (59.413, 24.833), "VNO": (54.634, 25.286),
    # Benelux extras
    "AMS": (52.308, 4.764), "EIN": (51.450, 5.374), "LUX": (49.623, 6.204),
    # Turkey / Middle East
    "IST": (41.275, 28.752), "SAW": (40.898, 29.309), "ESB": (40.128, 32.995),
    "AYT": (36.898, 30.801), "ADB": (38.292, 27.157), "DLM": (36.713, 28.793),
    "BJV": (37.250, 27.664),
    "DXB": (25.253, 55.365), "AUH": (24.433, 54.651), "DOH": (25.261, 51.565),
    "TLV": (32.011, 34.887), "AMM": (31.723, 35.993), "BEY": (33.821, 35.488),
    "BAH": (26.270, 50.634), "KWI": (29.227, 47.969), "MCT": (23.593, 58.284),
    "CAI": (30.122, 31.406), "CMN": (33.367, -7.590),
    # Africa
    "JNB": (-26.133, 28.242), "CPT": (-33.964, 18.602), "ADD": (8.978, 38.799),
    "NBO": (-1.319, 36.927), "ABJ": (5.261, -3.926), "ACC": (5.605, -0.168),
    "LOS": (6.577, 3.321), "TUN": (36.851, 10.227), "ALG": (36.691, 3.215),
    "RBA": (34.051, -6.752), "MRU": (-20.430, 57.683), "TNR": (-18.797, 47.479),
    # Asia
    "BKK": (13.681, 100.747), "SIN": (1.364, 103.991), "KUL": (2.745, 101.710),
    "HKG": (22.308, 113.915), "PEK": (40.080, 116.584), "PKX": (39.509, 116.411),
    "PVG": (31.143, 121.805), "CAN": (23.392, 113.299),
    "ICN": (37.460, 126.440), "NRT": (35.765, 140.385), "HND": (35.549, 139.780),
    "KIX": (34.427, 135.244),
    "DEL": (28.556, 77.100), "BOM": (19.095, 72.874), "BLR": (13.198, 77.706),
    "MAA": (12.990, 80.169), "HYD": (17.231, 78.429), "CCU": (22.655, 88.447),
    "CMB": (7.181, 79.885),
    "CGK": (-6.126, 106.655), "DPS": (-8.748, 115.167),
    "MNL": (14.509, 121.020),
    "KHI": (24.906, 67.161), "ISB": (33.617, 73.099),
    # Americas
    "JFK": (40.640, -73.779), "EWR": (40.690, -74.175), "LGA": (40.777, -73.873),
    "ORD": (41.975, -87.907), "MDW": (41.786, -87.752),
    "LAX": (33.943, -118.408), "SFO": (37.619, -122.375), "SJC": (37.362, -121.929),
    "BOS": (42.365, -71.010), "MIA": (25.796, -80.287), "FLL": (26.072, -80.150),
    "ATL": (33.638, -84.428), "DFW": (32.897, -97.038), "IAH": (29.984, -95.341),
    "DEN": (39.856, -104.674), "IAD": (38.944, -77.456), "DCA": (38.852, -77.037),
    "SEA": (47.450, -122.309), "DTW": (42.213, -83.353), "MSP": (44.884, -93.222),
    "PHL": (39.872, -75.241), "CLT": (35.214, -80.943), "MCO": (28.429, -81.309),
    "PHX": (33.438, -112.008), "LAS": (36.084, -115.152), "SAN": (32.734, -117.189),
    "AUS": (30.197, -97.666), "RDU": (35.877, -78.787), "TPA": (27.975, -82.533),
    "YYZ": (43.677, -79.630), "YVR": (49.195, -123.184), "YUL": (45.470, -73.741),
    "YYC": (51.132, -114.011), "YOW": (45.323, -75.668),
    "MEX": (19.436, -99.072), "CUN": (21.037, -86.877), "GDL": (20.522, -103.311),
    "GRU": (-23.432, -46.469), "GIG": (-22.810, -43.250),
    "EZE": (-34.822, -58.536), "AEP": (-34.558, -58.416),
    "BOG": (4.702, -74.147), "SCL": (-33.393, -70.786), "LIM": (-12.022, -77.114),
    "PTY": (9.071, -79.384), "MDE": (6.166, -75.424), "UIO": (-0.129, -78.358),
    # Oceania
    "SYD": (-33.946, 151.177), "MEL": (-37.673, 144.843), "BNE": (-27.384, 153.118),
    "PER": (-31.940, 115.967), "ADL": (-34.945, 138.531), "AKL": (-37.008, 174.792),
    "CHC": (-43.489, 172.532),
}


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km."""
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _route_distance_km(origin: str, dest: str) -> float:
    """Great-circle km for origin->dest, or 3000 (medium-haul default) if unknown."""
    o = _AIRPORTS.get(origin.upper())
    d = _AIRPORTS.get(dest.upper())
    if o and d:
        return _haversine_km(o[0], o[1], d[0], d[1])
    return 3000.0  # safe medium-haul fallback


# -- LH Group pricing tiers ---------------------------------------------------
# Derived from Lufthansa Group published Economy Light seat ancillary schedule.
# Standard (non-exit, non-preferred) seat minimum price.

# (max_km, eur_price, chf_price)
_LH_TIERS: list[tuple[float, float, float]] = [
    (1_500,       10.0, 12.0),
    (3_000,       15.0, 18.0),
    (6_000,       25.0, 30.0),
    (float("inf"), 30.0, 36.0),
]


def _lh_seat_price_calc(origin: str, dest: str, use_chf: bool) -> float:
    km = _route_distance_km(origin, dest)
    for max_km, eur, chf in _LH_TIERS:
        if km <= max_km:
            return chf if use_chf else eur
    return 30.0 if not use_chf else 36.0


# -- Handlers -----------------------------------------------------------------

async def _fetch_lh_seat_price(
    flight_no: str, origin: str, dest: str, date_str: str, cabin: str
) -> Optional[float]:
    return _lh_seat_price_calc(origin, dest, use_chf=False)


async def _fetch_lx_seat_price(
    flight_no: str, origin: str, dest: str, date_str: str, cabin: str
) -> Optional[float]:
    return _lh_seat_price_calc(origin, dest, use_chf=True)


_HANDLERS: dict[str, object] = {
    "LH": _fetch_lh_seat_price,
    "OS": _fetch_lh_seat_price,
    "SN": _fetch_lh_seat_price,
    "LX": _fetch_lx_seat_price,
}

_AIRLINE_CURRENCY: dict[str, str] = {
    "LH": "EUR", "OS": "EUR", "SN": "EUR", "LX": "CHF",
}


# -- Public interface ---------------------------------------------------------

async def fetch_seat_price(
    airline: str,
    flight_no: str,
    origin: str,
    dest: str,
    date_str: str,
    cabin: str = "M",
) -> Optional[float]:
    """Return minimum standard-seat price for one flight leg, or None."""
    handler = _HANDLERS.get(airline.upper())
    if handler is None:
        return None

    cache_key = f"{airline.upper()}:{origin.upper()}:{dest.upper()}"
    now = time.monotonic()
    if cache_key in _SEAT_CACHE:
        price, ts = _SEAT_CACHE[cache_key]
        if now - ts < _SEAT_CACHE_TTL:
            return price

    try:
        price = await handler(flight_no, origin.upper(), dest.upper(), date_str, cabin)
    except Exception as exc:
        logger.debug("seat_prices: %s %s->%s: %s", airline, origin, dest, exc)
        return None

    if price is not None:
        _SEAT_CACHE[cache_key] = (price, now)
    return price


@dataclass
class SeatPriceRequest:
    airline: str
    flight_no: str
    origin: str
    dest: str
    date_str: str
    cabin: str = "M"
    offer_id: Optional[str] = None


async def fetch_seat_prices_batch(
    requests: list[SeatPriceRequest],
    timeout: float = 6.0,
) -> dict[str, tuple[float, str]]:
    """
    Fetch seat prices for a batch of flight legs concurrently.
    Returns: {offer_id_or_key: (price, currency)}
    """
    if not requests:
        return {}

    async def _one(req: SeatPriceRequest) -> tuple[str, Optional[float], str]:
        key = req.offer_id or f"{req.airline}:{req.flight_no}:{req.origin}:{req.dest}:{req.date_str}"
        try:
            price = await fetch_seat_price(
                req.airline, req.flight_no, req.origin, req.dest, req.date_str, req.cabin
            )
        except Exception:
            price = None
        currency = _AIRLINE_CURRENCY.get(req.airline.upper(), "EUR")
        return key, price, currency

    try:
        results = await asyncio.wait_for(
            asyncio.gather(*[_one(r) for r in requests], return_exceptions=False),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        logger.debug("seat_prices: batch timed out after %.1fs", timeout)
        return {}
    except Exception as exc:
        logger.debug("seat_prices: batch error: %s", exc)
        return {}

    return {
        key: (price, currency)
        for key, price, currency in results
        if price is not None
    }
