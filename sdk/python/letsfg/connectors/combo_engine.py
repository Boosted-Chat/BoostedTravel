"""
Round-trip combo engine — same-airline RT + cross-airline virtual interlining.

For round-trip searches the engine fires every connector twice (outbound +
reversed inbound).  This module pairs those one-way legs into proper offers:

  1. **Same-airline RT** — outbound + return from the SAME connector.
     Presented as native round-trip offers (source = connector, no
     "virtual_interlining" label, proper booking URL).  Processed first
     so they appear before cross-airline combos in the results.

  2. **Cross-airline combos** — outbound from one airline, return from
     another (e.g. Ryanair out + Wizzair back).  Labelled as virtual
     interlining with split booking URLs.

Every connector instantly "supports" round-trip through this engine —
no per-connector RT code needed.
"""

from __future__ import annotations

import hashlib
import logging

from ..models.flights import (
    FlightOffer,
    FlightRoute,
)
from .currency import _fallback_convert

logger = logging.getLogger(__name__)

# Max one-way legs to keep per direction per source (prevents combinatorial explosion)
_MAX_LEGS_PER_SOURCE = 10
# Max total combos to generate
_MAX_COMBOS = 150


def _leg_key(route: FlightRoute) -> str:
    """Unique key for a leg based on flights and timing."""
    if not route or not route.segments:
        return ""
    parts = []
    for seg in route.segments:
        try:
            dep_str = seg.departure.isoformat() if seg.departure else "?"
        except Exception:
            dep_str = str(seg.departure)
        parts.append(f"{seg.flight_no}_{dep_str}")
    return "|".join(parts)


def build_combos(
    outbound_offers: list[FlightOffer],
    return_offers: list[FlightOffer],
    target_currency: str,
    max_combos: int = _MAX_COMBOS,
) -> list[FlightOffer]:
    """
    Build round-trip offers from one-way outbound + one-way return legs.

    Same-source pairs are presented as native RT offers (``rt_`` prefix,
    connector's own source/airline).  Cross-source pairs become virtual-
    interlining combos (``combo_`` prefix, split booking URLs).

    Same-airline RT offers are generated first so they take priority.
    """
    if not outbound_offers or not return_offers:
        return []
    combo_limit = max(1, max_combos)

    # Deduplicate legs per direction PER SOURCE — keeps each source's version
    # of the same flight so cross-source combos always have both sides.
    out_by_source: dict[str, dict[str, FlightOffer]] = {}
    for o in outbound_offers:
        if not o.outbound:
            continue
        key = _leg_key(o.outbound)
        if not key:
            continue
        src = o.source
        if src not in out_by_source:
            out_by_source[src] = {}
        if key not in out_by_source[src] or o.price < out_by_source[src][key].price:
            out_by_source[src][key] = o

    ret_by_source: dict[str, dict[str, FlightOffer]] = {}
    for r in return_offers:
        if not r.outbound:
            continue
        key = _leg_key(r.outbound)
        if not key:
            continue
        src = r.source
        if src not in ret_by_source:
            ret_by_source[src] = {}
        if key not in ret_by_source[src] or r.price < ret_by_source[src][key].price:
            ret_by_source[src][key] = r

    # Keep top N per source (sorted by price_normalized, fallback to price)
    def _sort_price(o: FlightOffer) -> float:
        return o.price_normalized if o.price_normalized is not None else o.price

    out_trimmed: dict[str, list[FlightOffer]] = {}
    for src, by_key in out_by_source.items():
        out_trimmed[src] = sorted(by_key.values(), key=_sort_price)[:_MAX_LEGS_PER_SOURCE]

    ret_trimmed: dict[str, list[FlightOffer]] = {}
    for src, by_key in ret_by_source.items():
        ret_trimmed[src] = sorted(by_key.values(), key=_sort_price)[:_MAX_LEGS_PER_SOURCE]

    combos: list[FlightOffer] = []
    seen_combo_keys: set[str] = set()

    # ── Separate same-source (same-airline RT) and cross-source pairs ──
    # Same-source combos look like native round-trip offers (not "virtual
    # interlining") — they carry the connector's own source, airline name,
    # and booking URL so the UX is indistinguishable from a connector that
    # natively searched round-trip.  Cross-source combos keep the existing
    # virtual-interlining presentation (split booking URLs, combo: source).
    out_sources = list(out_trimmed.keys())
    ret_sources = list(ret_trimmed.keys())

    same_source_pairs: list[tuple[FlightOffer, FlightOffer]] = []
    cross_source_pairs: list[tuple[FlightOffer, FlightOffer]] = []

    for src_a in out_sources:
        for src_b in ret_sources:
            for ob in out_trimmed[src_a]:
                for rt in ret_trimmed[src_b]:
                    if src_a == src_b:
                        same_source_pairs.append((ob, rt))
                    else:
                        cross_source_pairs.append((ob, rt))

    same_source_pairs.sort(key=lambda p: _sort_price(p[0]) + _sort_price(p[1]))

    def _cross_sort_price(pair: tuple[FlightOffer, FlightOffer]) -> float:
        ob, rt = pair
        # When both legs have price_normalized, use that for sorting.
        # Otherwise convert to target_currency so we never compare raw
        # numbers from different currencies.
        ob_p = ob.price_normalized if ob.price_normalized is not None else _fallback_convert(ob.price, ob.currency, target_currency)
        rt_p = rt.price_normalized if rt.price_normalized is not None else _fallback_convert(rt.price, rt.currency, target_currency)
        return ob_p + rt_p

    cross_source_pairs.sort(key=_cross_sort_price)

    def _make_offer(
        ob: FlightOffer, rt: FlightOffer, *, same_source: bool,
    ) -> FlightOffer:
        """Build a combined RT offer from outbound + return one-way legs."""
        # Price
        if ob.price_normalized is not None and rt.price_normalized is not None:
            total_normalized = ob.price_normalized + rt.price_normalized
        else:
            total_normalized = None

        total_price = ob.price + rt.price
        if ob.currency == rt.currency:
            combo_currency = ob.currency
            combo_price = total_price
        else:
            combo_currency = target_currency
            if total_normalized:
                combo_price = total_normalized
            else:
                # Convert each leg to target currency before adding — never
                # mix raw prices from different currencies.
                ob_converted = _fallback_convert(ob.price, ob.currency, target_currency)
                rt_converted = _fallback_convert(rt.price, rt.currency, target_currency)
                combo_price = ob_converted + rt_converted

        # Airlines — outbound first, then inbound-only carriers
        ob_airlines = list(dict.fromkeys(ob.airlines)) if ob.airlines else []
        rt_airlines = list(dict.fromkeys(rt.airlines)) if rt.airlines else []
        ob_set = set(ob_airlines)
        all_airlines = ob_airlines + [a for a in rt_airlines if a not in ob_set]

        combo_hash = hashlib.md5(
            f"{ob.id[:8]}{rt.id[:8]}".encode()
        ).hexdigest()[:12]

        if same_source:
            # Same-airline round-trip — looks like a native RT offer
            # Merge original conditions with per-leg booking URLs so both
            # outbound and return links are available to the consumer.
            _rt_conds = dict(ob.conditions or {})
            _rt_conds["outbound_booking_url"] = ob.booking_url or ""
            _rt_conds["inbound_booking_url"] = rt.booking_url or ""
            return FlightOffer(
                id=f"rt_{combo_hash}",
                price=round(combo_price, 2),
                currency=combo_currency,
                price_formatted=f"{combo_price:.2f} {combo_currency}",
                price_normalized=total_normalized,
                outbound=ob.outbound,
                inbound=rt.outbound,
                airlines=all_airlines,
                owner_airline=ob.owner_airline or (all_airlines[0] if all_airlines else ""),
                conditions=_rt_conds,
                booking_url=ob.booking_url or "",
                is_locked=False,
                source=ob.source,
                source_tier="free",
            )
        # Cross-airline virtual interlining
        return FlightOffer(
            id=f"combo_{combo_hash}",
            price=round(combo_price, 2),
            currency=combo_currency,
            price_formatted=f"{combo_price:.2f} {combo_currency}",
            price_normalized=total_normalized,
            outbound=ob.outbound,
            inbound=rt.outbound,
            airlines=all_airlines,
            owner_airline="|".join(all_airlines),
            conditions={
                "combo_type": "virtual_interlining",
                "outbound_booking_url": ob.booking_url or "",
                "inbound_booking_url": rt.booking_url or "",
                "outbound_source": ob.source,
                "inbound_source": rt.source,
            },
            booking_url="",
            is_locked=False,
            source=f"combo:{ob.source}+{rt.source}",
            source_tier="free",
        )

    rt_count = 0
    cross_count = 0

    # ── Phase 1: Same-airline round-trips (processed first — higher quality) ──
    for ob, rt in same_source_pairs:
        if len(combos) >= combo_limit:
            break
        ob_key = _leg_key(ob.outbound)
        rt_key = _leg_key(rt.outbound)
        combo_key = f"{ob_key}::{rt_key}"
        if combo_key in seen_combo_keys:
            continue
        seen_combo_keys.add(combo_key)
        combos.append(_make_offer(ob, rt, same_source=True))
        rt_count += 1

    # ── Phase 2: Cross-airline virtual interlining ──
    for ob, rt in cross_source_pairs:
        if len(combos) >= combo_limit:
            break
        ob_key = _leg_key(ob.outbound)
        rt_key = _leg_key(rt.outbound)
        combo_key = f"{ob_key}::{rt_key}"
        if combo_key in seen_combo_keys:
            continue
        seen_combo_keys.add(combo_key)
        combos.append(_make_offer(ob, rt, same_source=False))
        cross_count += 1

    logger.info(
        "Combo engine: %d RT (same-airline) + %d cross-airline combos "
        "from %d sources out × %d sources ret",
        rt_count, cross_count, len(out_sources), len(ret_sources),
    )
    return combos
