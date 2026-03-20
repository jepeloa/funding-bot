"""
Ψ-JAM Section 7d: Pseudo-L3 Ghost Flow
=======================================
Estimate hidden directional intent by cross-referencing L2 order book snapshots
with aggregated trade data (pseudo-L3).

Classifies each price level as GHOST, FILLED, ICEBERG, or MIXED.
Outputs a Directional Liquidity Imbalance (DLI) score indicating probable price direction.

Literature:
  - Frey & Sandås (2009, 2017): Iceberg detection via replenishment
  - Zotikov (2019): CME iceberg detection/prediction
  - Li et al. (2023): Particle momentum for spoofing detection
  - Debie et al. (2022): Message-level visualization for manipulation
"""

import asyncio
from collections import defaultdict
from dataclasses import dataclass, field


@dataclass
class ClassifiedLevel:
    price: float
    side: str  # 'bid' or 'ask'
    qty_snap1: float
    qty_snap2: float
    trades: float
    disappeared: float
    fill_rate: float
    replenishment: float
    replenishment_ratio: float
    ghost_confidence: float
    classification: str
    estimated_hidden: float = 0.0


async def analyze_ghost_flow(
    client,
    symbol: str,
    delay_seconds: float = 15,
    depth: int = 20,
    trades_limit: int = 1000,
    ghost_threshold: float = 0.7,
    iceberg_threshold: float = 1.5,
) -> dict:
    """
    Full pseudo-L3 ghost flow analysis.

    Makes 4 API calls (2x orderbook + 2x agg_trades) with a configurable delay
    between snapshot pairs.
    """
    symbol = symbol.upper()

    # ── CALL 1 & 2: First snapshot + trades buffer ──
    snap1_raw = await client.get_orderbook(symbol, limit=depth)
    trades_buf1 = await client.get_agg_trades(symbol, limit=trades_limit)

    snap1_time = snap1_raw["timestamp"]

    # ── WAIT ──
    await asyncio.sleep(delay_seconds)

    # ── CALL 3 & 4: Second snapshot + trades buffer update ──
    snap2_raw = await client.get_orderbook(symbol, limit=depth)
    trades_buf2 = await client.get_agg_trades(symbol, limit=trades_limit)

    snap2_time = snap2_raw["timestamp"]
    delta_seconds = (snap2_time - snap1_time) / 1000.0

    # ── Parse snapshots into price→qty dicts ──
    snap1_bids = {p: q for p, q in snap1_raw["bids"]}
    snap1_asks = {p: q for p, q in snap1_raw["asks"]}
    snap2_bids = {p: q for p, q in snap2_raw["bids"]}
    snap2_asks = {p: q for p, q in snap2_raw["asks"]}

    mid_price_start = snap1_raw["metrics"]["mid_price"]
    mid_price_end = snap2_raw["metrics"]["mid_price"]
    price_change_pct = ((mid_price_end - mid_price_start) / mid_price_start * 100) if mid_price_start else 0

    # ── Merge all trades, deduplicate by agg_id ──
    seen_ids = set()
    all_trades = []
    for t in trades_buf1 + trades_buf2:
        if t["agg_id"] not in seen_ids:
            seen_ids.add(t["agg_id"])
            all_trades.append(t)

    # ── Filter trades in the window ──
    trades_window = [t for t in all_trades if snap1_time <= t["time"] <= snap2_time]

    # ── Aggregate trades by price and side ──
    bid_consumed: dict[float, float] = defaultdict(float)
    ask_consumed: dict[float, float] = defaultdict(float)

    for trade in trades_window:
        if trade["is_buyer_maker"]:  # sell aggressor → consumes bids
            bid_consumed[trade["price"]] += trade["qty"]
        else:  # buy aggressor → consumes asks
            ask_consumed[trade["price"]] += trade["qty"]

    # ── Classify each price level ──
    classified_levels: list[ClassifiedLevel] = []

    def _classify_levels(snap1_side, snap2_side, consumed, side_name):
        all_prices = set(snap1_side.keys()) | set(snap2_side.keys())
        for p in all_prices:
            qty_snap1 = snap1_side.get(p, 0)
            qty_snap2 = snap2_side.get(p, 0)
            trades_at_p = consumed.get(p, 0)

            if qty_snap1 == 0:
                # Level didn't exist in snap1 → NEW
                classified_levels.append(ClassifiedLevel(
                    price=p, side=side_name,
                    qty_snap1=0, qty_snap2=qty_snap2,
                    trades=trades_at_p, disappeared=0,
                    fill_rate=0, replenishment=qty_snap2,
                    replenishment_ratio=0, ghost_confidence=0,
                    classification="NEW",
                ))
                continue

            disappeared = max(qty_snap1 - qty_snap2, 0)

            fill_rate = min(trades_at_p / max(disappeared, 1e-12), 1.0) if disappeared > 0 else (1.0 if trades_at_p > 0 else 0.0)

            ghost_confidence = 1.0 - fill_rate

            expected_remaining = max(qty_snap1 - trades_at_p, 0)
            replenishment = max(qty_snap2 - expected_remaining, 0)
            replenishment_ratio = replenishment / max(qty_snap1, 1e-12)

            estimated_hidden = 0.0

            if disappeared == 0 and trades_at_p == 0:
                classification = "PERSISTS"
            elif trades_at_p == 0 and qty_snap2 == 0:
                classification = "GHOST_PURE"
                ghost_confidence = 1.0
            elif fill_rate < ghost_threshold:
                classification = "GHOST"
            elif (trades_at_p > qty_snap1 * 0.8
                  and qty_snap2 > qty_snap1 * 0.5
                  and replenishment_ratio > iceberg_threshold):
                classification = "ICEBERG"
                estimated_hidden = qty_snap2 * 3  # 3 more tranches (Frey & Sandås)
            elif fill_rate >= ghost_threshold:
                classification = "FILLED"
            else:
                classification = "MIXED"

            classified_levels.append(ClassifiedLevel(
                price=p, side=side_name,
                qty_snap1=qty_snap1, qty_snap2=qty_snap2,
                trades=trades_at_p, disappeared=disappeared,
                fill_rate=fill_rate, replenishment=replenishment,
                replenishment_ratio=replenishment_ratio,
                ghost_confidence=ghost_confidence,
                classification=classification,
                estimated_hidden=estimated_hidden,
            ))

    _classify_levels(snap1_bids, snap2_bids, bid_consumed, "bid")
    _classify_levels(snap1_asks, snap2_asks, ask_consumed, "ask")

    # ── Per-side aggregation ──
    def _aggregate_side(side_name):
        levels = [l for l in classified_levels if l.side == side_name]
        total_disappeared = sum(l.disappeared for l in levels)
        total_filled = sum(min(l.trades, l.disappeared) for l in levels)
        total_cancelled = total_disappeared - total_filled
        total_replenished = sum(l.replenishment for l in levels)
        icebergs = [l for l in levels if l.classification == "ICEBERG"]

        ghost_rate = total_cancelled / max(total_disappeared, 1)
        fill_rate = total_filled / max(total_disappeared, 1)
        repl_rate = total_replenished / max(total_filled, 1)

        # Effective depth
        snap2_depth = snap2_raw["metrics"]["bid_depth_total"] if side_name == "bid" else snap2_raw["metrics"]["ask_depth_total"]
        effective_depth = snap2_depth * (1 - ghost_rate)

        return {
            "total_disappeared": round(total_disappeared, 2),
            "total_filled": round(total_filled, 2),
            "total_cancelled": round(total_cancelled, 2),
            "ghost_rate": round(ghost_rate, 3),
            "fill_rate": round(fill_rate, 3),
            "repl_rate": round(repl_rate, 3),
            "replenishment_total": round(total_replenished, 2),
            "icebergs_detected": len(icebergs),
            "effective_depth": round(effective_depth, 2),
        }, ghost_rate, levels, icebergs

    bid_analysis, ghost_rate_bid, bid_levels, bid_icebergs = _aggregate_side("bid")
    ask_analysis, ghost_rate_ask, ask_levels, ask_icebergs = _aggregate_side("ask")

    all_icebergs = bid_icebergs + ask_icebergs

    # ── GAS (Ghost Asymmetry Score) ──
    gas_denom = max(ghost_rate_ask + ghost_rate_bid, 0.01)
    GAS = (ghost_rate_ask - ghost_rate_bid) / gas_denom

    if GAS > 0.2:
        gas_interp = "Más ghost asks → manipulador finge resistencia → intento alcista oculto"
    elif GAS < -0.2:
        gas_interp = "Más ghost bids → manipulador finge soporte → intento bajista oculto"
    else:
        gas_interp = "Manipulación simétrica o baja actividad ghost"

    # ── DLI (Directional Liquidity Imbalance) ──
    real_bid_liq = sum(
        l.qty_snap2 * l.fill_rate + l.replenishment
        for l in bid_levels
    )
    real_ask_liq = sum(
        l.qty_snap2 * l.fill_rate + l.replenishment
        for l in ask_levels
    )
    DLI = (real_bid_liq - real_ask_liq) / max(real_bid_liq + real_ask_liq, 1)

    if DLI > 0.3:
        dli_interp = "Bids reales >> asks reales → piso fuerte, difícil que caiga"
    elif DLI > 0.1:
        dli_interp = "Liquidez real sesgada a bids → soporte moderado"
    elif DLI > -0.1:
        dli_interp = "Liquidez real balanceada → sin sesgo direccional"
    elif DLI > -0.3:
        dli_interp = "Liquidez real sesgada a asks → techo moderado"
    else:
        dli_interp = "Asks reales >> bids reales → techo fuerte, difícil que suba → DOWN"

    # ── Ghost-Adjusted Depth Ratio Delta ──
    net_sell_aggression = sum(bid_consumed.values())
    net_buy_aggression = sum(ask_consumed.values())
    net_flow = net_buy_aggression - net_sell_aggression

    snap1_total_depth = snap1_raw["metrics"]["bid_depth_total"] + snap1_raw["metrics"]["ask_depth_total"]
    organic_DR_delta = -net_flow / max(snap1_total_depth, 1)

    snap1_dr = snap1_raw["metrics"]["depth_ratio"]
    snap2_dr = snap2_raw["metrics"]["depth_ratio"]
    actual_DR_delta = snap2_dr - snap1_dr
    ghost_DR_component = actual_DR_delta - organic_DR_delta

    # ── Iceberg directional signal ──
    iceberg_signal = 0.0
    for level in all_icebergs:
        if level.side == "ask":
            iceberg_signal -= level.estimated_hidden
        else:
            iceberg_signal += level.estimated_hidden

    iceberg_direction = iceberg_signal / max(abs(iceberg_signal), 1) if iceberg_signal != 0 else 0.0

    if iceberg_direction > 0.3:
        ice_interp = "Iceberg en bids absorbiendo vendedores → alcista"
    elif iceberg_direction < -0.3:
        ice_interp = "Iceberg en asks absorbiendo compradores → bajista"
    else:
        ice_interp = "Sin señal de iceberg direccional significativa"

    # ── Real support/resistance levels ──
    real_support = sorted([
        {
            "price": l.price,
            "strength": round(l.fill_rate * l.qty_snap2 + l.replenishment, 2),
            "classification": l.classification,
            **({"estimated_hidden": round(l.estimated_hidden, 2)} if l.classification == "ICEBERG" else {}),
        }
        for l in bid_levels
        if l.classification in ("FILLED", "ICEBERG", "MIXED") and l.fill_rate > 0.5
    ], key=lambda x: -x["strength"])

    real_resistance = sorted([
        {
            "price": l.price,
            "strength": round(l.fill_rate * l.qty_snap2 + l.replenishment, 2),
            "classification": l.classification,
            **({"estimated_hidden": round(l.estimated_hidden, 2)} if l.classification == "ICEBERG" else {}),
        }
        for l in ask_levels
        if l.classification in ("FILLED", "ICEBERG", "MIXED") and l.fill_rate > 0.5
    ], key=lambda x: -x["strength"])

    # ── Level detail (only interesting levels, not PERSISTS/NEW) ──
    level_detail = sorted([
        {
            "price": l.price,
            "side": l.side,
            "qty_snap1": round(l.qty_snap1, 2),
            "qty_snap2": round(l.qty_snap2, 2),
            "trades": round(l.trades, 2),
            "fill_rate": round(l.fill_rate, 3),
            "replenishment": round(l.replenishment, 2),
            "replenishment_ratio": round(l.replenishment_ratio, 3),
            "ghost_confidence": round(l.ghost_confidence, 3),
            "classification": l.classification,
            **({"estimated_hidden": round(l.estimated_hidden, 2)} if l.estimated_hidden > 0 else {}),
        }
        for l in classified_levels
        if l.classification not in ("PERSISTS", "NEW")
    ], key=lambda x: -x["ghost_confidence"])

    # ── Verdict ──
    verdict = _compute_verdict(
        DLI=DLI,
        GAS=GAS,
        iceberg_direction=iceberg_direction,
        ghost_DR_component=ghost_DR_component,
        all_icebergs=all_icebergs,
        real_support=real_support,
        real_resistance=real_resistance,
        bid_levels=bid_levels,
        ask_levels=ask_levels,
    )

    return {
        "symbol": symbol,
        "snap1_time": snap1_time,
        "snap2_time": snap2_time,
        "delta_seconds": round(delta_seconds, 1),
        "mid_price_start": mid_price_start,
        "mid_price_end": mid_price_end,
        "price_change_pct": round(price_change_pct, 3),
        "trades_in_window": len(trades_window),
        "bid_analysis": bid_analysis,
        "ask_analysis": ask_analysis,
        "signals": {
            "GAS": round(GAS, 3),
            "GAS_interpretation": gas_interp,
            "DLI": round(DLI, 3),
            "DLI_interpretation": dli_interp,
            "ghost_DR_component": round(ghost_DR_component, 4),
            "effective_bid_depth": bid_analysis["effective_depth"],
            "effective_ask_depth": ask_analysis["effective_depth"],
            "iceberg_direction": round(iceberg_direction, 3),
            "iceberg_direction_interpretation": ice_interp,
        },
        "real_support": real_support[:5],
        "real_resistance": real_resistance[:5],
        "level_detail": level_detail[:20],
        "verdict": verdict,
    }


def _compute_verdict(
    DLI: float,
    GAS: float,
    iceberg_direction: float,
    ghost_DR_component: float,
    all_icebergs: list,
    real_support: list,
    real_resistance: list,
    bid_levels: list,
    ask_levels: list,
) -> dict:
    """Compute directional verdict from ghost flow signals."""
    score = 0.0
    reasons = []

    # DLI is the primary signal (weight: 40%)
    score += DLI * 40
    reasons.append(f"DLI={DLI:+.3f} (×40)")

    # Iceberg direction (weight: 30%)
    score += iceberg_direction * 30
    if iceberg_direction != 0:
        reasons.append(f"Iceberg dir={iceberg_direction:+.1f} (×30)")

    # GAS: if no icebergs, manipulation signal dominates; otherwise iceberg overrides
    if not all_icebergs:
        score += GAS * 15
        reasons.append(f"GAS={GAS:+.3f} (×15, sin iceberg)")
    else:
        score -= GAS * 10
        reasons.append(f"GAS={GAS:+.3f} invertido (×10, iceberg anula manipulación)")

    # Ghost DR component (weight: 15%)
    score += ghost_DR_component * 15
    if abs(ghost_DR_component) > 0.05:
        reasons.append(f"Ghost DR={ghost_DR_component:+.4f} (×15)")

    # Direction
    if score < -10:
        direction = "DOWN"
    elif score > 10:
        direction = "UP"
    else:
        direction = "NEUTRAL"

    confidence = min(abs(score) / 50, 1.0)

    # Ghost floor/ceiling from classified levels
    ghost_bids = [l for l in bid_levels if l.classification in ("GHOST", "GHOST_PURE")]
    ghost_asks = [l for l in ask_levels if l.classification in ("GHOST", "GHOST_PURE")]

    ghost_floor = max((l.price for l in ghost_bids), default=None)
    ghost_ceiling = min((l.price for l in ghost_asks), default=None)

    real_ceiling = real_resistance[0]["price"] if real_resistance else None
    real_floor = real_support[0]["price"] if real_support else None

    return {
        "direction": direction,
        "confidence": round(confidence, 2),
        "score": round(score, 1),
        "for_short": "FAVORABLE" if direction == "DOWN" else ("DESFAVORABLE" if direction == "UP" else "NEUTRAL"),
        "reasoning": " + ".join(reasons),
        "real_ceiling": real_ceiling,
        "real_floor": real_floor,
        "ghost_floor": ghost_floor,
        "ghost_ceiling": ghost_ceiling,
    }
