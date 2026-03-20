"""
Range Asymmetry Analysis Module
================================
Evaluates risk/reward asymmetry for SHORT (or LONG) entries based on
full historical cycle detection: pump→dump zigzag, retrace statistics,
win rate simulation, funding carry, volume structure, and composite scoring.

Question answered: "At $X, is the downside potential greater than the upside risk?"
"""

import numpy as np
from typing import Optional


# ═══════════════════════════════════════════════════
# SECTION 1: ADAPTIVE ZIGZAG CYCLE DETECTION
# ═══════════════════════════════════════════════════

def _compute_atr(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 14) -> np.ndarray:
    """Average True Range."""
    prev_close = np.concatenate([[closes[0]], closes[:-1]])
    tr = np.maximum(
        highs - lows,
        np.maximum(np.abs(highs - prev_close), np.abs(lows - prev_close)),
    )
    atr = np.empty_like(tr)
    atr[:period] = np.nan
    atr[period - 1] = np.mean(tr[:period])
    for i in range(period, len(tr)):
        atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period
    return atr


def _adaptive_zigzag(
    highs: np.ndarray,
    lows: np.ndarray,
    closes: np.ndarray,
    atr_mult: float = 2.0,
    atr_period: int = 14,
    min_pct_threshold: float = 5.0,
) -> list[dict]:
    """
    Adaptive zigzag using ATR-based threshold.
    Returns list of swing points: {index, price, type: 'HIGH'|'LOW'}.
    """
    n = len(closes)
    if n < atr_period + 5:
        return []

    atr = _compute_atr(highs, lows, closes, atr_period)

    swings = []
    # Initialize: find first valid direction
    start_idx = atr_period
    last_high_idx = start_idx
    last_high = highs[start_idx]
    last_low_idx = start_idx
    last_low = lows[start_idx]
    direction = 0  # 0=undecided, 1=looking for high (uptrend), -1=looking for low (downtrend)

    for i in range(start_idx + 1, n):
        threshold = max(atr[i] * atr_mult, closes[i] * min_pct_threshold / 100)

        if direction >= 0:  # looking for higher high or reversal down
            if highs[i] > last_high:
                last_high = highs[i]
                last_high_idx = i
            elif last_high - lows[i] >= threshold:
                # Confirmed swing high
                swings.append({"index": last_high_idx, "price": last_high, "type": "HIGH"})
                direction = -1
                last_low = lows[i]
                last_low_idx = i

        if direction <= 0:  # looking for lower low or reversal up
            if lows[i] < last_low:
                last_low = lows[i]
                last_low_idx = i
            elif highs[i] - last_low >= threshold:
                # Confirmed swing low
                swings.append({"index": last_low_idx, "price": last_low, "type": "LOW"})
                direction = 1
                last_high = highs[i]
                last_high_idx = i

    # Remove duplicates and ensure alternation
    cleaned = []
    for s in swings:
        if not cleaned or cleaned[-1]["type"] != s["type"]:
            cleaned.append(s)
        else:
            # Keep the more extreme one
            if s["type"] == "HIGH" and s["price"] > cleaned[-1]["price"]:
                cleaned[-1] = s
            elif s["type"] == "LOW" and s["price"] < cleaned[-1]["price"]:
                cleaned[-1] = s

    return cleaned


def detect_cycles(
    highs: np.ndarray,
    lows: np.ndarray,
    closes: np.ndarray,
    open_times: list[int],
    side: str = "SHORT",
    atr_mult: float = 2.0,
    min_pct_threshold: float = 5.0,
) -> dict:
    """
    Detect pump→dump cycles (or dump→pump if LONG).

    Returns:
      cycles_detected, current_state, current_pump, cycles list
    """
    n = len(closes)
    if n < 30:
        return {
            "cycles_detected": 0,
            "current_state": "INSUFFICIENT_HISTORY",
            "current_pump": None,
            "cycles": [],
            "swings": [],
            "warning": f"Solo {n} velas disponibles. Se necesitan >= 30 para análisis confiable.",
        }

    swings = _adaptive_zigzag(highs, lows, closes, atr_mult=atr_mult, min_pct_threshold=min_pct_threshold)

    if len(swings) < 2:
        return {
            "cycles_detected": 0,
            "current_state": "NO_SWINGS_DETECTED",
            "current_pump": None,
            "cycles": [],
            "swings": [],
            "warning": "No se detectaron swings significativos con el threshold actual.",
        }

    # Build cycles: pump (LOW→HIGH) followed by dump (HIGH→LOW)
    cycles = []
    if side == "SHORT":
        # We want: LOW → HIGH → LOW = pump then dump
        for i in range(len(swings) - 2):
            s1, s2, s3 = swings[i], swings[i + 1], swings[i + 2]
            if s1["type"] == "LOW" and s2["type"] == "HIGH" and s3["type"] == "LOW":
                pump_pct = (s2["price"] - s1["price"]) / s1["price"] * 100
                dump_pct = (s2["price"] - s3["price"]) / s2["price"] * 100
                pump_range = s2["price"] - s1["price"]
                dump_range = s2["price"] - s3["price"]
                retrace_ratio = dump_range / pump_range if pump_range > 0 else 0

                pump_days = (open_times[s2["index"]] - open_times[s1["index"]]) / 86400000
                dump_days = (open_times[s3["index"]] - open_times[s2["index"]]) / 86400000

                cycles.append({
                    "pump_start_price": round(s1["price"], 6),
                    "pump_peak_price": round(s2["price"], 6),
                    "dump_bottom_price": round(s3["price"], 6),
                    "pump_start_idx": s1["index"],
                    "pump_peak_idx": s2["index"],
                    "dump_bottom_idx": s3["index"],
                    "pump_pct": round(pump_pct, 2),
                    "dump_pct": round(dump_pct, 2),
                    "retrace_ratio": round(retrace_ratio, 4),
                    "pump_days": round(pump_days, 1),
                    "dump_days": round(dump_days, 1),
                    "net_change_pct": round((s3["price"] - s1["price"]) / s1["price"] * 100, 2),
                })
    else:
        # LONG: HIGH → LOW → HIGH = dump then pump
        for i in range(len(swings) - 2):
            s1, s2, s3 = swings[i], swings[i + 1], swings[i + 2]
            if s1["type"] == "HIGH" and s2["type"] == "LOW" and s3["type"] == "HIGH":
                dump_pct = (s1["price"] - s2["price"]) / s1["price"] * 100
                pump_pct = (s3["price"] - s2["price"]) / s2["price"] * 100
                dump_range = s1["price"] - s2["price"]
                pump_range = s3["price"] - s2["price"]
                retrace_ratio = pump_range / dump_range if dump_range > 0 else 0

                dump_days = (open_times[s2["index"]] - open_times[s1["index"]]) / 86400000
                pump_days = (open_times[s3["index"]] - open_times[s2["index"]]) / 86400000

                cycles.append({
                    "dump_start_price": round(s1["price"], 6),
                    "dump_bottom_price": round(s2["price"], 6),
                    "pump_peak_price": round(s3["price"], 6),
                    "dump_start_idx": s1["index"],
                    "dump_bottom_idx": s2["index"],
                    "pump_peak_idx": s3["index"],
                    "dump_pct": round(dump_pct, 2),
                    "pump_pct": round(pump_pct, 2),
                    "retrace_ratio": round(retrace_ratio, 4),
                    "dump_days": round(dump_days, 1),
                    "pump_days": round(pump_days, 1),
                    "net_change_pct": round((s3["price"] - s1["price"]) / s1["price"] * 100, 2),
                })

    # Determine current state
    current_price = float(closes[-1])
    current_state = "CONSOLIDATION"
    current_pump = None

    if swings:
        last_swing = swings[-1]
        if side == "SHORT":
            if last_swing["type"] == "LOW":
                # Price rose from last low → potential pump active
                pct_from_start = (current_price - last_swing["price"]) / last_swing["price"] * 100
                if pct_from_start > min_pct_threshold:
                    current_state = "PUMP_ACTIVE"
                    pump_start_idx = last_swing["index"]
                    # Find peak in this pump
                    peak_price = float(np.max(highs[pump_start_idx:]))
                    peak_idx = pump_start_idx + int(np.argmax(highs[pump_start_idx:]))
                    pct_from_peak = (peak_price - current_price) / peak_price * 100
                    pump_days = (open_times[-1] - open_times[pump_start_idx]) / 86400000

                    current_pump = {
                        "pump_start_price": round(last_swing["price"], 6),
                        "pump_start_idx": pump_start_idx,
                        "peak_price": round(peak_price, 6),
                        "peak_idx": peak_idx,
                        "current_price": round(current_price, 6),
                        "pct_from_start": round(pct_from_start, 2),
                        "pct_from_peak": round(pct_from_peak, 2),
                        "pump_days": round(pump_days, 1),
                    }
                else:
                    current_state = "CONSOLIDATION"
            elif last_swing["type"] == "HIGH":
                pct_from_peak = (last_swing["price"] - current_price) / last_swing["price"] * 100
                if pct_from_peak > min_pct_threshold:
                    current_state = "DUMP_ACTIVE"
                else:
                    current_state = "CONSOLIDATION"
                    # Even in dump, build current_pump from the last HIGH→preceding LOW
                    for j in range(len(swings) - 2, -1, -1):
                        if swings[j]["type"] == "LOW":
                            pump_start = swings[j]
                            pump_start_idx = pump_start["index"]
                            peak_price = last_swing["price"]
                            peak_idx = last_swing["index"]
                            pct_from_start_c = (current_price - pump_start["price"]) / pump_start["price"] * 100
                            pct_from_peak_c = (peak_price - current_price) / peak_price * 100
                            pump_days_c = (open_times[-1] - open_times[pump_start_idx]) / 86400000
                            current_pump = {
                                "pump_start_price": round(pump_start["price"], 6),
                                "pump_start_idx": pump_start_idx,
                                "peak_price": round(peak_price, 6),
                                "peak_idx": peak_idx,
                                "current_price": round(current_price, 6),
                                "pct_from_start": round(pct_from_start_c, 2),
                                "pct_from_peak": round(pct_from_peak_c, 2),
                                "pump_days": round(pump_days_c, 1),
                            }
                            break

    # If we didn't get a current_pump yet, try to build one from the last identified pair
    if current_pump is None and swings:
        # Find the last LOW before the current position
        for j in range(len(swings) - 1, -1, -1):
            if swings[j]["type"] == "LOW":
                pump_start = swings[j]
                pump_start_idx = pump_start["index"]
                peak_price = float(np.max(highs[pump_start_idx:]))
                peak_idx = pump_start_idx + int(np.argmax(highs[pump_start_idx:]))
                pct_from_start = (current_price - pump_start["price"]) / pump_start["price"] * 100
                pct_from_peak = (peak_price - current_price) / peak_price * 100
                pump_days = (open_times[-1] - open_times[pump_start_idx]) / 86400000
                current_pump = {
                    "pump_start_price": round(pump_start["price"], 6),
                    "pump_start_idx": pump_start_idx,
                    "peak_price": round(peak_price, 6),
                    "peak_idx": peak_idx,
                    "current_price": round(current_price, 6),
                    "pct_from_start": round(pct_from_start, 2),
                    "pct_from_peak": round(pct_from_peak, 2),
                    "pump_days": round(pump_days, 1),
                }
                break

    # Serialize swings for output
    swings_out = [
        {"index": s["index"], "price": round(s["price"], 6), "type": s["type"],
         "time": open_times[s["index"]] if s["index"] < len(open_times) else None}
        for s in swings
    ]

    result = {
        "cycles_detected": len(cycles),
        "current_state": current_state,
        "current_pump": current_pump,
        "cycles": cycles,
        "swings": swings_out,
    }

    if len(cycles) < 5:
        result["confidence_warning"] = (
            f"Solo {len(cycles)} ciclos detectados. Estadísticas con baja confianza (ideal >= 7)."
        )

    return result


# ═══════════════════════════════════════════════════
# SECTION 2: RETRACE STATISTICS
# ═══════════════════════════════════════════════════

def _percentile_safe(arr, p):
    """Safe percentile that handles empty arrays."""
    if len(arr) == 0:
        return None
    return round(float(np.percentile(arr, p)), 4)


def _stats_block(arr: np.ndarray, label: str) -> dict:
    """Compute standard stats for an array."""
    if len(arr) == 0:
        return {f"{label}_mean": None, f"{label}_median": None, f"{label}_std": None,
                f"{label}_min": None, f"{label}_max": None, f"{label}_p25": None,
                f"{label}_p75": None, f"{label}_p90": None}
    return {
        f"{label}_mean": round(float(np.mean(arr)), 4),
        f"{label}_median": round(float(np.median(arr)), 4),
        f"{label}_std": round(float(np.std(arr)), 4),
        f"{label}_min": round(float(np.min(arr)), 4),
        f"{label}_max": round(float(np.max(arr)), 4),
        f"{label}_p25": _percentile_safe(arr, 25),
        f"{label}_p75": _percentile_safe(arr, 75),
        f"{label}_p90": _percentile_safe(arr, 90),
    }


def compute_retrace_stats(cycles: list[dict], side: str = "SHORT") -> dict:
    """Section 2: Distribution of historical retraces."""
    if not cycles:
        return {"error": "No cycles available for statistics"}

    retrace_ratios = np.array([c["retrace_ratio"] for c in cycles])
    dump_pcts = np.array([c["dump_pct"] for c in cycles])
    pump_pcts = np.array([c["pump_pct"] for c in cycles])

    if side == "SHORT":
        net_changes = np.array([c.get("net_change_pct", 0) for c in cycles])
    else:
        net_changes = np.array([c.get("net_change_pct", 0) for c in cycles])

    stats = {}
    stats.update(_stats_block(retrace_ratios, "retrace_ratio"))
    stats.update(_stats_block(dump_pcts, "dump_pct"))
    stats.update(_stats_block(pump_pcts, "pump_pct"))

    # Net after cycle
    stats["net_after_cycle_mean"] = round(float(np.mean(net_changes)), 4)
    stats["net_after_cycle_median"] = round(float(np.median(net_changes)), 4)
    stats["pct_full_retrace"] = round(float(np.mean(net_changes < 0) * 100), 1) if side == "SHORT" else round(float(np.mean(net_changes > 0) * 100), 1)

    # Interpretation
    interpretation = []
    median_retrace = float(np.median(retrace_ratios))
    if median_retrace > 1.0:
        interpretation.append("Token con BIAS BEARISH: los dumps superan los pumps históricamente")
    elif median_retrace < 0.5:
        interpretation.append("Token con BIAS BULLISH: retiene >50% de cada pump")
    else:
        interpretation.append(f"Retrace parcial típico: devuelve {median_retrace*100:.0f}% de cada pump")

    pct_full = stats["pct_full_retrace"]
    if pct_full and pct_full > 60:
        interpretation.append(f"Gravedad fuerte: {pct_full}% de pumps se borran completamente")
    elif pct_full and pct_full > 40:
        interpretation.append(f"Gravedad moderada: {pct_full}% de pumps se borran completamente")

    stats["interpretation"] = interpretation
    stats["cycle_count"] = len(cycles)

    return stats


# ═══════════════════════════════════════════════════
# SECTION 3: POSITION IN RANGE & R:R ASYMMETRY
# ═══════════════════════════════════════════════════

def compute_asymmetry(
    entry_price: float,
    current_pump: dict,
    retrace_stats: dict,
    cycles: list[dict],
    side: str = "SHORT",
) -> dict:
    """Section 3: R:R asymmetry at the proposed entry price."""
    if not current_pump:
        return {"error": "No active pump/cycle detected to compute asymmetry"}

    pump_start = current_pump["pump_start_price"]
    peak = current_pump["peak_price"]
    pump_range = peak - pump_start

    if pump_range <= 0:
        return {"error": "Invalid pump range (peak <= start)"}

    # Position in range
    position_in_range = (entry_price - pump_start) / pump_range * 100
    position_in_range = max(0, min(100, position_in_range))
    distance_to_peak_pct = (peak - entry_price) / entry_price * 100 if entry_price > 0 else 0

    # Expected dump bottoms based on retrace history
    retrace_ratios = np.array([c["retrace_ratio"] for c in cycles]) if cycles else np.array([0.5])
    retrace_median = float(np.median(retrace_ratios))
    retrace_mean = float(np.mean(retrace_ratios))
    retrace_p75 = float(np.percentile(retrace_ratios, 75)) if len(retrace_ratios) > 0 else 0.75
    retrace_p90 = float(np.percentile(retrace_ratios, 90)) if len(retrace_ratios) > 0 else 0.90
    retrace_p25 = float(np.percentile(retrace_ratios, 25)) if len(retrace_ratios) > 0 else 0.25
    retrace_min = float(np.min(retrace_ratios)) if len(retrace_ratios) > 0 else 0.1

    if side == "SHORT":
        # Downside = entry - expected_bottom, Upside = peak - entry
        upside_risk = max(peak - entry_price, entry_price * 0.01)  # min 1% upside risk

        expected_bottom_median = peak - pump_range * retrace_median
        expected_bottom_mean = peak - pump_range * retrace_mean
        expected_bottom_p75 = peak - pump_range * retrace_p75
        expected_bottom_p90 = peak - pump_range * retrace_p90
        expected_bottom_conservative = peak - pump_range * retrace_min

        downside_median = entry_price - expected_bottom_median
        downside_p75 = entry_price - expected_bottom_p75
        downside_conservative = entry_price - expected_bottom_conservative

        rr_median = downside_median / upside_risk if upside_risk > 0 else 0
        rr_p75 = downside_p75 / upside_risk if upside_risk > 0 else 0
        rr_conservative = downside_conservative / upside_risk if upside_risk > 0 else 0
    else:
        # LONG: downside_risk = entry - expected_low, upside = expected_high - entry
        downside_risk = max(entry_price - pump_start, entry_price * 0.01)
        expected_top_median = pump_start + pump_range * retrace_median
        expected_top_p75 = pump_start + pump_range * retrace_p75
        expected_top_conservative = pump_start + pump_range * retrace_min

        upside_median = expected_top_median - entry_price
        upside_p75 = expected_top_p75 - entry_price

        rr_median = upside_median / downside_risk if downside_risk > 0 else 0
        rr_p75 = upside_p75 / downside_risk if downside_risk > 0 else 0
        rr_conservative = (expected_top_conservative - entry_price) / downside_risk if downside_risk > 0 else 0

        expected_bottom_median = None
        expected_bottom_mean = None
        expected_bottom_p75 = None
        expected_bottom_p90 = None
        expected_bottom_conservative = None

    # Classify asymmetry signal
    if rr_median >= 2.5 and rr_conservative >= 1.0:
        signal = "STRONG_FAVORABLE"
    elif rr_median >= 1.5 and rr_conservative >= 0.7:
        signal = "FAVORABLE"
    elif rr_median >= 1.0:
        signal = "NEUTRAL"
    elif rr_median >= 0.5:
        signal = "UNFAVORABLE"
    else:
        signal = "STRONG_UNFAVORABLE"

    result = {
        "entry_price": round(entry_price, 6),
        "pump_start": round(pump_start, 6),
        "pump_peak": round(peak, 6),
        "pump_range": round(pump_range, 6),
        "position_in_range_pct": round(position_in_range, 1),
        "distance_to_peak_pct": round(distance_to_peak_pct, 2),
        "rr_median": round(rr_median, 2),
        "rr_p75": round(rr_p75, 2),
        "rr_conservative": round(rr_conservative, 2),
        "asymmetry_signal": signal,
    }

    if side == "SHORT" and expected_bottom_median is not None:
        result.update({
            "expected_bottom_median": round(expected_bottom_median, 6),
            "expected_bottom_mean": round(expected_bottom_mean, 6),
            "expected_bottom_p75": round(expected_bottom_p75, 6),
            "expected_bottom_p90": round(expected_bottom_p90, 6),
            "expected_bottom_conservative": round(expected_bottom_conservative, 6),
            "upside_risk_pct": round(distance_to_peak_pct, 2),
            "downside_median_pct": round((entry_price - expected_bottom_median) / entry_price * 100, 2),
            "downside_p75_pct": round((entry_price - expected_bottom_p75) / entry_price * 100, 2),
        })

    return result


# ═══════════════════════════════════════════════════
# SECTION 4: WIN RATE HISTÓRICO
# ═══════════════════════════════════════════════════

def compute_win_rate(
    entry_price: float,
    current_pump: dict,
    cycles: list[dict],
    side: str = "SHORT",
) -> dict:
    """Section 4: Historical win rate at the given position in range."""
    if not cycles:
        return {"error": "No historical cycles for win rate calculation"}
    if not current_pump:
        return {"error": "No current pump to compute position"}

    pump_start = current_pump["pump_start_price"]
    peak = current_pump["peak_price"]
    pump_range = peak - pump_start
    if pump_range <= 0:
        return {"error": "Invalid pump range"}

    position_pct = (entry_price - pump_start) / pump_range * 100

    # Simulate entry at same relative position in each historical cycle
    wins = []
    losses = []

    for c in cycles:
        if side == "SHORT":
            c_start = c["pump_start_price"]
            c_peak = c["pump_peak_price"]
            c_bottom = c["dump_bottom_price"]
            c_range = c_peak - c_start
            if c_range <= 0:
                continue

            # Equivalent entry in this cycle
            sim_entry = c_start + c_range * (position_pct / 100)
            sim_upside = c_peak - sim_entry  # stop would be at peak
            sim_downside = sim_entry - c_bottom

            if c_bottom < sim_entry:
                # Winner: dump went below entry
                win_pct = (sim_entry - c_bottom) / sim_entry * 100
                wins.append(win_pct)
            else:
                # Loser: dump didn't reach entry
                loss_pct = 0  # held but didn't win
                losses.append(loss_pct)
        else:
            c_start = c.get("dump_start_price", c.get("pump_peak_price", 0))
            c_bottom = c.get("dump_bottom_price", c.get("pump_start_price", 0))
            c_recovery = c.get("pump_peak_price", c.get("dump_start_price", 0))
            c_range = c_start - c_bottom
            if c_range <= 0:
                continue

            sim_entry = c_start - c_range * (position_pct / 100)
            if c_recovery > sim_entry:
                win_pct = (c_recovery - sim_entry) / sim_entry * 100
                wins.append(win_pct)
            else:
                losses.append(0)

    total = len(wins) + len(losses)
    if total == 0:
        return {"error": "No valid cycles to simulate"}

    win_rate = len(wins) / total * 100
    avg_win = float(np.mean(wins)) if wins else 0
    avg_loss = float(np.mean(losses)) if losses else 0

    # Expected value
    ev = (win_rate / 100) * avg_win - ((100 - win_rate) / 100) * avg_loss
    profit_factor = sum(wins) / max(sum(losses), 0.001) if losses else float("inf")

    # Breakeven win rate given R:R
    breakeven_wr = None
    if avg_win > 0:
        breakeven_wr = round(avg_loss / (avg_win + avg_loss) * 100, 1) if (avg_win + avg_loss) > 0 else 0

    # Win rate table at different positions
    win_rate_table = []
    for pos in range(20, 100, 10):
        w_count = 0
        l_count = 0
        rr_list = []
        for c in cycles:
            if side == "SHORT":
                c_start = c["pump_start_price"]
                c_peak = c["pump_peak_price"]
                c_bottom = c["dump_bottom_price"]
                c_range = c_peak - c_start
                if c_range <= 0:
                    continue
                sim_e = c_start + c_range * (pos / 100)
                if c_bottom < sim_e:
                    w_count += 1
                    rr_list.append((sim_e - c_bottom) / max(c_peak - sim_e, 0.0001))
                else:
                    l_count += 1

        tot = w_count + l_count
        if tot > 0:
            wr = w_count / tot * 100
            avg_rr = float(np.mean(rr_list)) if rr_list else 0
            win_rate_table.append({
                "position_pct": pos,
                "win_rate": round(wr, 1),
                "avg_rr": round(avg_rr, 2),
                "sample_size": tot,
            })

    result = {
        "position_in_range_pct": round(position_pct, 1),
        "win_rate_at_entry": round(win_rate, 1),
        "win_count": len(wins),
        "loss_count": len(losses),
        "avg_winner_pct": round(avg_win, 2),
        "avg_loser_pct": round(avg_loss, 2),
        "expected_value_pct": round(ev, 2),
        "profit_factor": round(profit_factor, 2) if profit_factor != float("inf") else "∞",
        "breakeven_win_rate": breakeven_wr,
        "win_rate_table": win_rate_table,
    }

    if total < 7:
        result["confidence_warning"] = f"Solo {total} ciclos simulados. Win rate con baja confianza."

    return result


# ═══════════════════════════════════════════════════
# SECTION 5: FUNDING RATE HISTÓRICO Y CARRY
# ═══════════════════════════════════════════════════

def compute_funding_carry(
    funding_history: list[dict],
    cycles: list[dict],
    open_times: list[int],
    side: str = "SHORT",
    expected_hold_days: Optional[float] = None,
) -> dict:
    """Section 5: Funding carry analysis."""
    if not funding_history:
        return {"error": "No funding data available"}

    rates = np.array([f["funding_rate"] for f in funding_history])
    f_times = np.array([f["funding_time"] for f in funding_history])

    # General funding stats
    funding_mean = float(np.mean(rates))
    funding_median = float(np.median(rates))
    pct_positive = float(np.mean(rates > 0) * 100)
    pct_negative = float(np.mean(rates < 0) * 100)

    # Daily carry: 3 fundings per day
    if side == "SHORT":
        daily_carry = funding_mean * 3 * 100  # positive funding = shorts earn
        monthly_carry = daily_carry * 30
    else:
        daily_carry = -funding_mean * 3 * 100  # negative funding = longs earn
        monthly_carry = daily_carry * 30

    # Funding during pump/dump phases (cross with cycle timestamps)
    funding_during_pumps = []
    funding_during_dumps = []

    for c in cycles:
        if side == "SHORT":
            pump_start_t = open_times[c["pump_start_idx"]] if c["pump_start_idx"] < len(open_times) else None
            pump_peak_t = open_times[c["pump_peak_idx"]] if c["pump_peak_idx"] < len(open_times) else None
            dump_bottom_t = open_times[c["dump_bottom_idx"]] if c["dump_bottom_idx"] < len(open_times) else None

            if pump_start_t and pump_peak_t:
                mask_pump = (f_times >= pump_start_t) & (f_times <= pump_peak_t)
                if np.any(mask_pump):
                    funding_during_pumps.extend(rates[mask_pump].tolist())

            if pump_peak_t and dump_bottom_t:
                mask_dump = (f_times >= pump_peak_t) & (f_times <= dump_bottom_t)
                if np.any(mask_dump):
                    funding_during_dumps.extend(rates[mask_dump].tolist())

    # Current funding
    current_funding = float(rates[-1]) if len(rates) > 0 else 0
    funding_std = float(np.std(rates)) if len(rates) > 1 else 0.001
    funding_zscore = (current_funding - funding_mean) / max(funding_std, 1e-8)

    if current_funding > 0.0001:
        funding_regime = "LONGS_PAY"
    elif current_funding < -0.0001:
        funding_regime = "SHORTS_PAY"
    else:
        funding_regime = "NEUTRAL"

    # Hold time estimation
    if expected_hold_days is None and cycles:
        dump_days = [c["dump_days"] for c in cycles if c["dump_days"] > 0]
        expected_hold_days = float(np.median(dump_days)) if dump_days else 3.0

    expected_carry_pct = daily_carry * (expected_hold_days or 3.0)

    result = {
        "funding_lifetime": {
            "mean": round(funding_mean, 6),
            "median": round(funding_median, 6),
            "pct_positive": round(pct_positive, 1),
            "pct_negative": round(pct_negative, 1),
            "daily_carry_short_pct": round(daily_carry, 4),
            "monthly_carry_short_pct": round(monthly_carry, 4),
        },
        "funding_during_pumps": {
            "mean": round(float(np.mean(funding_during_pumps)), 6) if funding_during_pumps else None,
            "median": round(float(np.median(funding_during_pumps)), 6) if funding_during_pumps else None,
            "max": round(float(np.max(funding_during_pumps)), 6) if funding_during_pumps else None,
            "samples": len(funding_during_pumps),
        },
        "funding_during_dumps": {
            "mean": round(float(np.mean(funding_during_dumps)), 6) if funding_during_dumps else None,
            "median": round(float(np.median(funding_during_dumps)), 6) if funding_during_dumps else None,
            "samples": len(funding_during_dumps),
        },
        "funding_current": round(current_funding, 6),
        "funding_zscore": round(funding_zscore, 2),
        "funding_regime": funding_regime,
        "expected_hold_days": round(expected_hold_days or 3.0, 1),
        "expected_carry_pct": round(expected_carry_pct, 4),
    }

    # Interpretation
    interp = []
    if side == "SHORT":
        pump_mean = result["funding_during_pumps"]["mean"]
        if pump_mean is not None and pump_mean > 0:
            interp.append(f"Favorable: shorts cobran funding durante pumps (avg {pump_mean*100:.4f}%)")
        elif pump_mean is not None and pump_mean < 0:
            interp.append(f"Desfavorable: shorts pagan funding durante pumps (avg {pump_mean*100:.4f}%)")
        interp.append(f"Carry estimado: {'+' if expected_carry_pct > 0 else ''}{expected_carry_pct:.4f}% durante {expected_hold_days:.0f} días de hold")
    result["interpretation"] = interp

    return result


# ═══════════════════════════════════════════════════
# SECTION 6: VELOCIDAD Y TIMING
# ═══════════════════════════════════════════════════

def compute_velocity_timing(
    cycles: list[dict],
    current_pump: Optional[dict],
    side: str = "SHORT",
) -> dict:
    """Section 6: Speed and timing analysis."""
    if not cycles:
        return {"error": "No cycles for velocity analysis"}

    pump_durations = np.array([c["pump_days"] for c in cycles if c["pump_days"] > 0])
    dump_durations = np.array([c["dump_days"] for c in cycles if c["dump_days"] > 0])
    pump_pcts = np.array([c["pump_pct"] for c in cycles])
    dump_pcts = np.array([c["dump_pct"] for c in cycles])

    # Velocity per day
    pump_velocities = pump_pcts / np.maximum(pump_durations, 0.1) if len(pump_durations) > 0 else np.array([])
    dump_velocities = dump_pcts / np.maximum(dump_durations, 0.1) if len(dump_durations) > 0 else np.array([])

    velocity_ratio = (
        float(np.median(dump_velocities)) / max(float(np.median(pump_velocities)), 0.01)
        if len(dump_velocities) > 0 and len(pump_velocities) > 0
        else None
    )

    result = {
        "pump_duration": {
            "mean": round(float(np.mean(pump_durations)), 1) if len(pump_durations) > 0 else None,
            "median": round(float(np.median(pump_durations)), 1) if len(pump_durations) > 0 else None,
            "min": round(float(np.min(pump_durations)), 1) if len(pump_durations) > 0 else None,
            "max": round(float(np.max(pump_durations)), 1) if len(pump_durations) > 0 else None,
        },
        "dump_duration": {
            "mean": round(float(np.mean(dump_durations)), 1) if len(dump_durations) > 0 else None,
            "median": round(float(np.median(dump_durations)), 1) if len(dump_durations) > 0 else None,
            "min": round(float(np.min(dump_durations)), 1) if len(dump_durations) > 0 else None,
            "max": round(float(np.max(dump_durations)), 1) if len(dump_durations) > 0 else None,
        },
        "dump_velocity_daily": {
            "mean": round(float(np.mean(dump_velocities)), 2) if len(dump_velocities) > 0 else None,
            "median": round(float(np.median(dump_velocities)), 2) if len(dump_velocities) > 0 else None,
        },
        "pump_velocity_daily": {
            "mean": round(float(np.mean(pump_velocities)), 2) if len(pump_velocities) > 0 else None,
            "median": round(float(np.median(pump_velocities)), 2) if len(pump_velocities) > 0 else None,
        },
        "velocity_ratio": round(velocity_ratio, 2) if velocity_ratio else None,
    }

    # Velocity interpretation
    if velocity_ratio:
        if velocity_ratio > 1.5:
            result["velocity_interpretation"] = "Dumps mucho más rápidos que pumps → capital-eficiente para shorts"
        elif velocity_ratio > 1.0:
            result["velocity_interpretation"] = "Dumps ligeramente más rápidos que pumps"
        else:
            result["velocity_interpretation"] = "Dumps más lentos que pumps → mayor holding time"

    # Pump maturity
    if current_pump and len(pump_durations) > 0:
        current_age = current_pump.get("pump_days", 0)
        # Percentile of current age vs historical pump durations
        age_percentile = float(np.mean(pump_durations <= current_age) * 100)
        result["pump_maturity"] = {
            "current_pump_age_days": round(current_age, 1),
            "pump_age_percentile": round(age_percentile, 1),
            "interpretation": (
                "Pump maduro (estadísticamente cerca del peak)" if age_percentile > 80
                else "Pump en fase media" if age_percentile > 40
                else "Pump joven, podría seguir subiendo"
            ),
        }

        # Estimated days to target
        if len(dump_velocities) > 0 and current_pump.get("pct_from_peak", 0) <= 5:
            median_dump_vel = float(np.median(dump_velocities))
            median_dump_pct = float(np.median(dump_pcts))
            if median_dump_vel > 0:
                result["estimated_days_to_target"] = round(median_dump_pct / median_dump_vel, 1)

    return result


# ═══════════════════════════════════════════════════
# SECTION 7: VOLUMEN Y ESTRUCTURA
# ═══════════════════════════════════════════════════

def compute_volume_structure(
    volumes: np.ndarray,
    taker_buy_volumes: np.ndarray,
    current_pump: Optional[dict],
    cycles: list[dict],
    side: str = "SHORT",
) -> dict:
    """Section 7: Volume and structure analysis."""
    if current_pump is None:
        return {"error": "No current pump for volume analysis"}

    pump_start_idx = current_pump.get("pump_start_idx", 0)
    n = len(volumes)

    # Current pump volume
    pump_vols = volumes[pump_start_idx:]
    if len(pump_vols) == 0:
        return {"error": "No volume data in pump range"}

    current_pump_volume = float(np.sum(pump_vols))

    # Historical pump volumes
    hist_pump_vols = []
    for c in cycles:
        if side == "SHORT":
            si = c.get("pump_start_idx", 0)
            ei = c.get("pump_peak_idx", 0)
            if 0 <= si < ei < n:
                hist_pump_vols.append(float(np.sum(volumes[si:ei + 1])))

    historical_median = float(np.median(hist_pump_vols)) if hist_pump_vols else current_pump_volume
    volume_ratio = current_pump_volume / max(historical_median, 1) if historical_median > 0 else 1.0

    # Volume decay
    if len(pump_vols) >= 3:
        peak_vol_day = int(np.argmax(pump_vols))
        vol_at_peak = float(pump_vols[peak_vol_day])
        vol_recent = float(np.mean(pump_vols[-3:]))
        volume_decay_pct = (vol_at_peak - vol_recent) / vol_at_peak * 100 if vol_at_peak > 0 else 0
        volume_declining = bool(
            pump_vols[-1] < pump_vols[-2] < pump_vols[-3]
        ) if len(pump_vols) >= 3 else False
    else:
        peak_vol_day = 0
        volume_decay_pct = 0
        volume_declining = False

    # Taker buy ratio
    if len(taker_buy_volumes) > pump_start_idx:
        pump_taker = taker_buy_volumes[pump_start_idx:]
        pump_total = volumes[pump_start_idx:]
        taker_ratios = pump_taker / np.maximum(pump_total, 1e-10)
        taker_buy_ratio_avg = float(np.mean(taker_ratios))
        taker_buy_ratio_current = float(taker_ratios[-1]) if len(taker_ratios) > 0 else 0.5

        taker_buy_declining = False
        if len(taker_ratios) >= 3:
            taker_buy_declining = bool(
                taker_ratios[-1] < taker_ratios[-2] < taker_ratios[-3]
            )
    else:
        taker_buy_ratio_avg = 0.5
        taker_buy_ratio_current = 0.5
        taker_buy_declining = False

    # Volume profile match
    if hist_pump_vols:
        if volume_ratio > 2.0:
            volume_profile = "UNPRECEDENTED"
        elif volume_ratio > 1.3:
            volume_profile = "SIMILAR_TO_MAJOR_PUMPS"
        elif volume_ratio > 0.7:
            volume_profile = "SIMILAR_TO_TYPICAL_PUMPS"
        else:
            volume_profile = "SIMILAR_TO_MINOR_PUMPS"
    else:
        volume_profile = "NO_REFERENCE"

    result = {
        "current_pump_volume": round(current_pump_volume, 2),
        "historical_pump_volume_median": round(historical_median, 2),
        "volume_ratio": round(volume_ratio, 2),
        "volume_peak_day": peak_vol_day,
        "volume_decay_pct": round(volume_decay_pct, 1),
        "volume_declining": volume_declining,
        "taker_buy_ratio_pump_avg": round(taker_buy_ratio_avg, 4),
        "taker_buy_ratio_current": round(taker_buy_ratio_current, 4),
        "taker_buy_declining": taker_buy_declining,
        "volume_profile_match": volume_profile,
    }

    # Volume interpretation
    interps = []
    if volume_declining:
        interps.append("Volumen declinando → señal de agotamiento de compradores")
    if taker_buy_declining:
        interps.append("Taker buy ratio declinando → compradores se están agotando")
    if volume_ratio > 2.0:
        interps.append("Volumen excepcional vs histórico → pump atípico, precaución")
    elif volume_ratio < 0.5:
        interps.append("Volumen débil vs histórico → pump frágil, dump probable")
    result["interpretation"] = interps

    return result


# ═══════════════════════════════════════════════════
# SECTION 8: COMPOSITE SCORE & VERDICT
# ═══════════════════════════════════════════════════

def _score_asymmetry(asymmetry: dict) -> float:
    """Score 0-100 based on R:R asymmetry (35% weight)."""
    rr = asymmetry.get("rr_median", 0)
    rr_cons = asymmetry.get("rr_conservative", 0)

    if rr >= 3.0 and rr_cons >= 1.5:
        return 95
    elif rr >= 2.5 and rr_cons >= 1.0:
        return 85
    elif rr >= 2.0 and rr_cons >= 0.7:
        return 75
    elif rr >= 1.5 and rr_cons >= 0.5:
        return 65
    elif rr >= 1.0:
        return 50
    elif rr >= 0.5:
        return 30
    else:
        return 10


def _score_win_rate(win_rate_data: dict) -> float:
    """Score 0-100 based on win rate (25% weight)."""
    wr = win_rate_data.get("win_rate_at_entry", 0)
    pf = win_rate_data.get("profit_factor", 0)
    if isinstance(pf, str):
        pf = 10  # inf

    if wr >= 85 and pf >= 3.0:
        return 95
    elif wr >= 75 and pf >= 2.0:
        return 80
    elif wr >= 65 and pf >= 1.5:
        return 65
    elif wr >= 50:
        return 50
    elif wr >= 35:
        return 30
    else:
        return 10


def _score_funding(funding: dict) -> float:
    """Score 0-100 based on funding carry (15% weight)."""
    if "error" in funding:
        return 50  # neutral if no data

    carry = funding.get("expected_carry_pct", 0)
    regime = funding.get("funding_regime", "NEUTRAL")

    if regime == "LONGS_PAY" and carry > 0.1:
        return 90
    elif regime == "LONGS_PAY":
        return 70
    elif regime == "NEUTRAL":
        return 50
    elif carry > -0.05:
        return 35
    else:
        return 15


def _score_timing(velocity: dict) -> float:
    """Score 0-100 based on timing/maturity (15% weight)."""
    if "error" in velocity:
        return 50

    maturity = velocity.get("pump_maturity", {})
    pct = maturity.get("pump_age_percentile", 50)
    vel_ratio = velocity.get("velocity_ratio")

    score = 50
    # Maturity contributes up to ±30
    if pct > 80:
        score += 30
    elif pct > 60:
        score += 15
    elif pct < 30:
        score -= 20

    # Velocity ratio contributes up to ±20
    if vel_ratio:
        if vel_ratio > 1.5:
            score += 20
        elif vel_ratio > 1.0:
            score += 10
        elif vel_ratio < 0.7:
            score -= 15

    return max(0, min(100, score))


def _score_volume(volume: dict) -> float:
    """Score 0-100 based on volume structure (embedded in timing weight)."""
    if "error" in volume:
        return 50

    score = 50
    if volume.get("volume_declining", False):
        score += 20
    if volume.get("taker_buy_declining", False):
        score += 15
    vol_ratio = volume.get("volume_ratio", 1.0)
    if vol_ratio < 0.5:
        score += 10  # weak pump
    elif vol_ratio > 2.0:
        score -= 10  # very strong pump, might continue
    return max(0, min(100, score))


def compute_composite_score(
    asymmetry: dict,
    win_rate_data: dict,
    funding: dict,
    velocity: dict,
    volume: dict,
    side: str = "SHORT",
) -> dict:
    """Section 8: Composite score and final verdict."""

    components = {
        "asymmetry": {"raw_score": _score_asymmetry(asymmetry), "weight": 0.35},
        "win_rate": {"raw_score": _score_win_rate(win_rate_data), "weight": 0.25},
        "funding": {"raw_score": _score_funding(funding), "weight": 0.15},
        "timing": {"raw_score": _score_timing(velocity), "weight": 0.15},
        "volume": {"raw_score": _score_volume(volume), "weight": 0.10},
    }

    composite = sum(c["raw_score"] * c["weight"] for c in components.values())
    composite = round(composite, 1)

    # Classification
    side_label = "SHORT" if side == "SHORT" else "LONG"
    if composite >= 90:
        classification = f"ELITE {side_label}"
    elif composite >= 75:
        classification = f"STRONG {side_label}"
    elif composite >= 60:
        classification = f"MODERATE {side_label}"
    elif composite >= 45:
        classification = f"WEAK {side_label}"
    elif composite >= 30:
        classification = f"POOR {side_label}"
    else:
        classification = "AVOID"

    # Key factors
    key_factors = []
    rr_med = asymmetry.get("rr_median", 0)
    if rr_med >= 1.5:
        key_factors.append(f"✓ R:R mediano {rr_med}x (favorable)")
    elif rr_med >= 1.0:
        key_factors.append(f"~ R:R mediano {rr_med}x (aceptable)")
    else:
        key_factors.append(f"✗ R:R mediano {rr_med}x (desfavorable)")

    wr = win_rate_data.get("win_rate_at_entry", 0)
    if wr >= 70:
        key_factors.append(f"✓ Win rate {wr}% a esta posición en rango")
    elif wr >= 50:
        key_factors.append(f"~ Win rate {wr}% (moderado)")
    else:
        key_factors.append(f"✗ Win rate {wr}% (bajo)")

    if "error" not in funding:
        carry = funding.get("expected_carry_pct", 0)
        regime = funding.get("funding_regime", "NEUTRAL")
        if regime == "LONGS_PAY":
            key_factors.append(f"✓ Funding favorable: +{carry:.4f}% carry estimado")
        elif regime == "SHORTS_PAY":
            key_factors.append(f"✗ Funding negativo: {carry:.4f}% carry cost")
        else:
            key_factors.append("~ Funding neutral")

    maturity = velocity.get("pump_maturity", {})
    mat_pct = maturity.get("pump_age_percentile", 0)
    if mat_pct > 70:
        key_factors.append(f"✓ Pump maduro (percentil {mat_pct}% de duración)")
    elif mat_pct > 40:
        key_factors.append(f"~ Pump en fase media (percentil {mat_pct}%)")
    else:
        key_factors.append(f"✗ Pump joven (percentil {mat_pct}%)")

    if volume.get("volume_declining", False):
        key_factors.append("✓ Volumen declinando (señal de agotamiento)")
    elif volume.get("taker_buy_declining", False):
        key_factors.append("~ Taker buy ratio declinando")
    else:
        key_factors.append("~ Volumen aún no declina (precaución)")

    # Targets based on retrace
    pump_start = asymmetry.get("pump_start", 0)
    pump_peak = asymmetry.get("pump_peak", 0)
    pump_range = pump_peak - pump_start

    targets = {}
    if pump_range > 0 and asymmetry.get("expected_bottom_median") is not None:
        targets = {
            "conservative": {
                "price": asymmetry.get("expected_bottom_conservative"),
                "based_on": "Min retrace histórico",
            },
            "median": {
                "price": asymmetry.get("expected_bottom_median"),
                "based_on": "Median retrace",
            },
            "aggressive": {
                "price": asymmetry.get("expected_bottom_p75"),
                "based_on": "P75 retrace",
            },
        }

    # Stop loss
    stop_loss = None
    if side == "SHORT" and pump_peak > 0:
        sl_price = round(pump_peak * 1.05, 6)
        stop_loss = {
            "price": sl_price,
            "above_peak_pct": 5.0,
            "note": f"5% above peak ({pump_peak})",
        }

    # Expected hold
    expected_hold = {}
    if "error" not in velocity:
        dump_dur = velocity.get("dump_duration", {})
        median_days = dump_dur.get("median")
        if median_days and "error" not in funding:
            expected_hold = {
                "days": median_days,
                "carry_pct": funding.get("expected_carry_pct", 0),
            }

    # Optimal entry (find position where R:R is best)
    optimal_entry = None
    wrt = win_rate_data.get("win_rate_table", [])
    if wrt and pump_range > 0:
        best = max(wrt, key=lambda x: x["avg_rr"] * x["win_rate"] / 100 if x["avg_rr"] > 0 else 0)
        opt_price = round(pump_start + pump_range * (best["position_pct"] / 100), 6)
        optimal_entry = {
            "price": opt_price,
            "position_in_range": f"{best['position_pct']}%",
            "rr_at_optimal": f"{best['avg_rr']}x",
            "win_rate_at_optimal": f"{best['win_rate']}%",
        }

    return {
        "composite_score": composite,
        "classification": classification,
        "score_components": {k: {"score": v["raw_score"], "weight": f"{v['weight']*100:.0f}%"} for k, v in components.items()},
        "key_factors": key_factors,
        "targets": targets,
        "stop_loss": stop_loss,
        "optimal_entry": optimal_entry,
        "expected_hold": expected_hold,
    }


# ═══════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════

async def analyze_range_asymmetry(
    binance_client,
    symbol: str,
    entry_price: Optional[float] = None,
    side: str = "SHORT",
    timeframe: str = "1d",
    pump_threshold: float = 5.0,
) -> dict:
    """
    Full range asymmetry analysis. Orchestrates all 8 sections.

    Args:
        binance_client: BinanceClient instance
        symbol: Trading pair (e.g. VVVUSDT)
        entry_price: Proposed entry price (uses current if None)
        side: "SHORT" or "LONG"
        timeframe: "1d" for swing, "1h" for intraday
        pump_threshold: Min % for zigzag threshold
    """
    import time as _time
    t_start = _time.time()

    symbol = symbol.upper()
    side = side.upper()
    if side not in ("SHORT", "LONG"):
        side = "SHORT"

    # ── Fetch data in parallel ──
    import asyncio

    # Fetch klines (max history)
    klines_task = binance_client.get_klines(symbol=symbol, interval=timeframe, limit=1500)

    # Fetch ticker for current price
    ticker_task = binance_client.get_ticker_24h(symbol)

    klines, ticker = await asyncio.gather(klines_task, ticker_task)

    if not klines or len(klines) < 10:
        return {
            "symbol": symbol,
            "error": "INSUFFICIENT_DATA",
            "message": f"Solo {len(klines) if klines else 0} velas disponibles. Se necesitan >= 10.",
        }

    # Extract arrays
    highs = np.array([k["high"] for k in klines])
    lows = np.array([k["low"] for k in klines])
    closes = np.array([k["close"] for k in klines])
    volumes = np.array([k["volume"] for k in klines])
    taker_buy_vols = np.array([k["taker_buy_volume"] for k in klines])
    open_times = [k["open_time"] for k in klines]

    current_price = float(ticker["last_price"])
    if entry_price is None:
        entry_price = current_price

    # ── SECTION 1: Cycle Detection ──
    cycle_data = detect_cycles(
        highs, lows, closes, open_times,
        side=side,
        min_pct_threshold=pump_threshold,
    )

    cycles = cycle_data["cycles"]
    current_pump = cycle_data["current_pump"]

    # If no current pump found, use global range
    if current_pump is None:
        # Fallback: use all-time range
        global_low = float(np.min(lows))
        global_high = float(np.max(highs))
        global_high_idx = int(np.argmax(highs))
        global_low_idx = int(np.argmin(lows))
        current_pump = {
            "pump_start_price": round(global_low, 6),
            "pump_start_idx": global_low_idx,
            "peak_price": round(global_high, 6),
            "peak_idx": global_high_idx,
            "current_price": round(current_price, 6),
            "pct_from_start": round((current_price - global_low) / max(global_low, 1e-10) * 100, 2),
            "pct_from_peak": round((global_high - current_price) / max(global_high, 1e-10) * 100, 2),
            "pump_days": round((open_times[-1] - open_times[0]) / 86400000, 1),
            "note": "Usando rango global (no se detectó pump activo específico)",
        }

    # ── SECTION 2: Retrace Statistics ──
    retrace_stats = compute_retrace_stats(cycles, side=side)

    # ── SECTION 3: R:R Asymmetry ──
    asymmetry = compute_asymmetry(entry_price, current_pump, retrace_stats, cycles, side=side)

    # ── SECTION 4: Win Rate ──
    win_rate_data = compute_win_rate(entry_price, current_pump, cycles, side=side)

    # ── SECTION 5: Funding & Carry ──
    # Paginate funding rate to get max history
    funding_history = await _fetch_full_funding(binance_client, symbol)
    funding_carry = compute_funding_carry(funding_history, cycles, open_times, side=side)

    # Carry-adjusted R:R
    if "error" not in funding_carry and "error" not in asymmetry:
        carry_pct = funding_carry.get("expected_carry_pct", 0)
        rr_med = asymmetry.get("rr_median", 0)
        # Adjust: if carry is positive (earn), add to downside; if negative (cost), subtract
        # Simple approximation: adjust the ratio
        entry = asymmetry.get("entry_price", 0)
        peak = asymmetry.get("pump_peak", 0)
        upside_risk_pct = asymmetry.get("upside_risk_pct", asymmetry.get("distance_to_peak_pct", 1))
        if upside_risk_pct and upside_risk_pct > 0:
            downside_med_pct = asymmetry.get("downside_median_pct", 0) or 0
            adjusted_downside = downside_med_pct + carry_pct
            carry_adjusted_rr = adjusted_downside / max(upside_risk_pct, 0.01)
            funding_carry["carry_adjusted_rr"] = round(carry_adjusted_rr, 2)

    # ── SECTION 6: Velocity & Timing ──
    velocity = compute_velocity_timing(cycles, current_pump, side=side)

    # ── SECTION 7: Volume & Structure ──
    volume_structure = compute_volume_structure(volumes, taker_buy_vols, current_pump, cycles, side=side)

    # ── SECTION 8: Composite Score ──
    composite = compute_composite_score(
        asymmetry, win_rate_data, funding_carry, velocity, volume_structure, side=side,
    )

    elapsed = round(_time.time() - t_start, 2)

    return {
        "symbol": symbol,
        "side": side,
        "timeframe": timeframe,
        "entry_price": round(entry_price, 6),
        "current_price": round(current_price, 6),
        "candles_analyzed": len(klines),
        "elapsed_sec": elapsed,
        "section_1_cycles": cycle_data,
        "section_2_retrace_stats": retrace_stats,
        "section_3_asymmetry": asymmetry,
        "section_4_win_rate": win_rate_data,
        "section_5_funding_carry": funding_carry,
        "section_6_velocity_timing": velocity,
        "section_7_volume_structure": volume_structure,
        "section_8_verdict": composite,
    }


async def _fetch_full_funding(binance_client, symbol: str, max_pages: int = 5) -> list[dict]:
    """
    Fetch full funding rate history with pagination.
    Each page = 1000 entries (8h intervals) ≈ 333 days.
    """
    all_funding = []
    start_time = None

    for _ in range(max_pages):
        try:
            batch = await binance_client.get_funding_rate(
                symbol=symbol, limit=1000, start_time=start_time,
            )
        except Exception:
            break

        if not batch:
            break

        all_funding.extend(batch)

        # Next page starts after last entry
        last_time = batch[-1]["funding_time"]
        if start_time and last_time <= start_time:
            break
        start_time = last_time + 1

        # If we got less than 1000, we've reached the end
        if len(batch) < 1000:
            break

    # Deduplicate by funding_time
    seen = set()
    deduped = []
    for f in all_funding:
        if f["funding_time"] not in seen:
            seen.add(f["funding_time"])
            deduped.append(f)

    return sorted(deduped, key=lambda x: x["funding_time"])
