"""
OI Level — Open Interest Energy Divergence (OIED) Indicator
============================================================
Análisis del estado energético del mercado de futuros a través del
Open Interest descompuesto en sus componentes de masa y energía.

Concepto central:
  El OI tiene dos componentes que Binance reporta por separado:
    - OI_tokens (sum_open_interest)  = número de contratos abiertos  → MASA del sistema
    - OI_value  (sum_open_interest_value) = nocional en USD           → ENERGÍA CINÉTICA
  
  Precio implícito = OI_value / OI_tokens
    → precio promedio ponderado al que están posicionados los participantes

Indicador OIED — 5 estados energéticos (Tabla 2 del paper):
  ΔE = ROC(OI_value) - ROC(OI_tokens)
  
  Estado              │ ΔE   │ ROC(tok) │ Significado
  ────────────────────┼──────┼──────────┼──────────────────────────────────
  ENERGY_INFLOW       │  >0  │    >0    │ Capital entrando a precios altos (bullish)
  ENERGY_RECOVERY     │  >0  │    <0    │ Precio sube, posiciones se cierran
  ENERGY_TRAP         │  <0  │    >0    │ ⚠ Longs atrapados, energía drena (Ψ₂)
  ENERGY_DISSIPATION  │  <0  │    ≈0    │ ⚠ Energía drena, masa congelada (pre-jamming)
  ENERGY_DELEVERAGING │  <0  │    <0    │ Crash activo / liquidación forzada

Señal crítica Ψ₂:
  ENERGY_TRAP sostenido (≥3 periodos) = precursor de crash con ~36h de lead time.
  Secuencia de picos OI_val → OI_tok → Price confirma cadena causal.

Capas de análisis:
  1. Divergencia ROC (OI_tok vs Precio): BEAR_DIV, BULL_DIV, CONFIRM_*
  2. OIED: 5 estados energéticos Langevin (lo central del indicador)
  3. Swing divergences: HH/LH clásicas entre precio y OI
  4. Peak ordering: secuencia temporal de máximos (OI_val → OI_tok → Price)

Salida:
  - Gráfico interactivo HTML (guardado en /tmp/psi_jam_charts/)
  - Estado energético actual + conteo de estados + rachas
  - Divergencias detectadas + interpretación
"""

import asyncio
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Plotly is optional — fall back to data-only output if unavailable
try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    HAS_PLOTLY = True
except ImportError:
    HAS_PLOTLY = False


# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────

VALID_OI_PERIODS = ["5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"]

PERIOD_MS = {
    "5m": 5 * 60_000,
    "15m": 15 * 60_000,
    "30m": 30 * 60_000,
    "1h": 60 * 60_000,
    "2h": 2 * 60 * 60_000,
    "4h": 4 * 60 * 60_000,
    "6h": 6 * 60 * 60_000,
    "12h": 12 * 60 * 60_000,
    "1d": 24 * 60 * 60_000,
}

# Map OI periods to kline intervals (they share most values)
OI_TO_KLINE_INTERVAL = {
    "5m": "5m", "15m": "15m", "30m": "30m",
    "1h": "1h", "2h": "2h", "4h": "4h",
    "6h": "6h", "12h": "12h", "1d": "1d",
}


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def _ts_to_str(ts_ms: int) -> str:
    """Convert epoch ms to human-readable string."""
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return dt.strftime("%b %d %H:%M")


def _find_nearest(series: list[dict], target_ts: int, tolerance_ms: int, ts_key: str = "timestamp") -> Optional[dict]:
    """Find the entry in series whose timestamp is closest to target_ts within tolerance."""
    best = None
    best_delta = float("inf")
    for entry in series:
        delta = abs(entry[ts_key] - target_ts)
        if delta < best_delta:
            best_delta = delta
            best = entry
    if best and best_delta <= tolerance_ms:
        return best
    return None


# ─────────────────────────────────────────────
# DATA PIPELINE
# ─────────────────────────────────────────────

async def fetch_oi_chart_data(binance_client, symbol: str, period: str, limit: int):
    """Fetch klines, OI history and funding in parallel."""
    kline_interval = OI_TO_KLINE_INTERVAL.get(period, period)

    klines, oi_hist, funding = await asyncio.gather(
        binance_client.get_klines(
            symbol=symbol, interval=kline_interval, limit=limit,
        ),
        binance_client.get_open_interest_hist(
            symbol=symbol, period=period, limit=limit,
        ),
        _safe_fetch_funding(binance_client, symbol),
    )
    return klines, oi_hist, funding


async def _safe_fetch_funding(binance_client, symbol: str):
    """Fetch funding rate, returning empty list on failure."""
    try:
        return await binance_client.get_funding_rate(symbol=symbol, limit=100)
    except Exception:
        return []


def merge_data(klines: list[dict], oi_hist: list[dict], funding: list[dict], period: str) -> list[dict]:
    """
    Merge klines with OI by nearest timestamp.
    Funding (every 8h) is matched to the nearest kline.
    """
    period_ms = PERIOD_MS.get(period, 3_600_000)
    merged = []

    for k in klines:
        vol = k["volume"]
        point = {
            "timestamp": k["open_time"],
            "datetime": _ts_to_str(k["open_time"]),
            "open": k["open"],
            "high": k["high"],
            "low": k["low"],
            "close": k["close"],
            "volume": vol,
            "quote_volume": k.get("quote_volume", 0),
            "taker_buy_volume": k["taker_buy_volume"],
            "tbr": k["taker_buy_volume"] / vol if vol > 0 else 0.5,
        }

        # Match OI (nearest within 1 period tolerance)
        oi_match = _find_nearest(oi_hist, k["open_time"], tolerance_ms=period_ms * 2)
        if oi_match:
            point["oi"] = oi_match["sum_open_interest"]
            point["oi_value"] = oi_match["sum_open_interest_value"]

        # Match funding (nearest within 8h)
        fr_match = _find_nearest(funding, k["open_time"], tolerance_ms=8 * 3_600_000, ts_key="funding_time")
        if fr_match:
            point["funding_rate"] = fr_match["funding_rate"]

        merged.append(point)

    return merged


# ─────────────────────────────────────────────
# METRICS & DIVERGENCE
# ─────────────────────────────────────────────

def detect_divergence(oi_roc: float, price_roc: float,
                      threshold_oi: float = 1.0, threshold_price: float = 1.0) -> str:
    """
    Classify divergence between OI and Price ROC.
      BEAR_DIV      — OI↑, Price↓  (crash warning)
      BULL_DIV      — OI↓, Price↑  (rally sin convicción)
      CONFIRM_BULL  — both up       (healthy)
      CONFIRM_BEAR  — both down     (orderly capitulation)
      NEUTRAL       — no significant move
    """
    oi_up = oi_roc > threshold_oi
    oi_down = oi_roc < -threshold_oi
    price_up = price_roc > threshold_price
    price_down = price_roc < -threshold_price

    if oi_up and price_down:
        return "BEAR_DIV"
    elif oi_down and price_up:
        return "BULL_DIV"
    elif oi_up and price_up:
        return "CONFIRM_BULL"
    elif oi_down and price_down:
        return "CONFIRM_BEAR"
    else:
        return "NEUTRAL"


def detect_energy_divergence(oi_value_roc: float, oi_tokens_roc: float,
                              threshold_delta: float = 0.5,
                              threshold_tok: float = 0.3) -> str:
    """
    Classify OI Energy Divergence using the tuple (ΔE, ROC(OI_tok)).
    Implements the 5-state taxonomy from the OIED paper (Table 2):
    
      ΔE > 0, ROC(tok) > 0  →  ENERGY_INFLOW       (bullish: capital entering at rising prices)
      ΔE > 0, ROC(tok) < 0  →  ENERGY_RECOVERY      (price recovery during deleveraging)
      ΔE < 0, ROC(tok) > 0  →  ENERGY_TRAP           (⚠ bearish precursor: capital eroding
                                                        while participation persists/grows)
      ΔE < 0, ROC(tok) ≈ 0  →  ENERGY_DISSIPATION    (⚠ strong warning: energy draining,
                                                        particles frozen — pre-jamming)
      ΔE < 0, ROC(tok) < 0  →  ENERGY_DELEVERAGING   (active crash / forced liquidation)
      
    The critical bearish precursor is ENERGY_TRAP: ΔE < 0 with ROC(OI_tok) ≥ 0.
    This means the market is losing energy while maintaining or increasing trapped participants.
    In Langevin terms: kinetic energy dissipating without particle escape → jamming.
    """
    delta = oi_value_roc - oi_tokens_roc
    
    if abs(delta) <= threshold_delta:
        return "ENERGY_NEUTRAL"
    
    tok_up = oi_tokens_roc > threshold_tok
    tok_down = oi_tokens_roc < -threshold_tok
    tok_flat = not tok_up and not tok_down  # |ROC(tok)| ≤ threshold
    
    if delta > threshold_delta:
        # ΔE positive: OI_value growing faster than OI_tokens
        if tok_down:
            return "ENERGY_RECOVERY"       # Price rising, positions closing
        else:
            return "ENERGY_INFLOW"         # Genuine capital inflow (bullish)
    else:
        # ΔE negative: OI_value shrinking faster than OI_tokens
        if tok_up:
            return "ENERGY_TRAP"           # 🔴 Most dangerous: trapped longs, energy draining
        elif tok_flat:
            return "ENERGY_DISSIPATION"    # 🟠 Strong warning: energy draining, mass frozen
        else:
            return "ENERGY_DELEVERAGING"   # Active crash / orderly deleveraging


def compute_metrics(data: list[dict], window: int = 6) -> list[dict]:
    """
    Compute ROC, divergence type, jamming ratio, and Langevin energy metrics.
    Interpolates OI gaps of ≤2 periods linearly.
    """
    # — Interpolate small OI gaps (≤2 consecutive) —
    _interpolate_oi_gaps(data, max_gap=2)

    # — Compute implied price for all points that have both OI fields —
    for d in data:
        oi_tok = d.get("oi")
        oi_val = d.get("oi_value")
        if oi_tok and oi_tok > 0 and oi_val:
            d["implied_price"] = oi_val / oi_tok

    for i in range(window, len(data)):
        current = data[i]
        lookback = data[i - window]

        # OI Tokens Rate of Change (masa)
        lb_oi = lookback.get("oi")
        cur_oi = current.get("oi")
        if lb_oi and lb_oi > 0 and cur_oi:
            current["oi_roc"] = (cur_oi / lb_oi - 1) * 100
        else:
            current["oi_roc"] = 0.0

        # OI Value Rate of Change (energía)
        lb_val = lookback.get("oi_value")
        cur_val = current.get("oi_value")
        if lb_val and lb_val > 0 and cur_val:
            current["oi_value_roc"] = (cur_val / lb_val - 1) * 100
        else:
            current["oi_value_roc"] = 0.0

        # Energy delta: ROC(value) - ROC(tokens)
        # Positive = new positions at higher prices (bullish pressure)
        # Negative = capital draining faster than positions close (bearish)
        current["energy_delta"] = round(
            current["oi_value_roc"] - current["oi_roc"], 4
        )

        # Energy divergence classification
        current["energy_state"] = detect_energy_divergence(
            current["oi_value_roc"], current["oi_roc"]
        )

        # Price Rate of Change
        if lookback["close"] > 0:
            current["price_roc"] = (current["close"] / lookback["close"] - 1) * 100
        else:
            current["price_roc"] = 0.0

        # Divergence classification (OI tokens vs price)
        current["divergence"] = detect_divergence(
            oi_roc=current["oi_roc"],
            price_roc=current["price_roc"],
        )

        # Jamming ratio (OI / avg volume over window)
        vol_sum = sum(d["volume"] for d in data[i - window:i])
        avg_vol = vol_sum / window if window > 0 else 0
        if avg_vol > 0 and cur_oi:
            current["jamming_ratio"] = round(cur_oi / avg_vol, 4)

    return data


def _interpolate_oi_gaps(data: list[dict], max_gap: int = 2):
    """Fill small OI gaps (≤max_gap consecutive nulls) with linear interpolation."""
    n = len(data)
    i = 0
    while i < n:
        if data[i].get("oi") is None:
            # Find gap extent
            gap_start = i
            while i < n and data[i].get("oi") is None:
                i += 1
            gap_end = i  # first index WITH data after gap
            gap_len = gap_end - gap_start

            if gap_len <= max_gap and gap_start > 0 and gap_end < n:
                oi_before = data[gap_start - 1].get("oi")
                oi_after = data[gap_end].get("oi")
                if oi_before is not None and oi_after is not None:
                    for j in range(gap_start, gap_end):
                        frac = (j - gap_start + 1) / (gap_len + 1)
                        data[j]["oi"] = oi_before + (oi_after - oi_before) * frac
                        data[j]["oi_interpolated"] = True

                        # Also interpolate OI value if available
                        val_before = data[gap_start - 1].get("oi_value")
                        val_after = data[gap_end].get("oi_value")
                        if val_before is not None and val_after is not None:
                            data[j]["oi_value"] = val_before + (val_after - val_before) * frac
        else:
            i += 1


# ─────────────────────────────────────────────
# SWING DIVERGENCE DETECTION
# ─────────────────────────────────────────────

def _find_swing_highs(data: list[dict], key: str, window: int = 5) -> list[dict]:
    """Find local maxima (swing highs) in a series."""
    highs = []
    for i in range(window, len(data) - window):
        val = data[i].get(key)
        if val is None:
            continue
        is_high = True
        for j in range(i - window, i + window + 1):
            if j == i:
                continue
            other = data[j].get(key)
            if other is not None and other >= val:
                is_high = False
                break
        if is_high:
            highs.append({
                "index": i,
                "timestamp": data[i]["timestamp"],
                "datetime": data[i]["datetime"],
                "value": val,
            })
    return highs


def _find_swing_lows(data: list[dict], key: str, window: int = 5) -> list[dict]:
    """Find local minima (swing lows) in a series."""
    lows = []
    for i in range(window, len(data) - window):
        val = data[i].get(key)
        if val is None:
            continue
        is_low = True
        for j in range(i - window, i + window + 1):
            if j == i:
                continue
            other = data[j].get(key)
            if other is not None and other <= val:
                is_low = False
                break
        if is_low:
            lows.append({
                "index": i,
                "timestamp": data[i]["timestamp"],
                "datetime": data[i]["datetime"],
                "value": val,
            })
    return lows


def detect_swing_divergences(data: list[dict], swing_window: int = 5) -> list[dict]:
    """
    Classic swing divergence (like RSI divergence, but with OI):
      - Price HH + OI LH → BEARISH_SWING_DIV
      - Price LL + OI HL → BULLISH_SWING_DIV
    """
    price_highs = _find_swing_highs(data, "close", swing_window)
    oi_highs = _find_swing_highs(data, "oi", swing_window)
    price_lows = _find_swing_lows(data, "close", swing_window)
    oi_lows = _find_swing_lows(data, "oi", swing_window)

    divergences = []

    # Bearish: price Higher High, OI Lower High
    for i in range(1, len(price_highs)):
        ph_prev = price_highs[i - 1]
        ph_curr = price_highs[i]

        oh_prev = _find_nearest_swing(oi_highs, ph_prev["timestamp"])
        oh_curr = _find_nearest_swing(oi_highs, ph_curr["timestamp"])

        if oh_prev and oh_curr and oh_prev["index"] != oh_curr["index"]:
            price_hh = ph_curr["value"] > ph_prev["value"]
            oi_lh = oh_curr["value"] < oh_prev["value"]

            if price_hh and oi_lh:
                divergences.append({
                    "type": "BEARISH_SWING_DIV",
                    "timestamp": ph_curr["timestamp"],
                    "datetime": ph_curr["datetime"],
                    "price_prev_high": round(ph_prev["value"], 6),
                    "price_curr_high": round(ph_curr["value"], 6),
                    "oi_prev_high": round(oh_prev["value"], 2),
                    "oi_curr_high": round(oh_curr["value"], 2),
                    "severity": "HIGH",
                    "description": (
                        f"Precio HH (${ph_curr['value']:.4f} > ${ph_prev['value']:.4f}) "
                        f"pero OI LH ({oh_curr['value']/1e6:.1f}M < {oh_prev['value']/1e6:.1f}M)"
                    ),
                })

    # Bullish: price Lower Low, OI Higher Low
    for i in range(1, len(price_lows)):
        pl_prev = price_lows[i - 1]
        pl_curr = price_lows[i]

        ol_prev = _find_nearest_swing(oi_lows, pl_prev["timestamp"])
        ol_curr = _find_nearest_swing(oi_lows, pl_curr["timestamp"])

        if ol_prev and ol_curr and ol_prev["index"] != ol_curr["index"]:
            price_ll = pl_curr["value"] < pl_prev["value"]
            oi_hl = ol_curr["value"] > ol_prev["value"]

            if price_ll and oi_hl:
                divergences.append({
                    "type": "BULLISH_SWING_DIV",
                    "timestamp": pl_curr["timestamp"],
                    "datetime": pl_curr["datetime"],
                    "price_prev_low": round(pl_prev["value"], 6),
                    "price_curr_low": round(pl_curr["value"], 6),
                    "oi_prev_low": round(ol_prev["value"], 2),
                    "oi_curr_low": round(ol_curr["value"], 2),
                    "severity": "HIGH",
                    "description": (
                        f"Precio LL (${pl_curr['value']:.4f} < ${pl_prev['value']:.4f}) "
                        f"pero OI HL ({ol_curr['value']/1e6:.1f}M > {ol_prev['value']/1e6:.1f}M)"
                    ),
                })

    # Sort by timestamp
    divergences.sort(key=lambda d: d["timestamp"])
    return divergences


def _find_nearest_swing(swings: list[dict], target_ts: int) -> Optional[dict]:
    """Find the swing point closest in time to target_ts."""
    if not swings:
        return None
    best = min(swings, key=lambda s: abs(s["timestamp"] - target_ts))
    return best


# ─────────────────────────────────────────────
# INTERPRETIVE CONTEXT
# ─────────────────────────────────────────────

def interpret_divergences(roc_divs: list[dict], swing_divs: list[dict],
                          energy_summary: Optional[dict] = None) -> str:
    """Generate a human-readable interpretation of detected divergences + energy state."""
    parts = []

    bear_roc = len(roc_divs)
    bear_swing = len([d for d in swing_divs if d["type"] == "BEARISH_SWING_DIV"])
    bull_swing = len([d for d in swing_divs if d["type"] == "BULLISH_SWING_DIV"])

    if bear_roc > 0:
        parts.append(
            f"{bear_roc} zona{'s' if bear_roc > 1 else ''} BEAR_DIV (OI↑/Price↓) detectada{'s' if bear_roc > 1 else ''}."
        )
    if bear_swing > 0:
        parts.append(
            f"{bear_swing} swing divergence bearish (precio HH pero OI LH). "
            "Señal de agotamiento del rally."
        )
    if bull_swing > 0:
        parts.append(
            f"{bull_swing} swing divergence bullish (precio LL pero OI HL). "
            "Acumulación en soporte."
        )

    if bear_roc >= 3 or bear_swing >= 1:
        parts.append(
            "Patrón de agotamiento confirmado. Divergencia OI/Precio es señal predictiva de crash "
            "(lead time típico: 8-12h)."
        )
    elif bear_roc == 0 and bear_swing == 0 and bull_swing == 0:
        parts.append("Sin divergencias significativas. OI y precio se mueven en sintonía.")

    # Energy (Langevin OIED) interpretation — using paper's taxonomy
    if energy_summary:
        trap_n = energy_summary.get("trap_count", 0)
        dissipation_n = energy_summary.get("dissipation_count", 0)
        delev_n = energy_summary.get("deleveraging_count", 0)
        inflow_n = energy_summary.get("inflow_count", 0)
        trap_streak = energy_summary.get("trap_streak", 0)
        dissipation_streak = energy_summary.get("dissipation_streak", 0)
        curr_state = energy_summary.get("current_state")
        curr_delta = energy_summary.get("current_delta", 0)
        peak_order = energy_summary.get("peak_ordering")

        # ENERGY_TRAP is the Ψ₂ signal — most dangerous bearish precursor
        if trap_n > 0:
            parts.append(
                f"⚡ Ψ₂ ENERGY_TRAP: {trap_n} periodos donde ΔE<0 con ROC(OI_tok)>0 — "
                "capital erosionándose mientras participación sube (longs atrapados)."
            )
        if dissipation_n > 0:
            parts.append(
                f"⚡ ENERGY_DISSIPATION: {dissipation_n} periodos donde ΔE<0 con OI_tok estable — "
                "energía disipándose sin liberación de partículas (pre-jamming)."
            )
        if trap_streak >= 3:
            parts.append(
                f"⚠️ Racha de {trap_streak} periodos consecutivos ENERGY_TRAP — "
                "señal Ψ₂ sostenida, lead time típico 36h antes del crash."
            )
        if dissipation_streak >= 3:
            parts.append(
                f"⚠️ Racha de {dissipation_streak} periodos ENERGY_DISSIPATION — "
                "sistema jammed, energía drenándose sostenidamente."
            )
        if delev_n > 0 and trap_n == 0:
            parts.append(
                f"⚡ ENERGY_DELEVERAGING: {delev_n} periodos — "
                "liquidación activa, crash en progreso."
            )
        if inflow_n > 0 and trap_n == 0 and dissipation_n == 0:
            parts.append(
                f"⚡ Energía Langevin: {inflow_n} periodos ENERGY_INFLOW "
                "(posiciones nuevas a precios altos, demanda fuerte)."
            )

        # Peak ordering sequence (Ψ₂ paper Section 4.4)
        if peak_order:
            seq = peak_order.get("sequence", "")
            if seq == "OIval→OItok→Price":
                parts.append(
                    f"🔴 Secuencia de picos: OI_val→OI_tok→Price (leadtime causal confirmado). "
                    f"OI_val peak: {peak_order.get('oi_val_peak_time', '?')}, "
                    f"OI_tok peak: {peak_order.get('oi_tok_peak_time', '?')}, "
                    f"Price peak: {peak_order.get('price_peak_time', '?')}."
                )
            elif seq:
                parts.append(f"Secuencia de picos: {seq}.")

        # Current state
        if curr_state and curr_state != "ENERGY_NEUTRAL":
            state_desc = {
                "ENERGY_TRAP": "🔴 TRAMPA ENERGÉTICA (ΔE<0, OI_tok↑)",
                "ENERGY_DISSIPATION": "🟠 DISIPACIÓN (ΔE<0, OI_tok≈0)",
                "ENERGY_DELEVERAGING": "🔻 DELEVERAGING (ΔE<0, OI_tok↓)",
                "ENERGY_INFLOW": "🟢 ENTRADA DE CAPITAL (ΔE>0, OI_tok↑)",
                "ENERGY_RECOVERY": "🟡 RECUPERACIÓN (ΔE>0, OI_tok↓)",
            }.get(curr_state, curr_state)
            parts.append(f"Estado energético actual: {state_desc} (ΔE={curr_delta:+.2f}%).")

    return " ".join(parts)


def _detect_peak_ordering(data: list[dict]) -> Optional[dict]:
    """
    Detect the peak ordering sequence OI_val → OI_tok → Price.
    
    From the paper (Section 4.4):
    The sequence OI_val peak → OI_tok peak → Price peak reveals the causal chain:
    energy peaks first, then participation peaks, then price peaks.
    This ordering is a strong crash precursor.
    """
    # Need sufficient data with OI values
    points_with_oi = [d for d in data if d.get("oi_value") is not None and d.get("oi") is not None]
    if len(points_with_oi) < 10:
        return None
    
    # Find peak indices (global max in the series)
    oi_val_max = max(points_with_oi, key=lambda d: d["oi_value"])
    oi_tok_max = max(points_with_oi, key=lambda d: d["oi"])
    price_max = max(data, key=lambda d: d["close"])
    
    peaks = [
        ("OIval", oi_val_max["timestamp"], oi_val_max["datetime"]),
        ("OItok", oi_tok_max["timestamp"], oi_tok_max["datetime"]),
        ("Price", price_max["timestamp"], price_max["datetime"]),
    ]
    
    # Sort by timestamp
    peaks.sort(key=lambda p: p[1])
    
    # Are all peaks in the past (not at the very end)? 
    # Only meaningful if peaks are not at the last data point
    last_ts = data[-1]["timestamp"]
    all_peaked = all(p[1] < last_ts for p in peaks)
    
    sequence = "→".join(p[0] for p in peaks)
    
    return {
        "sequence": sequence,
        "canonical": sequence == "OIval→OItok→Price",
        "all_peaked": all_peaked,
        "oi_val_peak_time": oi_val_max["datetime"],
        "oi_val_peak_value": round(oi_val_max["oi_value"], 2),
        "oi_tok_peak_time": oi_tok_max["datetime"],
        "oi_tok_peak_value": round(oi_tok_max["oi"], 2),
        "price_peak_time": price_max["datetime"],
        "price_peak_value": round(price_max["close"], 6),
        "oi_val_to_oi_tok_lag": oi_tok_max["timestamp"] - oi_val_max["timestamp"],
        "oi_tok_to_price_lag": price_max["timestamp"] - oi_tok_max["timestamp"],
    }


def _compute_energy_summary(data: list[dict]) -> dict:
    """Compute summary stats for Langevin energy divergence using paper's taxonomy."""
    # Count each state
    trap_points = [d for d in data if d.get("energy_state") == "ENERGY_TRAP"]
    dissipation_points = [d for d in data if d.get("energy_state") == "ENERGY_DISSIPATION"]
    delev_points = [d for d in data if d.get("energy_state") == "ENERGY_DELEVERAGING"]
    inflow_points = [d for d in data if d.get("energy_state") == "ENERGY_INFLOW"]
    recovery_points = [d for d in data if d.get("energy_state") == "ENERGY_RECOVERY"]

    # Compute current streak for critical states (TRAP or DISSIPATION from end)
    trap_streak = 0
    for d in reversed(data):
        if d.get("energy_state") == "ENERGY_TRAP":
            trap_streak += 1
        elif d.get("energy_state") is not None:
            break

    dissipation_streak = 0
    for d in reversed(data):
        if d.get("energy_state") == "ENERGY_DISSIPATION":
            dissipation_streak += 1
        elif d.get("energy_state") is not None:
            break

    # Combined bearish streak (TRAP or DISSIPATION consecutively)
    bearish_energy_streak = 0
    for d in reversed(data):
        st = d.get("energy_state")
        if st in ("ENERGY_TRAP", "ENERGY_DISSIPATION"):
            bearish_energy_streak += 1
        elif st is not None:
            break

    # Peak ordering detection
    peak_ordering = _detect_peak_ordering(data)

    last = data[-1] if data else {}

    # Bearish zones = TRAP + DISSIPATION (the critical Ψ₂ precursors)
    bearish_energy_zones = [
        {
            "datetime": d["datetime"],
            "state": d.get("energy_state"),
            "oi_value_roc": round(d.get("oi_value_roc", 0), 2),
            "oi_tokens_roc": round(d.get("oi_roc", 0), 2),
            "energy_delta": d.get("energy_delta", 0),
        }
        for d in data
        if d.get("energy_state") in ("ENERGY_TRAP", "ENERGY_DISSIPATION")
    ]

    return {
        # State counts (paper taxonomy)
        "trap_count": len(trap_points),
        "dissipation_count": len(dissipation_points),
        "deleveraging_count": len(delev_points),
        "inflow_count": len(inflow_points),
        "recovery_count": len(recovery_points),
        
        # Streaks
        "trap_streak": trap_streak,
        "dissipation_streak": dissipation_streak,
        "bearish_energy_streak": bearish_energy_streak,
        
        # Current state
        "current_state": last.get("energy_state"),
        "current_delta": last.get("energy_delta", 0),
        "current_oi_value_roc": round(last.get("oi_value_roc", 0), 2),
        "current_oi_tokens_roc": round(last.get("oi_roc", 0), 2),
        "current_implied_price": last.get("implied_price"),
        
        # Peak ordering (Ψ₂ paper Section 4.4)
        "peak_ordering": peak_ordering,
        
        # Bearish energy zones (TRAP + DISSIPATION detail)
        "bearish_energy_zones": bearish_energy_zones,
    }


# ─────────────────────────────────────────────
# CHART GENERATION (Plotly)
# ─────────────────────────────────────────────

def generate_chart(data: list[dict], swing_divs: list[dict],
                   symbol: str, period: str,
                   show_volume: bool = True,
                   show_funding: bool = True,
                   annotate: bool = True) -> Optional[object]:
    """
    Generate a multi-panel Plotly chart. Returns the figure object,
    or None if Plotly is not available.
    """
    if not HAS_PLOTLY:
        return None

    # ── Extract series ──
    timestamps = [d["datetime"] for d in data]
    prices = [d["close"] for d in data]
    ois = [d.get("oi") for d in data]
    volumes = [d["volume"] for d in data]
    tbrs = [d.get("tbr", 0.5) for d in data]
    oi_rocs = [d.get("oi_roc", 0) for d in data]
    price_rocs = [d.get("price_roc", 0) for d in data]
    fundings = [d.get("funding_rate") for d in data]

    has_funding = show_funding and any(f is not None for f in fundings)

    vol_colors = [
        '#22c55e' if tbr > 0.52 else '#ef4444' if tbr < 0.48 else '#555555'
        for tbr in tbrs
    ]

    # ── Extract energy series ──
    oi_value_rocs = [d.get("oi_value_roc", 0) for d in data]
    oi_tokens_rocs = [d.get("oi_roc", 0) for d in data]
    energy_deltas = [d.get("energy_delta", 0) for d in data]
    energy_states = [d.get("energy_state") for d in data]
    implied_prices = [d.get("implied_price") for d in data]
    has_energy = any(d.get("oi_value") is not None for d in data)

    # ── Subplot layout ──
    # Build row heights and specs
    rows_config = []
    titles = []

    # Panel 1: Price + OI + implied_price (always)
    rows_config.append({"secondary_y": True})
    titles.append(f"{symbol} — Precio vs OI ({period})")

    if show_volume:
        rows_config.append({"secondary_y": False})
        titles.append("Volumen (color = taker buy ratio)")

    rows_config.append({"secondary_y": False})
    titles.append("ROC: OI vs Precio (divergencias)")

    # Energy panel (ROC value vs tokens + delta)
    if has_energy:
        rows_config.append({"secondary_y": False})
        titles.append("⚡ Energía Langevin: ROC(OI_value) vs ROC(OI_tokens)")

    if has_funding:
        rows_config.append({"secondary_y": False})
        titles.append("Funding Rate")

    total_rows = len(rows_config)

    # Height distribution — dynamic based on panel count
    height_map = {
        2: [0.60, 0.40],
        3: [0.50, 0.25, 0.25],
        4: [0.42, 0.18, 0.22, 0.18],
        5: [0.36, 0.15, 0.18, 0.18, 0.13],
        6: [0.30, 0.14, 0.16, 0.16, 0.12, 0.12],
    }
    row_heights = height_map.get(total_rows, [1.0 / total_rows] * total_rows)

    fig = make_subplots(
        rows=total_rows, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=row_heights,
        specs=[[spec] for spec in rows_config],
        subplot_titles=titles,
    )

    # ── Track current row ──
    row_idx = 1

    # ── Panel 1: Price + OI ──
    # Price (left axis)
    fig.add_trace(
        go.Scatter(
            x=timestamps, y=prices,
            name="Precio",
            line=dict(color="#e5e5e5", width=2),
        ),
        row=row_idx, col=1, secondary_y=False,
    )

    # OI (right axis)
    oi_display = [o / 1e6 if o else None for o in ois]
    fig.add_trace(
        go.Scatter(
            x=timestamps, y=oi_display,
            name="Open Interest (M)",
            line=dict(color="#3b82f6", width=2),
            fill="tozeroy",
            fillcolor="rgba(59,130,246,0.08)",
        ),
        row=row_idx, col=1, secondary_y=True,
    )

    # Implied price (OI_value / OI_tokens) — dashed overlay on price axis
    if has_energy and any(ip is not None for ip in implied_prices):
        fig.add_trace(
            go.Scatter(
                x=timestamps, y=implied_prices,
                name="Precio Implícito (OI_val/OI_tok)",
                line=dict(color="#f59e0b", width=1.5, dash="dot"),
                opacity=0.8,
            ),
            row=row_idx, col=1, secondary_y=False,
        )

    # BEAR_DIV shading on main panel
    for i, d in enumerate(data):
        if d.get("divergence") == "BEAR_DIV":
            x1 = data[min(i + 1, len(data) - 1)]["datetime"]
            fig.add_vrect(
                x0=d["datetime"], x1=x1,
                fillcolor="rgba(239,68,68,0.12)",
                line_width=0,
                row=row_idx, col=1,
            )

    # Annotate swing divergences
    if annotate and swing_divs:
        for div in swing_divs:
            if div["type"] == "BEARISH_SWING_DIV":
                ts_str = _ts_to_str(div["timestamp"])
                oi_cmp = (
                    f"OI: {div['oi_curr_high']/1e6:.1f}M < {div['oi_prev_high']/1e6:.1f}M"
                )
                fig.add_annotation(
                    x=ts_str,
                    y=div["price_curr_high"],
                    text=f"🔴 OI no confirma\n{oi_cmp}",
                    showarrow=True,
                    arrowhead=2,
                    arrowcolor="#ef4444",
                    font=dict(size=9, color="#ef4444"),
                    bgcolor="rgba(0,0,0,0.8)",
                    bordercolor="#ef4444",
                    row=row_idx, col=1,
                )
            elif div["type"] == "BULLISH_SWING_DIV":
                ts_str = _ts_to_str(div["timestamp"])
                oi_cmp = (
                    f"OI: {div['oi_curr_low']/1e6:.1f}M > {div['oi_prev_low']/1e6:.1f}M"
                )
                fig.add_annotation(
                    x=ts_str,
                    y=div["price_curr_low"],
                    text=f"🟢 OI acumula\n{oi_cmp}",
                    showarrow=True,
                    arrowhead=2,
                    arrowcolor="#22c55e",
                    font=dict(size=9, color="#22c55e"),
                    bgcolor="rgba(0,0,0,0.8)",
                    bordercolor="#22c55e",
                    row=row_idx, col=1,
                )

    fig.update_yaxes(title_text="Precio ($)", row=row_idx, col=1, secondary_y=False)
    fig.update_yaxes(title_text="OI (M)", row=row_idx, col=1, secondary_y=True)
    row_idx += 1

    # ── Panel 2: Volume ──
    if show_volume:
        fig.add_trace(
            go.Bar(
                x=timestamps,
                y=[v / 1e6 for v in volumes],
                name="Volume (M)",
                marker_color=vol_colors,
                opacity=0.7,
            ),
            row=row_idx, col=1,
        )
        fig.update_yaxes(title_text="Vol (M)", row=row_idx, col=1)
        row_idx += 1

    # ── Panel 3: ROC divergence ──
    fig.add_trace(
        go.Scatter(
            x=timestamps, y=oi_rocs,
            name="OI ROC%",
            line=dict(color="#3b82f6", width=1.5),
        ),
        row=row_idx, col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=timestamps, y=price_rocs,
            name="Price ROC%",
            line=dict(color="#e5e5e5", width=1.5),
        ),
        row=row_idx, col=1,
    )
    fig.add_hline(y=0, line_dash="dash", line_color="#333", row=row_idx, col=1)

    # BEAR_DIV shading on ROC panel
    for i, d in enumerate(data):
        if d.get("divergence") == "BEAR_DIV":
            x1 = data[min(i + 1, len(data) - 1)]["datetime"]
            fig.add_vrect(
                x0=d["datetime"], x1=x1,
                fillcolor="rgba(239,68,68,0.15)",
                line_width=0,
                row=row_idx, col=1,
            )

    fig.update_yaxes(title_text="ROC %", row=row_idx, col=1)
    row_idx += 1

    # ── Energy Panel: ROC(OI_value) vs ROC(OI_tokens) + delta ──
    if has_energy:
        # ROC lines
        fig.add_trace(
            go.Scatter(
                x=timestamps, y=oi_value_rocs,
                name="ROC OI_value% (energía)",
                line=dict(color="#f59e0b", width=1.5),
            ),
            row=row_idx, col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=timestamps, y=oi_tokens_rocs,
                name="ROC OI_tokens% (masa)",
                line=dict(color="#8b5cf6", width=1.5),
            ),
            row=row_idx, col=1,
        )

        # Energy delta as bars — colored by OIED taxonomy
        delta_colors = []
        for j, d in enumerate(data):
            st = d.get("energy_state")
            if st == "ENERGY_TRAP":
                delta_colors.append('rgba(239,68,68,0.55)')       # Red — most dangerous
            elif st == "ENERGY_DISSIPATION":
                delta_colors.append('rgba(249,115,22,0.45)')      # Orange — strong warning
            elif st == "ENERGY_DELEVERAGING":
                delta_colors.append('rgba(239,68,68,0.30)')       # Dim red — crash in progress
            elif st == "ENERGY_INFLOW":
                delta_colors.append('rgba(34,197,94,0.45)')       # Green — bullish
            elif st == "ENERGY_RECOVERY":
                delta_colors.append('rgba(234,179,8,0.35)')       # Yellow — mixed
            else:
                delta_colors.append('rgba(100,100,100,0.15)')     # Gray — neutral
        
        fig.add_trace(
            go.Bar(
                x=timestamps, y=energy_deltas,
                name="ΔE (OI_val−OI_tok)",
                marker_color=delta_colors,
                opacity=0.7,
            ),
            row=row_idx, col=1,
        )

        fig.add_hline(y=0, line_dash="dash", line_color="#333", row=row_idx, col=1)

        # Shade ENERGY_TRAP zones (most critical)
        for i, d in enumerate(data):
            st = d.get("energy_state")
            if st == "ENERGY_TRAP":
                x1 = data[min(i + 1, len(data) - 1)]["datetime"]
                fig.add_vrect(
                    x0=d["datetime"], x1=x1,
                    fillcolor="rgba(239,68,68,0.10)",
                    line_width=0,
                    row=row_idx, col=1,
                )
            elif st == "ENERGY_DISSIPATION":
                x1 = data[min(i + 1, len(data) - 1)]["datetime"]
                fig.add_vrect(
                    x0=d["datetime"], x1=x1,
                    fillcolor="rgba(249,115,22,0.08)",
                    line_width=0,
                    row=row_idx, col=1,
                )

        fig.update_yaxes(title_text="Δ Energy %", row=row_idx, col=1)
        row_idx += 1

    # ── Funding Panel (optional) ──
    if has_funding:
        fr_values = [f * 100 if f is not None else 0 for f in fundings]
        fr_colors = ['#22c55e' if f >= 0 else '#ef4444' for f in fr_values]
        fig.add_trace(
            go.Bar(
                x=timestamps, y=fr_values,
                name="Funding %",
                marker_color=fr_colors,
                opacity=0.8,
            ),
            row=row_idx, col=1,
        )
        fig.update_yaxes(title_text="Funding %", row=row_idx, col=1)

    # ── Global layout ──
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#0a0a0a",
        plot_bgcolor="#0a0a0a",
        font=dict(family="JetBrains Mono, monospace", size=10, color="#888"),
        height=max(600, 150 * total_rows + 100),
        showlegend=True,
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02,
            xanchor="right", x=1,
            font=dict(size=9),
        ),
        margin=dict(l=50, r=50, t=60, b=30),
    )

    return fig


# ─────────────────────────────────────────────
# MAIN TOOL ENTRY POINT
# ─────────────────────────────────────────────

# Default directory for chart files
CHART_OUTPUT_DIR = Path(tempfile.gettempdir()) / "psi_jam_charts"


async def oi_level(
    binance_client,
    symbol: str,
    period: str = "1h",
    limit: int = 100,
    divergence_window: int = 6,
    show_volume: bool = True,
    show_funding: bool = True,
    annotate: bool = True,
    include_series: bool = False,
    chart_dir: Optional[str] = None,
) -> dict:
    """
    OI Level — Análisis OIED (Open Interest Energy Divergence).
    
    Descompone el Open Interest en masa (tokens) y energía (value) para
    clasificar el estado energético del mercado en 5 estados Langevin.
    Genera un gráfico multi-panel y devuelve el análisis completo.
    
    El gráfico se guarda como archivo HTML en disco (no se embebe en la
    respuesta MCP) para mantener el payload liviano.
    
    Parameters
    ----------
    symbol : str
        Par de trading (e.g. BTCUSDT).
    period : str
        Periodo del OI histórico: 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d.
    limit : int
        Número de puntos de datos (max ~500).
    divergence_window : int
        Ventana en periodos para calcular ROC.
    include_series : bool
        Si True, incluye arrays crudos en la respuesta (payload grande).
    chart_dir : str, optional
        Directorio donde se guarda el HTML del chart.
    
    Returns
    -------
    dict
        chart_file, divergences, energy (5-state OIED), current state, context.
    """
    symbol = symbol.upper()

    # Validate period
    if period not in VALID_OI_PERIODS:
        return {"error": f"Invalid period '{period}'. Valid: {VALID_OI_PERIODS}"}

    # 1. Fetch data in parallel
    klines, oi_hist, funding = await fetch_oi_chart_data(binance_client, symbol, period, limit)

    if not klines:
        return {"error": f"No kline data returned for {symbol}"}

    # 2. Merge
    data = merge_data(klines, oi_hist, funding, period)

    # 3. Compute metrics + divergences
    data = compute_metrics(data, window=divergence_window)

    # 4. Swing divergences
    swing_divs = detect_swing_divergences(data, swing_window=max(3, divergence_window // 2))

    # 5. ROC-based divergences
    roc_divs = [d for d in data if d.get("divergence") == "BEAR_DIV"]
    bull_divs = [d for d in data if d.get("divergence") == "BULL_DIV"]
    confirm_bull = [d for d in data if d.get("divergence") == "CONFIRM_BULL"]
    confirm_bear = [d for d in data if d.get("divergence") == "CONFIRM_BEAR"]

    # 6. Langevin energy summary
    energy_summary = _compute_energy_summary(data)

    # 7. Generate chart → save to file (not embedded in response)
    chart_file = None
    if HAS_PLOTLY:
        fig = generate_chart(
            data, swing_divs, symbol, period,
            show_volume=show_volume,
            show_funding=show_funding,
            annotate=annotate,
        )
        if fig:
            out_dir = Path(chart_dir) if chart_dir else CHART_OUTPUT_DIR
            out_dir.mkdir(parents=True, exist_ok=True)
            fname = f"{symbol}_{period}_{datetime.now(tz=timezone.utc).strftime('%Y%m%d_%H%M%S')}.html"
            chart_file = str(out_dir / fname)
            fig.write_html(chart_file, include_plotlyjs="cdn", full_html=True)

    # 8. Build response — lightweight, analysis-focused
    last = data[-1] if data else {}
    result = {
        "symbol": symbol,
        "period": period,
        "candles": len(data),
        "oi_data_points": sum(1 for d in data if d.get("oi") is not None),

        # Chart saved to disk
        "chart_file": chart_file,
        "plotly_available": HAS_PLOTLY,

        # Divergence summary
        "divergences": {
            "bear_div_count": len(roc_divs),
            "bear_div_zones": [
                {
                    "datetime": d["datetime"],
                    "oi_roc": round(d.get("oi_roc", 0), 2),
                    "price_roc": round(d.get("price_roc", 0), 2),
                }
                for d in roc_divs
            ],
            "bull_div_count": len(bull_divs),
            "swing_divergences": swing_divs,
            "total_confirm_bull": len(confirm_bull),
            "total_confirm_bear": len(confirm_bear),
        },

        # Langevin energy analysis
        "energy": energy_summary,

        # Current state
        "current": {
            "price": last.get("close"),
            "oi": last.get("oi"),
            "oi_value_usd": last.get("oi_value"),
            "oi_roc": round(last.get("oi_roc", 0), 2),
            "oi_value_roc": round(last.get("oi_value_roc", 0), 2),
            "price_roc": round(last.get("price_roc", 0), 2),
            "energy_delta": last.get("energy_delta", 0),
            "energy_state": last.get("energy_state"),
            "implied_price": last.get("implied_price"),
            "divergence": last.get("divergence"),
            "jamming_ratio": last.get("jamming_ratio"),
            "funding_rate": last.get("funding_rate"),
        },

        # Interpretive context
        "context": {
            "signal": interpret_divergences(roc_divs, swing_divs, energy_summary),
        },
    }

    # Raw time series — opt-in only (large payload)
    if include_series:
        result["series"] = {
            "timestamps": [d["datetime"] for d in data],
            "prices": [d["close"] for d in data],
            "oi": [d.get("oi") for d in data],
            "oi_value": [d.get("oi_value") for d in data],
            "implied_price": [d.get("implied_price") for d in data],
            "oi_roc": [d.get("oi_roc", 0) for d in data],
            "oi_value_roc": [d.get("oi_value_roc", 0) for d in data],
            "energy_delta": [d.get("energy_delta", 0) for d in data],
            "energy_state": [d.get("energy_state") for d in data],
            "price_roc": [d.get("price_roc", 0) for d in data],
            "divergences": [d.get("divergence", "NEUTRAL") for d in data],
            "volumes": [d["volume"] for d in data],
            "tbr": [d.get("tbr", 0.5) for d in data],
            "funding": [d.get("funding_rate") for d in data],
        }

    return result
