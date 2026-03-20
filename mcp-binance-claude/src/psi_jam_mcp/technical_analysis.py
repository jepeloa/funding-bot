"""
Technical Analysis Module
=========================
Pure-numpy implementation of standard technical indicators.
No external TA libraries required — everything computed from OHLCV arrays.

Indicators implemented:
  Trend:     SMA, EMA, MACD, ADX/DI+/DI-, Ichimoku Cloud, Supertrend
  Momentum:  RSI, Stochastic, CCI, Williams %R, MFI, ROC
  Volatility: Bollinger Bands, ATR, Keltner Channel
  Volume:    OBV, VWAP, A/D Line, CMF, Volume Profile
  Support/Resistance: Pivot Points, Fibonacci levels
  Candle patterns: Doji, Hammer, Engulfing, Morning/Evening Star
"""

import numpy as np
from typing import Optional


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def _ema(data: np.ndarray, period: int) -> np.ndarray:
    """Exponential Moving Average."""
    alpha = 2.0 / (period + 1)
    result = np.empty_like(data)
    result[0] = data[0]
    for i in range(1, len(data)):
        result[i] = alpha * data[i] + (1 - alpha) * result[i - 1]
    return result


def _sma(data: np.ndarray, period: int) -> np.ndarray:
    """Simple Moving Average with NaN padding."""
    result = np.full_like(data, np.nan)
    if len(data) < period:
        return result
    cumsum = np.cumsum(data)
    result[period - 1:] = (cumsum[period - 1:] - np.concatenate([[0], cumsum[:-period]])) / period
    return result


def _true_range(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray) -> np.ndarray:
    """True Range."""
    prev_close = np.concatenate([[closes[0]], closes[:-1]])
    tr1 = highs - lows
    tr2 = np.abs(highs - prev_close)
    tr3 = np.abs(lows - prev_close)
    return np.maximum(tr1, np.maximum(tr2, tr3))


def _wilder_smooth(data: np.ndarray, period: int) -> np.ndarray:
    """Wilder's smoothing (used in RSI, ADX, ATR)."""
    result = np.empty_like(data)
    result[:period] = np.nan
    result[period - 1] = np.mean(data[:period])
    for i in range(period, len(data)):
        result[i] = (result[i - 1] * (period - 1) + data[i]) / period
    return result


def _safe_round(val, decimals=4):
    """Safely round, handling NaN."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    return round(float(val), decimals)


def _series_tail(arr: np.ndarray, n: int = 20) -> list:
    """Return last n values as rounded list, replacing NaN with None."""
    tail = arr[-n:]
    return [_safe_round(v) for v in tail]


# ═══════════════════════════════════════════════════
# TREND INDICATORS
# ═══════════════════════════════════════════════════

def compute_moving_averages(closes: np.ndarray) -> dict:
    """SMA and EMA at multiple periods."""
    periods = [7, 20, 50, 100, 200]
    result = {}

    price = closes[-1]
    sma_summary = {}
    ema_summary = {}

    for p in periods:
        if len(closes) >= p:
            sma_val = _sma(closes, p)
            ema_val = _ema(closes, p)
            sma_last = float(sma_val[-1]) if not np.isnan(sma_val[-1]) else None
            ema_last = float(ema_val[-1])

            position_sma = "ABOVE" if price > sma_last else "BELOW" if sma_last else None
            position_ema = "ABOVE" if price > ema_last else "BELOW"

            sma_summary[f"SMA_{p}"] = {
                "value": _safe_round(sma_last),
                "price_position": position_sma,
            }
            ema_summary[f"EMA_{p}"] = {
                "value": _safe_round(ema_last),
                "price_position": position_ema,
            }

    # Golden/Death cross detection (SMA50 vs SMA200)
    cross = None
    if len(closes) >= 200:
        sma50 = _sma(closes, 50)
        sma200 = _sma(closes, 200)
        if not np.isnan(sma50[-1]) and not np.isnan(sma200[-1]):
            if not np.isnan(sma50[-2]) and not np.isnan(sma200[-2]):
                if sma50[-2] < sma200[-2] and sma50[-1] > sma200[-1]:
                    cross = "GOLDEN_CROSS"
                elif sma50[-2] > sma200[-2] and sma50[-1] < sma200[-1]:
                    cross = "DEATH_CROSS"

    return {"sma": sma_summary, "ema": ema_summary, "ma_cross_50_200": cross}


def compute_macd(closes: np.ndarray, fast: int = 12, slow: int = 26, signal: int = 9) -> dict:
    """MACD + Signal + Histogram."""
    if len(closes) < slow + signal:
        return {"error": "insufficient_data", "min_required": slow + signal}

    ema_fast = _ema(closes, fast)
    ema_slow = _ema(closes, slow)
    macd_line = ema_fast - ema_slow
    signal_line = _ema(macd_line, signal)
    histogram = macd_line - signal_line

    # Crossover detection
    crossover = None
    if histogram[-2] < 0 and histogram[-1] > 0:
        crossover = "BULLISH_CROSS"
    elif histogram[-2] > 0 and histogram[-1] < 0:
        crossover = "BEARISH_CROSS"

    # Divergence check (simplified: price up but MACD down, or vice versa)
    divergence = None
    lookback = min(20, len(closes) - 1)
    if lookback > 5:
        price_trend = closes[-1] - closes[-lookback]
        macd_trend = macd_line[-1] - macd_line[-lookback]
        if price_trend > 0 and macd_trend < 0:
            divergence = "BEARISH_DIVERGENCE"
        elif price_trend < 0 and macd_trend > 0:
            divergence = "BULLISH_DIVERGENCE"

    return {
        "macd": _safe_round(macd_line[-1]),
        "signal": _safe_round(signal_line[-1]),
        "histogram": _safe_round(histogram[-1]),
        "histogram_trend": "EXPANDING" if abs(histogram[-1]) > abs(histogram[-2]) else "CONTRACTING",
        "crossover": crossover,
        "divergence": divergence,
        "macd_series": _series_tail(macd_line),
        "signal_series": _series_tail(signal_line),
        "histogram_series": _series_tail(histogram),
    }


def compute_adx(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 14) -> dict:
    """ADX, DI+, DI- (trend strength and direction)."""
    n = len(closes)
    if n < period * 2:
        return {"error": "insufficient_data"}

    up_move = np.diff(highs)
    down_move = -np.diff(lows)

    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0)

    tr = _true_range(highs[1:], lows[1:], closes[:-1])

    atr = _wilder_smooth(tr, period)
    plus_di_raw = _wilder_smooth(plus_dm, period)
    minus_di_raw = _wilder_smooth(minus_dm, period)

    plus_di = 100 * plus_di_raw / (atr + 1e-12)
    minus_di = 100 * minus_di_raw / (atr + 1e-12)

    dx = 100 * np.abs(plus_di - minus_di) / (plus_di + minus_di + 1e-12)
    adx = _wilder_smooth(dx, period)

    trend_strength = "NO_TREND"
    adx_val = adx[-1]
    if not np.isnan(adx_val):
        if adx_val > 50:
            trend_strength = "VERY_STRONG"
        elif adx_val > 25:
            trend_strength = "STRONG"
        elif adx_val > 20:
            trend_strength = "MODERATE"
        else:
            trend_strength = "WEAK"

    direction = None
    if not np.isnan(plus_di[-1]) and not np.isnan(minus_di[-1]):
        direction = "BULLISH" if plus_di[-1] > minus_di[-1] else "BEARISH"

    return {
        "adx": _safe_round(adx_val),
        "plus_di": _safe_round(plus_di[-1]),
        "minus_di": _safe_round(minus_di[-1]),
        "trend_strength": trend_strength,
        "trend_direction": direction,
        "adx_series": _series_tail(adx),
    }


def compute_ichimoku(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray) -> dict:
    """Ichimoku Cloud (Tenkan, Kijun, Senkou A/B, Chikou)."""
    n = len(closes)
    if n < 52:
        return {"error": "insufficient_data", "min_required": 52}

    def donchian_mid(h, l, period, idx):
        start = max(0, idx - period + 1)
        return (np.max(h[start:idx + 1]) + np.min(l[start:idx + 1])) / 2

    # Tenkan-sen (9-period)
    tenkan = np.array([donchian_mid(highs, lows, 9, i) for i in range(n)])
    # Kijun-sen (26-period)
    kijun = np.array([donchian_mid(highs, lows, 26, i) for i in range(n)])
    # Senkou Span A (midpoint of tenkan & kijun, shifted 26 ahead — we compute current)
    senkou_a = (tenkan + kijun) / 2
    # Senkou Span B (52-period donchian mid)
    senkou_b = np.array([donchian_mid(highs, lows, 52, i) for i in range(n)])

    price = closes[-1]
    cloud_top = max(senkou_a[-1], senkou_b[-1])
    cloud_bottom = min(senkou_a[-1], senkou_b[-1])

    if price > cloud_top:
        position = "ABOVE_CLOUD"
    elif price < cloud_bottom:
        position = "BELOW_CLOUD"
    else:
        position = "INSIDE_CLOUD"

    # TK cross
    tk_cross = None
    if tenkan[-2] < kijun[-2] and tenkan[-1] > kijun[-1]:
        tk_cross = "BULLISH_TK_CROSS"
    elif tenkan[-2] > kijun[-2] and tenkan[-1] < kijun[-1]:
        tk_cross = "BEARISH_TK_CROSS"

    # Cloud color (future direction)
    cloud_color = "GREEN" if senkou_a[-1] > senkou_b[-1] else "RED"

    return {
        "tenkan_sen": _safe_round(tenkan[-1]),
        "kijun_sen": _safe_round(kijun[-1]),
        "senkou_span_a": _safe_round(senkou_a[-1]),
        "senkou_span_b": _safe_round(senkou_b[-1]),
        "cloud_top": _safe_round(cloud_top),
        "cloud_bottom": _safe_round(cloud_bottom),
        "cloud_thickness": _safe_round(cloud_top - cloud_bottom),
        "price_vs_cloud": position,
        "cloud_color": cloud_color,
        "tk_cross": tk_cross,
    }


def compute_supertrend(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray,
                        period: int = 10, multiplier: float = 3.0) -> dict:
    """Supertrend indicator."""
    n = len(closes)
    if n < period + 1:
        return {"error": "insufficient_data"}

    tr = _true_range(highs, lows, closes)
    atr = _wilder_smooth(tr, period)

    hl2 = (highs + lows) / 2
    upper_band = hl2 + multiplier * atr
    lower_band = hl2 - multiplier * atr

    supertrend = np.zeros(n)
    direction = np.zeros(n)  # 1 = up, -1 = down

    supertrend[0] = upper_band[0]
    direction[0] = 1

    for i in range(1, n):
        if np.isnan(upper_band[i]) or np.isnan(lower_band[i]):
            supertrend[i] = supertrend[i - 1]
            direction[i] = direction[i - 1]
            continue

        if closes[i] > supertrend[i - 1]:
            supertrend[i] = max(lower_band[i], supertrend[i - 1]) if direction[i - 1] == 1 else lower_band[i]
            direction[i] = 1
        else:
            supertrend[i] = min(upper_band[i], supertrend[i - 1]) if direction[i - 1] == -1 else upper_band[i]
            direction[i] = -1

    # Flip detection
    flip = None
    if direction[-2] == -1 and direction[-1] == 1:
        flip = "BULLISH_FLIP"
    elif direction[-2] == 1 and direction[-1] == -1:
        flip = "BEARISH_FLIP"

    return {
        "supertrend": _safe_round(supertrend[-1]),
        "direction": "UP" if direction[-1] == 1 else "DOWN",
        "flip": flip,
        "distance_pct": _safe_round((closes[-1] - supertrend[-1]) / supertrend[-1] * 100),
    }


# ═══════════════════════════════════════════════════
# MOMENTUM INDICATORS
# ═══════════════════════════════════════════════════

def compute_rsi(closes: np.ndarray, period: int = 14) -> dict:
    """RSI (Relative Strength Index)."""
    if len(closes) < period + 1:
        return {"error": "insufficient_data"}

    deltas = np.diff(closes)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)

    avg_gain = _wilder_smooth(gains, period)
    avg_loss = _wilder_smooth(losses, period)

    rs = avg_gain / (avg_loss + 1e-12)
    rsi = 100 - 100 / (1 + rs)

    rsi_val = rsi[-1]
    if np.isnan(rsi_val):
        rsi_val = 50.0

    zone = "NEUTRAL"
    if rsi_val > 70:
        zone = "OVERBOUGHT"
    elif rsi_val > 60:
        zone = "BULLISH"
    elif rsi_val < 30:
        zone = "OVERSOLD"
    elif rsi_val < 40:
        zone = "BEARISH"

    return {
        "rsi": _safe_round(rsi_val),
        "zone": zone,
        "rsi_series": _series_tail(rsi),
    }


def compute_stochastic(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray,
                        k_period: int = 14, d_period: int = 3) -> dict:
    """Stochastic Oscillator (%K, %D)."""
    n = len(closes)
    if n < k_period:
        return {"error": "insufficient_data"}

    k_values = np.full(n, np.nan)
    for i in range(k_period - 1, n):
        highest = np.max(highs[i - k_period + 1:i + 1])
        lowest = np.min(lows[i - k_period + 1:i + 1])
        denom = highest - lowest
        k_values[i] = ((closes[i] - lowest) / denom * 100) if denom > 0 else 50

    d_values = _sma(k_values, d_period)

    k_val = k_values[-1]
    d_val = d_values[-1]

    zone = "NEUTRAL"
    if k_val > 80:
        zone = "OVERBOUGHT"
    elif k_val < 20:
        zone = "OVERSOLD"

    crossover = None
    if not np.isnan(k_values[-2]) and not np.isnan(d_values[-2]):
        if k_values[-2] < d_values[-2] and k_values[-1] > d_values[-1]:
            crossover = "BULLISH_CROSS"
        elif k_values[-2] > d_values[-2] and k_values[-1] < d_values[-1]:
            crossover = "BEARISH_CROSS"

    return {
        "k": _safe_round(k_val),
        "d": _safe_round(d_val),
        "zone": zone,
        "crossover": crossover,
    }


def compute_cci(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 20) -> dict:
    """Commodity Channel Index."""
    n = len(closes)
    if n < period:
        return {"error": "insufficient_data"}

    tp = (highs + lows + closes) / 3
    sma_tp = _sma(tp, period)

    # Mean deviation
    md = np.full(n, np.nan)
    for i in range(period - 1, n):
        md[i] = np.mean(np.abs(tp[i - period + 1:i + 1] - sma_tp[i]))

    cci = (tp - sma_tp) / (0.015 * md + 1e-12)

    val = cci[-1]
    zone = "NEUTRAL"
    if not np.isnan(val):
        if val > 200:
            zone = "EXTREME_OVERBOUGHT"
        elif val > 100:
            zone = "OVERBOUGHT"
        elif val < -200:
            zone = "EXTREME_OVERSOLD"
        elif val < -100:
            zone = "OVERSOLD"

    return {"cci": _safe_round(val), "zone": zone, "cci_series": _series_tail(cci)}


def compute_williams_r(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 14) -> dict:
    """Williams %R."""
    n = len(closes)
    if n < period:
        return {"error": "insufficient_data"}

    wr = np.full(n, np.nan)
    for i in range(period - 1, n):
        highest = np.max(highs[i - period + 1:i + 1])
        lowest = np.min(lows[i - period + 1:i + 1])
        denom = highest - lowest
        wr[i] = ((highest - closes[i]) / denom * -100) if denom > 0 else -50

    val = wr[-1]
    zone = "NEUTRAL"
    if not np.isnan(val):
        if val > -20:
            zone = "OVERBOUGHT"
        elif val < -80:
            zone = "OVERSOLD"

    return {"williams_r": _safe_round(val), "zone": zone}


def compute_mfi(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray,
                volumes: np.ndarray, period: int = 14) -> dict:
    """Money Flow Index (volume-weighted RSI)."""
    n = len(closes)
    if n < period + 1:
        return {"error": "insufficient_data"}

    tp = (highs + lows + closes) / 3
    raw_money_flow = tp * volumes

    pos_flow = np.zeros(n - 1)
    neg_flow = np.zeros(n - 1)

    for i in range(n - 1):
        if tp[i + 1] > tp[i]:
            pos_flow[i] = raw_money_flow[i + 1]
        else:
            neg_flow[i] = raw_money_flow[i + 1]

    mfi_values = np.full(n - 1, np.nan)
    for i in range(period - 1, n - 1):
        pos_sum = np.sum(pos_flow[i - period + 1:i + 1])
        neg_sum = np.sum(neg_flow[i - period + 1:i + 1])
        ratio = pos_sum / (neg_sum + 1e-12)
        mfi_values[i] = 100 - 100 / (1 + ratio)

    val = mfi_values[-1]
    zone = "NEUTRAL"
    if not np.isnan(val):
        if val > 80:
            zone = "OVERBOUGHT"
        elif val < 20:
            zone = "OVERSOLD"

    return {"mfi": _safe_round(val), "zone": zone}


def compute_roc(closes: np.ndarray, period: int = 12) -> dict:
    """Rate of Change."""
    if len(closes) < period + 1:
        return {"error": "insufficient_data"}

    roc = (closes[period:] - closes[:-period]) / closes[:-period] * 100

    return {
        "roc": _safe_round(roc[-1]),
        "roc_series": _series_tail(roc),
    }


# ═══════════════════════════════════════════════════
# VOLATILITY INDICATORS
# ═══════════════════════════════════════════════════

def compute_bollinger(closes: np.ndarray, period: int = 20, std_dev: float = 2.0) -> dict:
    """Bollinger Bands."""
    if len(closes) < period:
        return {"error": "insufficient_data"}

    sma = _sma(closes, period)
    n = len(closes)
    std = np.full(n, np.nan)
    for i in range(period - 1, n):
        std[i] = np.std(closes[i - period + 1:i + 1], ddof=0)

    upper = sma + std_dev * std
    lower = sma - std_dev * std

    price = closes[-1]
    bb_width = (upper[-1] - lower[-1]) / sma[-1] if sma[-1] > 0 else 0
    pct_b = (price - lower[-1]) / (upper[-1] - lower[-1]) if (upper[-1] - lower[-1]) > 0 else 0.5

    position = "MIDDLE"
    if price > upper[-1]:
        position = "ABOVE_UPPER"
    elif price < lower[-1]:
        position = "BELOW_LOWER"
    elif pct_b > 0.8:
        position = "NEAR_UPPER"
    elif pct_b < 0.2:
        position = "NEAR_LOWER"

    # Squeeze detection (narrow bands)
    if len(std) > 50:
        avg_width = np.nanmean(std[-50:])
        squeeze = bool(std[-1] < avg_width * 0.5) if not np.isnan(std[-1]) else False
    else:
        squeeze = False

    return {
        "upper": _safe_round(upper[-1]),
        "middle": _safe_round(sma[-1]),
        "lower": _safe_round(lower[-1]),
        "bandwidth": _safe_round(bb_width),
        "percent_b": _safe_round(pct_b),
        "position": position,
        "squeeze": squeeze,
    }


def compute_atr(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 14) -> dict:
    """Average True Range."""
    if len(closes) < period + 1:
        return {"error": "insufficient_data"}

    tr = _true_range(highs, lows, closes)
    atr = _wilder_smooth(tr, period)

    atr_val = atr[-1]
    atr_pct = (atr_val / closes[-1] * 100) if closes[-1] > 0 else 0

    # Volatility regime
    if len(atr) > 50:
        avg_atr = np.nanmean(atr[-50:])
        ratio = atr_val / avg_atr if avg_atr > 0 else 1
        if ratio > 1.5:
            vol_regime = "HIGH"
        elif ratio < 0.7:
            vol_regime = "LOW"
        else:
            vol_regime = "NORMAL"
    else:
        vol_regime = "NORMAL"

    return {
        "atr": _safe_round(atr_val),
        "atr_pct": _safe_round(atr_pct),
        "volatility_regime": vol_regime,
        "atr_series": _series_tail(atr),
    }


def compute_keltner(closes: np.ndarray, highs: np.ndarray, lows: np.ndarray,
                    ema_period: int = 20, atr_period: int = 10, multiplier: float = 1.5) -> dict:
    """Keltner Channel."""
    if len(closes) < max(ema_period, atr_period) + 1:
        return {"error": "insufficient_data"}

    mid = _ema(closes, ema_period)
    tr = _true_range(highs, lows, closes)
    atr = _wilder_smooth(tr, atr_period)

    upper = mid + multiplier * atr
    lower = mid - multiplier * atr

    price = closes[-1]
    if price > upper[-1]:
        position = "ABOVE_UPPER"
    elif price < lower[-1]:
        position = "BELOW_LOWER"
    else:
        position = "INSIDE"

    return {
        "upper": _safe_round(upper[-1]),
        "middle": _safe_round(mid[-1]),
        "lower": _safe_round(lower[-1]),
        "position": position,
    }


# ═══════════════════════════════════════════════════
# VOLUME INDICATORS
# ═══════════════════════════════════════════════════

def compute_obv(closes: np.ndarray, volumes: np.ndarray) -> dict:
    """On-Balance Volume."""
    direction = np.sign(np.diff(closes))
    direction = np.concatenate([[0], direction])
    obv = np.cumsum(direction * volumes)

    # OBV trend (slope of last 20 points)
    lookback = min(20, len(obv) - 1)
    if lookback > 3:
        x = np.arange(lookback)
        slope = np.polyfit(x, obv[-lookback:], 1)[0]
        obv_trend = "RISING" if slope > 0 else "FALLING"
    else:
        obv_trend = "FLAT"

    return {
        "obv": _safe_round(obv[-1], 0),
        "obv_trend": obv_trend,
        "obv_series": _series_tail(obv, 20),
    }


def compute_vwap(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray,
                 volumes: np.ndarray) -> dict:
    """VWAP and standard deviation bands (intraday/session)."""
    tp = (highs + lows + closes) / 3
    cum_tp_vol = np.cumsum(tp * volumes)
    cum_vol = np.cumsum(volumes)
    vwap = cum_tp_vol / (cum_vol + 1e-12)

    # VWAP std bands
    cum_tp2_vol = np.cumsum(tp**2 * volumes)
    variance = cum_tp2_vol / (cum_vol + 1e-12) - vwap**2
    std = np.sqrt(np.maximum(variance, 0))

    price = closes[-1]
    vwap_val = vwap[-1]

    return {
        "vwap": _safe_round(vwap_val),
        "upper_1std": _safe_round(vwap_val + std[-1]),
        "lower_1std": _safe_round(vwap_val - std[-1]),
        "upper_2std": _safe_round(vwap_val + 2 * std[-1]),
        "lower_2std": _safe_round(vwap_val - 2 * std[-1]),
        "price_vs_vwap": "ABOVE" if price > vwap_val else "BELOW",
        "distance_pct": _safe_round((price - vwap_val) / vwap_val * 100),
    }


def compute_cmf(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray,
                volumes: np.ndarray, period: int = 20) -> dict:
    """Chaikin Money Flow."""
    n = len(closes)
    if n < period:
        return {"error": "insufficient_data"}

    clv = ((closes - lows) - (highs - closes)) / (highs - lows + 1e-12)
    mfv = clv * volumes

    cmf_values = np.full(n, np.nan)
    for i in range(period - 1, n):
        cmf_values[i] = np.sum(mfv[i - period + 1:i + 1]) / (np.sum(volumes[i - period + 1:i + 1]) + 1e-12)

    val = cmf_values[-1]
    signal = "NEUTRAL"
    if not np.isnan(val):
        if val > 0.1:
            signal = "STRONG_BUYING"
        elif val > 0:
            signal = "BUYING"
        elif val < -0.1:
            signal = "STRONG_SELLING"
        else:
            signal = "SELLING"

    return {"cmf": _safe_round(val), "signal": signal}


def compute_volume_profile(closes: np.ndarray, volumes: np.ndarray, bins: int = 20) -> dict:
    """Volume profile: POC, VAH, VAL."""
    if len(closes) < 10:
        return {"error": "insufficient_data"}

    price_min = np.min(closes)
    price_max = np.max(closes)
    bin_edges = np.linspace(price_min, price_max, bins + 1)
    vol_profile = np.zeros(bins)

    for i in range(len(closes)):
        idx = np.searchsorted(bin_edges[1:], closes[i])
        idx = min(idx, bins - 1)
        vol_profile[idx] += volumes[i]

    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2

    # POC (Point of Control) — price level with most volume
    poc_idx = np.argmax(vol_profile)
    poc_price = bin_centers[poc_idx]

    # Value Area (70% of volume)
    total_vol = np.sum(vol_profile)
    sorted_indices = np.argsort(vol_profile)[::-1]
    cum = 0
    va_indices = []
    for idx in sorted_indices:
        cum += vol_profile[idx]
        va_indices.append(idx)
        if cum >= total_vol * 0.7:
            break

    va_prices = bin_centers[va_indices]
    vah = float(np.max(va_prices))
    val_ = float(np.min(va_prices))

    price = closes[-1]
    if price > vah:
        position = "ABOVE_VALUE_AREA"
    elif price < val_:
        position = "BELOW_VALUE_AREA"
    else:
        position = "INSIDE_VALUE_AREA"

    return {
        "poc": _safe_round(poc_price),
        "value_area_high": _safe_round(vah),
        "value_area_low": _safe_round(val_),
        "price_position": position,
    }


# ═══════════════════════════════════════════════════
# SUPPORT / RESISTANCE
# ═══════════════════════════════════════════════════

def compute_pivot_points(high: float, low: float, close: float) -> dict:
    """Classic Pivot Points with support/resistance levels."""
    pp = (high + low + close) / 3
    r1 = 2 * pp - low
    s1 = 2 * pp - high
    r2 = pp + (high - low)
    s2 = pp - (high - low)
    r3 = high + 2 * (pp - low)
    s3 = low - 2 * (high - pp)

    return {
        "pivot": _safe_round(pp),
        "r1": _safe_round(r1), "r2": _safe_round(r2), "r3": _safe_round(r3),
        "s1": _safe_round(s1), "s2": _safe_round(s2), "s3": _safe_round(s3),
    }


def compute_fibonacci_levels(high: float, low: float) -> dict:
    """Fibonacci retracement and extension levels."""
    diff = high - low
    ratios = [0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0]
    extensions = [1.272, 1.618, 2.0, 2.618]

    retracements = {f"fib_{r}": _safe_round(high - diff * r) for r in ratios}
    exts = {f"ext_{e}": _safe_round(high - diff * (-e + 1)) for e in extensions}

    return {"retracements": retracements, "extensions": exts}


# ═══════════════════════════════════════════════════
# CANDLE PATTERNS
# ═══════════════════════════════════════════════════

def detect_candle_patterns(opens: np.ndarray, highs: np.ndarray,
                           lows: np.ndarray, closes: np.ndarray) -> list[dict]:
    """Detect common candlestick patterns in the last few candles."""
    patterns = []
    n = len(closes)
    if n < 3:
        return patterns

    # Helpers for the last 3 candles
    def body(i):
        return abs(closes[i] - opens[i])

    def upper_shadow(i):
        return highs[i] - max(opens[i], closes[i])

    def lower_shadow(i):
        return min(opens[i], closes[i]) - lows[i]

    def is_bullish(i):
        return closes[i] > opens[i]

    def full_range(i):
        return highs[i] - lows[i] + 1e-12

    i = n - 1  # Current candle

    # Doji
    if body(i) < full_range(i) * 0.1:
        patterns.append({"pattern": "DOJI", "candle": "current", "signal": "INDECISION"})

    # Hammer (bullish reversal)
    if lower_shadow(i) > body(i) * 2 and upper_shadow(i) < body(i) * 0.5:
        signal = "BULLISH_REVERSAL" if not is_bullish(i - 1) else "BULLISH"
        patterns.append({"pattern": "HAMMER", "candle": "current", "signal": signal})

    # Inverted Hammer
    if upper_shadow(i) > body(i) * 2 and lower_shadow(i) < body(i) * 0.5:
        patterns.append({"pattern": "INVERTED_HAMMER", "candle": "current", "signal": "POTENTIAL_REVERSAL"})

    # Shooting Star (bearish reversal at top)
    if upper_shadow(i) > body(i) * 2 and lower_shadow(i) < body(i) * 0.3 and is_bullish(i - 1):
        patterns.append({"pattern": "SHOOTING_STAR", "candle": "current", "signal": "BEARISH_REVERSAL"})

    # Bullish Engulfing
    if n >= 2:
        if not is_bullish(i - 1) and is_bullish(i) and opens[i] < closes[i - 1] and closes[i] > opens[i - 1]:
            patterns.append({"pattern": "BULLISH_ENGULFING", "candles": "last_2", "signal": "BULLISH_REVERSAL"})

    # Bearish Engulfing
    if n >= 2:
        if is_bullish(i - 1) and not is_bullish(i) and opens[i] > closes[i - 1] and closes[i] < opens[i - 1]:
            patterns.append({"pattern": "BEARISH_ENGULFING", "candles": "last_2", "signal": "BEARISH_REVERSAL"})

    # Morning Star (3-candle bullish reversal)
    if n >= 3:
        if (not is_bullish(i - 2) and body(i - 2) > full_range(i - 2) * 0.5
                and body(i - 1) < full_range(i - 1) * 0.3
                and is_bullish(i) and body(i) > full_range(i) * 0.5):
            patterns.append({"pattern": "MORNING_STAR", "candles": "last_3", "signal": "BULLISH_REVERSAL"})

    # Evening Star (3-candle bearish reversal)
    if n >= 3:
        if (is_bullish(i - 2) and body(i - 2) > full_range(i - 2) * 0.5
                and body(i - 1) < full_range(i - 1) * 0.3
                and not is_bullish(i) and body(i) > full_range(i) * 0.5):
            patterns.append({"pattern": "EVENING_STAR", "candles": "last_3", "signal": "BEARISH_REVERSAL"})

    # Three White Soldiers
    if n >= 3:
        if all(is_bullish(i - j) for j in range(3)) and closes[i] > closes[i - 1] > closes[i - 2]:
            patterns.append({"pattern": "THREE_WHITE_SOLDIERS", "candles": "last_3", "signal": "STRONG_BULLISH"})

    # Three Black Crows
    if n >= 3:
        if all(not is_bullish(i - j) for j in range(3)) and closes[i] < closes[i - 1] < closes[i - 2]:
            patterns.append({"pattern": "THREE_BLACK_CROWS", "candles": "last_3", "signal": "STRONG_BEARISH"})

    return patterns


# ═══════════════════════════════════════════════════
# FULL TECHNICAL ANALYSIS PIPELINE
# ═══════════════════════════════════════════════════

def full_technical_analysis(
    opens: np.ndarray,
    highs: np.ndarray,
    lows: np.ndarray,
    closes: np.ndarray,
    volumes: np.ndarray,
) -> dict:
    """
    Run ALL technical indicators on a single OHLCV dataset.
    Returns a comprehensive dict organized by category.
    """
    result = {
        "price_summary": {
            "current": _safe_round(closes[-1]),
            "open": _safe_round(opens[-1]),
            "high": _safe_round(highs[-1]),
            "low": _safe_round(lows[-1]),
            "change_pct": _safe_round((closes[-1] - closes[-2]) / closes[-2] * 100) if len(closes) > 1 else 0,
        },
    }

    # ── TREND ──
    try:
        result["moving_averages"] = compute_moving_averages(closes)
    except Exception as e:
        result["moving_averages"] = {"error": str(e)}

    try:
        result["macd"] = compute_macd(closes)
    except Exception as e:
        result["macd"] = {"error": str(e)}

    try:
        result["adx"] = compute_adx(highs, lows, closes)
    except Exception as e:
        result["adx"] = {"error": str(e)}

    try:
        result["ichimoku"] = compute_ichimoku(highs, lows, closes)
    except Exception as e:
        result["ichimoku"] = {"error": str(e)}

    try:
        result["supertrend"] = compute_supertrend(highs, lows, closes)
    except Exception as e:
        result["supertrend"] = {"error": str(e)}

    # ── MOMENTUM ──
    try:
        result["rsi"] = compute_rsi(closes)
    except Exception as e:
        result["rsi"] = {"error": str(e)}

    try:
        result["stochastic"] = compute_stochastic(highs, lows, closes)
    except Exception as e:
        result["stochastic"] = {"error": str(e)}

    try:
        result["cci"] = compute_cci(highs, lows, closes)
    except Exception as e:
        result["cci"] = {"error": str(e)}

    try:
        result["williams_r"] = compute_williams_r(highs, lows, closes)
    except Exception as e:
        result["williams_r"] = {"error": str(e)}

    try:
        result["mfi"] = compute_mfi(highs, lows, closes, volumes)
    except Exception as e:
        result["mfi"] = {"error": str(e)}

    try:
        result["roc"] = compute_roc(closes)
    except Exception as e:
        result["roc"] = {"error": str(e)}

    # ── VOLATILITY ──
    try:
        result["bollinger"] = compute_bollinger(closes)
    except Exception as e:
        result["bollinger"] = {"error": str(e)}

    try:
        result["atr"] = compute_atr(highs, lows, closes)
    except Exception as e:
        result["atr"] = {"error": str(e)}

    try:
        result["keltner"] = compute_keltner(closes, highs, lows)
    except Exception as e:
        result["keltner"] = {"error": str(e)}

    # ── VOLUME ──
    try:
        result["obv"] = compute_obv(closes, volumes)
    except Exception as e:
        result["obv"] = {"error": str(e)}

    try:
        result["vwap"] = compute_vwap(highs, lows, closes, volumes)
    except Exception as e:
        result["vwap"] = {"error": str(e)}

    try:
        result["cmf"] = compute_cmf(highs, lows, closes, volumes)
    except Exception as e:
        result["cmf"] = {"error": str(e)}

    try:
        result["volume_profile"] = compute_volume_profile(closes, volumes)
    except Exception as e:
        result["volume_profile"] = {"error": str(e)}

    # ── S/R ──
    try:
        # Use previous candle's HLC for pivot points
        result["pivot_points"] = compute_pivot_points(highs[-2], lows[-2], closes[-2])
    except Exception as e:
        result["pivot_points"] = {"error": str(e)}

    try:
        # Fibonacci on last N candles swing
        lookback = min(100, len(closes))
        swing_high = float(np.max(highs[-lookback:]))
        swing_low = float(np.min(lows[-lookback:]))
        result["fibonacci"] = compute_fibonacci_levels(swing_high, swing_low)
    except Exception as e:
        result["fibonacci"] = {"error": str(e)}

    # ── CANDLE PATTERNS ──
    try:
        result["candle_patterns"] = detect_candle_patterns(opens, highs, lows, closes)
    except Exception as e:
        result["candle_patterns"] = {"error": str(e)}

    # ── BIAS SUMMARY ──
    result["bias_summary"] = _compute_bias_summary(result)

    return result


def _compute_bias_summary(ta: dict) -> dict:
    """
    Aggregate all indicator signals into a directional bias score.
    Returns -1 (strong bearish) to +1 (strong bullish).
    """
    signals = []

    # RSI
    rsi = ta.get("rsi", {})
    if "rsi" in rsi and rsi["rsi"] is not None:
        v = rsi["rsi"]
        if v > 70:
            signals.append(("rsi", -0.5))   # overbought → mean revert
        elif v > 55:
            signals.append(("rsi", 0.3))
        elif v < 30:
            signals.append(("rsi", 0.5))    # oversold → bounce
        elif v < 45:
            signals.append(("rsi", -0.3))

    # MACD
    macd = ta.get("macd", {})
    if "histogram" in macd and macd["histogram"] is not None:
        h = macd["histogram"]
        if h > 0:
            signals.append(("macd", 0.5))
        else:
            signals.append(("macd", -0.5))
        if macd.get("crossover") == "BULLISH_CROSS":
            signals.append(("macd_cross", 0.7))
        elif macd.get("crossover") == "BEARISH_CROSS":
            signals.append(("macd_cross", -0.7))

    # ADX + DI
    adx = ta.get("adx", {})
    if adx.get("trend_direction") == "BULLISH" and (adx.get("adx") or 0) > 20:
        signals.append(("adx", 0.5))
    elif adx.get("trend_direction") == "BEARISH" and (adx.get("adx") or 0) > 20:
        signals.append(("adx", -0.5))

    # Supertrend
    st = ta.get("supertrend", {})
    if st.get("direction") == "UP":
        signals.append(("supertrend", 0.6))
    elif st.get("direction") == "DOWN":
        signals.append(("supertrend", -0.6))

    # Ichimoku
    ichi = ta.get("ichimoku", {})
    if ichi.get("price_vs_cloud") == "ABOVE_CLOUD":
        signals.append(("ichimoku", 0.6))
    elif ichi.get("price_vs_cloud") == "BELOW_CLOUD":
        signals.append(("ichimoku", -0.6))

    # Moving averages
    ma = ta.get("moving_averages", {})
    ema = ma.get("ema", {})
    for key in ["EMA_20", "EMA_50", "EMA_200"]:
        if key in ema:
            if ema[key].get("price_position") == "ABOVE":
                signals.append((key.lower(), 0.3))
            elif ema[key].get("price_position") == "BELOW":
                signals.append((key.lower(), -0.3))

    # Bollinger
    bb = ta.get("bollinger", {})
    if bb.get("position") == "ABOVE_UPPER":
        signals.append(("bb", -0.4))
    elif bb.get("position") == "BELOW_LOWER":
        signals.append(("bb", 0.4))

    # Volume (OBV + CMF)
    obv = ta.get("obv", {})
    if obv.get("obv_trend") == "RISING":
        signals.append(("obv", 0.3))
    elif obv.get("obv_trend") == "FALLING":
        signals.append(("obv", -0.3))

    cmf = ta.get("cmf", {})
    if cmf.get("signal") in ("STRONG_BUYING", "BUYING"):
        signals.append(("cmf", 0.3))
    elif cmf.get("signal") in ("STRONG_SELLING", "SELLING"):
        signals.append(("cmf", -0.3))

    # Stochastic
    stoch = ta.get("stochastic", {})
    if stoch.get("zone") == "OVERBOUGHT":
        signals.append(("stoch", -0.3))
    elif stoch.get("zone") == "OVERSOLD":
        signals.append(("stoch", 0.3))

    # MFI
    mfi = ta.get("mfi", {})
    if mfi.get("zone") == "OVERBOUGHT":
        signals.append(("mfi", -0.3))
    elif mfi.get("zone") == "OVERSOLD":
        signals.append(("mfi", 0.3))

    if signals:
        score = sum(s[1] for s in signals) / len(signals)
    else:
        score = 0

    bullish = sum(1 for _, v in signals if v > 0)
    bearish = sum(1 for _, v in signals if v < 0)

    if score > 0.3:
        bias = "STRONG_BULLISH"
    elif score > 0.1:
        bias = "BULLISH"
    elif score < -0.3:
        bias = "STRONG_BEARISH"
    elif score < -0.1:
        bias = "BEARISH"
    else:
        bias = "NEUTRAL"

    return {
        "bias": bias,
        "score": _safe_round(score),
        "bullish_signals": bullish,
        "bearish_signals": bearish,
        "total_signals": len(signals),
        "signal_details": [{"indicator": s[0], "value": _safe_round(s[1])} for s in signals],
    }
