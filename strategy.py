"""
Motor de la estrategia α_f Bifurcation Short — 4 variantes simultáneas.

Mantiene el estado de mercado compartido por símbolo y evalúa señales de
entrada/salida para cada variante (Conservative, Base, Aggressive, High-Energy).
Los trades virtuales se graban en SQLite via AsyncDBWriter.

Arquitectura:
  - SymbolState: estado de mercado en memoria con sliding windows (deque).
    Score Ŝ, energía E y exhaustion Ê se calculan una sola vez con los
    umbrales universales de Paper Table 1.
  - VariantTradeState: estado de trade por (símbolo, variante).
  - StrategyEngine: coordina todos los estados, evalúa entrada/salida
    para cada variante, delega persistencia al writer.
"""

import json
import logging
import os
import time
import asyncio
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

from config import (
    STRATEGY,
    STRATEGY_VERSION,
    VARIANTS,
    INITIAL_CAPITAL,
    TAKER_FEE,
    SYMBOLS,
    RECORDING,
    TRADING_MODE,
    ACTIVE_ACCOUNT,
    ACTIVE_VARIANT,
    BINANCE_ACCOUNTS,
    DATA_DIR,
    load_trading_config,
)
from adaptive_exit import AdaptiveExitCalibrator, TradeRecord
from shannon_exit import V5Exit

log = logging.getLogger("strategy")

# ══════════════════════════════════════════════════════════════════
#  Constantes de ventanas temporales (en segundos)
# ══════════════════════════════════════════════════════════════════
WINDOW_8H = 8 * 3600
WINDOW_12H = 12 * 3600
WINDOW_24H = 24 * 3600
WINDOW_48H = 48 * 3600

# Máximo de puntos en sliding windows (1 punto/s → 48h = 172800)
# Usamos 1 punto por segundo para mark price, pero trades se acumulan
MAX_MARK_POINTS = WINDOW_48H + 3600   # 49h de buffer
MAX_TRADE_POINTS = 500_000            # trades individuales (~48h para altcoins)
MAX_OI_POINTS = WINDOW_48H // 5 + 100  # OI cada 5s
MAX_CANDLE_POINTS = WINDOW_48H // 60 + 100  # 1-min candles sintéticas


@dataclass
class MiniCandle:
    """Vela de 1 minuto construida en streaming desde aggTrades."""
    ts: float           # timestamp de apertura (epoch s)
    open: float = 0.0
    high: float = 0.0
    low: float = float("inf")
    close: float = 0.0
    volume: float = 0.0
    buy_volume: float = 0.0
    sell_volume: float = 0.0
    trade_count: int = 0


@dataclass
class VariantTradeState:
    """Estado de trade por (símbolo, variante). Separado del estado de mercado."""
    open_trade_id: Optional[int] = None
    entry_price: float = 0.0
    entry_time: float = 0.0
    entry_oi: float = 0.0
    entry_notional: float = 0.0
    mfe: float = 0.0          # Maximum Favorable Excursion %
    mae: float = 0.0          # Maximum Adverse Excursion %
    funding_collected: float = 0.0
    last_funding_collection: float = 0.0
    last_trade_close_time: float = 0.0
    # Modo del trade ("paper" o "live") — se fija al abrir y NO cambia
    trade_mode: str = "paper"
    # Live trading attrs (None/0 en dry-run)
    binance_order_id: Optional[int] = None
    live_qty: float = 0.0
    # Real commission from Binance (0 for dry-run)
    entry_commission: float = 0.0
    # MFE timestamp tracking (AEPS)
    mfe_timestamp: float = 0.0  # epoch cuando se alcanzó MFE máximo
    # Partial TP tracking (MAX exit scheme)
    partial_tp_taken: bool = False
    original_qty: float = 0.0   # qty antes del cierre parcial
    partial_tp_pnl_usd: float = 0.0  # PnL realizado del TP parcial
    zombie_checked: bool = False  # True después de evaluar zombie_kill (una sola vez)
    mfe_2min_snapshot: Optional[float] = None  # MFE snapshot at 4min mark (base simplified exit)
    mfe_2min_snapshot_dirty: bool = False       # needs DB persist


class SymbolState:
    """
    Estado de mercado de un símbolo (compartido por todas las variantes).
    Todas las sliding windows son deques con maxlen fijo.
    Score, energía y exhaustion se calculan una sola vez con Table 1.
    """

    def __init__(self, symbol: str):
        self.symbol = symbol

        # ── Precio ──
        self.mark_price: float = 0.0
        self.index_price: float = 0.0
        self.premium: float = 0.0         # x = mark - index

        # ── Funding ──
        self.funding_rate: float = 0.0
        self.next_funding_ts: int = 0
        self.funding_interval_secs: float = WINDOW_8H  # default 8h, updated from /fapi/v1/fundingInfo

        # ── Open Interest ──
        self.oi_value: float = 0.0        # en USDT
        self.oi_contracts: float = 0.0
        self._oi_history: deque[tuple[float, float]] = deque(maxlen=MAX_OI_POINTS)
        # (timestamp, oi_value)

        # ── Trades / Volumen ──
        # Velas de 1 minuto sintéticas
        self._candles: deque[MiniCandle] = deque(maxlen=MAX_CANDLE_POINTS)
        self._current_candle: Optional[MiniCandle] = None
        self._current_candle_minute: int = 0  # minuto actual (epoch // 60)

        # ── Taker buy ratio (sliding window de 8h) ──
        # Acumuladores por minuto: (buy_vol, total_vol)
        self._taker_minutes: deque[tuple[float, float, float]] = deque(
            maxlen=WINDOW_8H // 60 * 2
        )  # (ts, buy_vol, total_vol)
        self._taker_buy_sum: float = 0.0   # running sum for O(1) ratio
        self._taker_vol_sum: float = 0.0

        # ── Mark price history (para SMA, price change) ──
        # Guardamos 1 punto por minuto para eficiencia
        self._price_minutes: deque[tuple[float, float]] = deque(
            maxlen=WINDOW_48H // 60 + 10
        )  # (ts, price)

        # ── Premium velocity ẋ (Paper §10.2) ──
        self._premium_ticks: deque[tuple[float, float]] = deque(maxlen=60)
        # (ts, premium) — 30-60s window at ~1/s for sliding OLS
        self.premium_velocity: float = 0.0        # ẋ estimate ($/s)
        self._prev_velocity_sign: int = 0          # for sign-change detection
        self.velocity_reversal: bool = False        # True when ẋ flipped + → −

        # ══════════════════════════════════════════════════════════
        #  Variables de la estrategia (compartidas entre variantes)
        # ══════════════════════════════════════════════════════════

        # Composite score Ŝ_αf
        self.c_fund: float = 0.0
        self.c_oi: float = 0.0
        self.c_price: float = 0.0
        self.c_taker: float = 0.0
        self.c_vol: float = 0.0
        self.score: float = 0.0

        # Energía acumulada E(t) en horas
        self.energy: float = 0.0
        self._last_energy_update: float = 0.0

        # Exhaustion Ê
        self.exhaustion: int = 0
        self.e_oi_declining: int = 0
        self.e_taker_flip: int = 0
        self.e_wick_reject: int = 0
        self.e_price_off_hi: int = 0
        self.e_vol_declining: int = 0

        # Métricas derivadas
        self.taker_buy_ratio: float = 0.5
        self.volume_ratio: float = 1.0     # V / V̄_48h
        self.price_change_12h: float = 0.0
        self.price_change_24h: float = 0.0  # para pump capture exit (Paper iv)
        self.price_max_24h: float = 0.0
        self.sma_24h: float = 0.0
        self.oi_change_24h: float = 0.0
        self.oi_change_1h: float = 0.0   # short-term dOI for reversal exit

        # ── ATR% continuo (AEPS) ──
        self.atr_pct: float = 0.0  # ATR como % del precio (rolling 14 candles de 1h)

        # ── Grabación condicional ──
        self.recording: bool = False       # ¿grabar datos pesados ahora?
        self._record_reason: str = ""      # razón para logging

        # ── Inicialización ──
        self._initialized: bool = False

    # ══════════════════════════════════════════════════════════════
    #  Actualización de datos de mercado
    # ══════════════════════════════════════════════════════════════

    def update_mark_price(self, mark: float, index: float,
                          funding: float, next_fund_ts: int, ts: float):
        """Llamado con cada mensaje @markPrice (cada 1s)."""
        self.mark_price = mark
        self.index_price = index
        self.premium = mark - index
        self.funding_rate = funding
        self.next_funding_ts = next_fund_ts

        # Premium velocity window (~1 tick per @markPrice update ≈ 1/s)
        self._premium_ticks.append((ts, self.premium))

        # Registrar precio por minuto
        minute = int(ts) // 60
        if not self._price_minutes or self._price_minutes[-1][0] // 60 != minute:
            self._price_minutes.append((ts, mark))

        if not self._initialized and mark > 0:
            self._initialized = True

    def update_trade(self, price: float, qty: float, is_buyer_maker: bool,
                     trade_time_ms: int):
        """Llamado con cada @aggTrade."""
        ts = trade_time_ms / 1000.0
        minute = int(ts) // 60

        # Construir vela de 1 minuto en streaming
        if self._current_candle is None or self._current_candle_minute != minute:
            # Cerrar vela anterior
            if self._current_candle is not None:
                self._candles.append(self._current_candle)
                # Registrar taker volumes del minuto + actualizar running sums
                c = self._current_candle
                self._taker_minutes.append((
                    c.ts,
                    c.buy_volume,
                    c.volume,
                ))
                self._taker_buy_sum += c.buy_volume
                self._taker_vol_sum += c.volume
            # Nueva vela
            self._current_candle = MiniCandle(ts=ts)
            self._current_candle_minute = minute
            self._current_candle.open = price

        c = self._current_candle
        c.high = max(c.high, price)
        if price < c.low:
            c.low = price
        c.close = price
        c.volume += qty * price  # volumen en USDT
        if not is_buyer_maker:
            c.buy_volume += qty * price  # taker buy
        else:
            c.sell_volume += qty * price
        c.trade_count += 1

    def update_oi(self, oi_contracts: float, oi_value: float, ts: float):
        """Llamado con cada poll de OI (cada ~5s)."""
        self.oi_contracts = oi_contracts
        self.oi_value = oi_value
        self._oi_history.append((ts, oi_value))

    # ══════════════════════════════════════════════════════════════
    #  Cálculos de la estrategia (llamados periódicamente)
    # ══════════════════════════════════════════════════════════════

    def compute_all(self, now: float):
        """
        Recalcula TODAS las métricas derivadas y señales.
        Llamar cada ~1-5 segundos desde el loop principal.
        """
        if not self._initialized or self.mark_price <= 0:
            return

        self._compute_price_metrics(now)
        self._compute_oi_metrics(now)
        self._compute_taker_ratio(now)
        self._compute_volume_ratio(now)
        self._compute_score()
        self._compute_energy(now)
        self._compute_exhaustion(now)
        self._compute_premium_velocity()
        self._compute_atr()

    def _compute_price_metrics(self, now: float):
        """ΔP_12h, ΔP_24h, P_max_24h, SMA_24h."""
        if len(self._price_minutes) < 2:
            return

        # Price change 12h
        cutoff_12h = now - WINDOW_12H
        cutoff_24h = now - WINDOW_24H

        prices_24h = []
        price_12h_ago = None
        for ts, p in self._price_minutes:
            if ts >= cutoff_24h:
                prices_24h.append(p)
            if price_12h_ago is None and ts >= cutoff_12h:
                price_12h_ago = p

        if price_12h_ago and price_12h_ago > 0:
            self.price_change_12h = (self.mark_price - price_12h_ago) / price_12h_ago
        else:
            self.price_change_12h = 0.0

        # Price change 24h (para pump capture exit, Paper condition iv)
        if prices_24h and prices_24h[0] > 0:
            self.price_change_24h = (self.mark_price - prices_24h[0]) / prices_24h[0]
        else:
            self.price_change_24h = 0.0

        if prices_24h:
            self.price_max_24h = max(prices_24h)
            self.sma_24h = sum(prices_24h) / len(prices_24h)
        else:
            self.price_max_24h = self.mark_price
            self.sma_24h = self.mark_price

    def _compute_oi_metrics(self, now: float):
        """ΔOI_24h and ΔOI_1h — O(1) via indexed deque access."""
        n = len(self._oi_history)
        if n < 2:
            return

        # Compute effective sample rate from deque timestamp span
        span = self._oi_history[-1][0] - self._oi_history[0][0]
        if span <= 0:
            return
        rate = (n - 1) / span  # samples per second

        # 24h lookback — O(1) index
        steps_24h = min(n - 1, int(rate * WINDOW_24H))
        oi_24h_val = self._oi_history[n - 1 - steps_24h][1]
        self.oi_change_24h = (
            (self.oi_value - oi_24h_val) / oi_24h_val
            if oi_24h_val > 0 else 0.0
        )

        # 1h lookback — O(1) index, used by reversal exit (Paper §2.4 exit v)
        steps_1h = min(n - 1, int(rate * 3600))
        oi_1h_val = self._oi_history[n - 1 - steps_1h][1]
        self.oi_change_1h = (
            (self.oi_value - oi_1h_val) / oi_1h_val
            if oi_1h_val > 0 else 0.0
        )

    def _compute_taker_ratio(self, now: float):
        """η_buy sobre ventana de 8h — O(1) amortized via running sums."""
        cutoff = now - WINDOW_8H
        # Trim aged-out entries (usually 0-1 per call → amortized O(1))
        while self._taker_minutes and self._taker_minutes[0][0] < cutoff:
            _, old_bv, old_tv = self._taker_minutes.popleft()
            self._taker_buy_sum -= old_bv
            self._taker_vol_sum -= old_tv
        # Guard against floating-point drift
        if self._taker_buy_sum < 0:
            self._taker_buy_sum = 0.0
        if self._taker_vol_sum < 0:
            self._taker_vol_sum = 0.0

        if self._taker_vol_sum > 0:
            self.taker_buy_ratio = self._taker_buy_sum / self._taker_vol_sum
        else:
            self.taker_buy_ratio = 0.5

    def _compute_volume_ratio(self, now: float):
        """V_reciente / V̄_48h.  Comparamos última hora vs promedio horario de 48h.
        Includes warmup normalization: projects partial first hour to full hour."""
        if len(self._candles) < 2:
            self.volume_ratio = 1.0
            return

        cutoff_1h = now - 3600
        cutoff_48h = now - WINDOW_48H

        vol_1h = 0.0
        vol_48h = 0.0
        minutes_48h = 0
        minutes_1h = 0

        for c in self._candles:
            if c.ts >= cutoff_48h:
                vol_48h += c.volume
                minutes_48h += 1
                if c.ts >= cutoff_1h:
                    vol_1h += c.volume
                    minutes_1h += 1

        # Agregar vela actual
        if self._current_candle:
            vol_48h += self._current_candle.volume
            minutes_48h += 1
            if self._current_candle.ts >= cutoff_1h:
                vol_1h += self._current_candle.volume
                minutes_1h += 1

        if minutes_48h > 60 and vol_48h > 0:
            avg_hourly = vol_48h / (minutes_48h / 60.0)
            if avg_hourly > 0:
                # Warmup: project partial first hour to full hour
                if 0 < minutes_1h < 60:
                    vol_1h = vol_1h * (60.0 / minutes_1h)
                self.volume_ratio = vol_1h / avg_hourly
            else:
                self.volume_ratio = 1.0
        else:
            self.volume_ratio = 1.0

    def _compute_score(self):
        """Composite score Ŝ_αf = Σ c_k (Paper Table 1, universal)."""
        p = STRATEGY

        # c1: Funding rate (solo positivo = longs pagan shorts = spring invertido)
        r = self.funding_rate
        if r >= p["funding_rate_full"]:
            self.c_fund = 1.0
        elif r >= p["funding_rate_half"]:
            self.c_fund = 0.5
        else:
            self.c_fund = 0.0

        # c2: OI growth 24h
        delta_oi = self.oi_change_24h
        if delta_oi >= p["oi_growth_24h_full"]:
            self.c_oi = 1.0
        elif delta_oi >= p["oi_growth_24h_half"]:
            self.c_oi = 0.5
        else:
            self.c_oi = 0.0

        # c3: Price pump 12h
        dp = self.price_change_12h
        if dp >= p["price_pump_12h_full"]:
            self.c_price = 1.0
        elif dp >= p["price_pump_12h_half"]:
            self.c_price = 0.5
        else:
            self.c_price = 0.0

        # c4: Taker buy ratio (8h)
        eta = self.taker_buy_ratio
        if eta >= p["taker_buy_ratio_full"]:
            self.c_taker = 1.0
        elif eta >= p["taker_buy_ratio_half"]:
            self.c_taker = 0.5
        else:
            self.c_taker = 0.0

        # c5: Volume spike
        vr = self.volume_ratio
        if vr >= p["volume_spike_full"]:
            self.c_vol = 1.0
        elif vr >= p["volume_spike_half"]:
            self.c_vol = 0.5
        else:
            self.c_vol = 0.0

        self.score = self.c_fund + self.c_oi + self.c_price + self.c_taker + self.c_vol

    def _compute_energy(self, now: float):
        """
        Función de acumulación de energía E(t) con histéresis.
        Acumula a rate 1 cuando Ŝ ≥ 3, decae a rate 1/2 si no.
        """
        if self._last_energy_update <= 0:
            self._last_energy_update = now
            return

        dt_hours = (now - self._last_energy_update) / 3600.0
        self._last_energy_update = now

        if dt_hours <= 0:
            return
        if dt_hours > 1:  # sanity: clamp dt a 1h máx tras reconexión
            dt_hours = 1.0

        if self.score >= STRATEGY["score_threshold"]:
            self.energy += dt_hours
        else:
            self.energy = max(0.0, self.energy - dt_hours / 2.0)

    def _compute_exhaustion(self, now: float):
        """
        Exhaustion composite Ê(t) = Σ indicadores de bifurcación reversal.
        """
        p = STRATEGY

        # e1: ΔOI < 0 (OI declining) — reuses oi_change_1h (already O(1))
        self.e_oi_declining = 1 if self.oi_change_1h < 0 else 0

        # e2: η_buy < 45% (taker buy ratio drops)
        self.e_taker_flip = 1 if self.taker_buy_ratio < 0.45 else 0

        # e3: Upper wick rejection (w_up > 2|body|)
        self.e_wick_reject = 0
        if len(self._candles) >= 3:
            # Revisar últimas 3 velas
            for c in list(self._candles)[-3:]:
                body = abs(c.close - c.open)
                upper_wick = c.high - max(c.open, c.close)
                if body > 0 and upper_wick > 2 * body:
                    self.e_wick_reject = 1
                    break

        # e4: P < 0.98 * P_max_24h (price off recent highs)
        if self.price_max_24h > 0:
            self.e_price_off_hi = 1 if self.mark_price < 0.98 * self.price_max_24h else 0
        else:
            self.e_price_off_hi = 0

        # e5: Volume declining post-spike (V̇/V < 0)
        self.e_vol_declining = 0
        if len(self._candles) >= 6:
            recent_3 = sum(c.volume for c in list(self._candles)[-3:])
            prev_3 = sum(c.volume for c in list(self._candles)[-6:-3])
            if prev_3 > 0 and recent_3 < prev_3:
                self.e_vol_declining = 1

        self.exhaustion = (
            self.e_oi_declining
            + self.e_taker_flip
            + self.e_wick_reject
            + self.e_price_off_hi
            + self.e_vol_declining
        )

    def _compute_premium_velocity(self):
        """
        Premium velocity ẋ via sliding OLS regression (Paper §10.2).
        Fits x(τ) = a + bτ over the last ~30s; slope b estimates ẋ.
        Detects sign reversal ẋ > 0 → ẋ < 0 (cascade onset for shorts).
        """
        n = len(self._premium_ticks)
        if n < 10:  # need at least 10s of data
            return

        # Use last 30 points (≈30s at ~1 tick/s)
        start = max(0, n - 30)
        t0 = self._premium_ticks[start][0]

        # OLS: slope = (n·Σ(tp) − Σt·Σp) / (n·Σ(t²) − (Σt)²)
        sum_t = sum_p = sum_tp = sum_t2 = 0.0
        nw = 0
        for i in range(start, n):
            ts, p = self._premium_ticks[i]
            t = ts - t0
            sum_t += t
            sum_p += p
            sum_tp += t * p
            sum_t2 += t * t
            nw += 1

        denom = nw * sum_t2 - sum_t * sum_t
        if abs(denom) < 1e-30:
            return

        slope = (nw * sum_tp - sum_t * sum_p) / denom
        new_sign = 1 if slope >= 0 else -1

        # Detect + → − reversal (premium was growing, now contracting)
        self.velocity_reversal = (self._prev_velocity_sign > 0 and new_sign < 0)
        self._prev_velocity_sign = new_sign
        self.premium_velocity = slope

    def _compute_atr(self):
        """ATR% sobre últimas 14 velas de 1 hora (rolling, sin I/O)."""
        # Necesitamos al menos 14h de velas de 1min (14*60 = 840)
        if len(self._candles) < 840:
            return

        # Agrupar velas de 1min en velas de 1h usando los últimos 840+ minutos
        candles_list = list(self._candles)
        # Solo necesitamos las últimas 15h de 1min candles (900 para 14 hourly + 1 prev close)
        recent = candles_list[-900:] if len(candles_list) > 900 else candles_list

        hourly = []
        bucket = []
        for c in recent:
            bucket.append(c)
            if len(bucket) >= 60:
                h = max(cc.high for cc in bucket)
                lo = min(cc.low for cc in bucket)
                cl = bucket[-1].close
                prev_cl = hourly[-1][2] if hourly else bucket[0].open
                tr = max(h - lo, abs(h - prev_cl), abs(lo - prev_cl))
                hourly.append((h, lo, cl, tr))
                bucket = []

        if len(hourly) < 14:
            return

        # ATR = SMA de los últimos 14 true ranges
        recent_trs = [h[3] for h in hourly[-14:]]
        atr = sum(recent_trs) / len(recent_trs)

        if self.mark_price > 0:
            self.atr_pct = atr / self.mark_price

    # ══════════════════════════════════════════════════════════════
    #  Snapshot (para grabar en DB)
    # ══════════════════════════════════════════════════════════════

    def to_snapshot(self, now: float) -> dict:
        return {
            "symbol": self.symbol,
            "timestamp": now,
            "score_total": self.score,
            "c_fund": self.c_fund,
            "c_oi": self.c_oi,
            "c_price": self.c_price,
            "c_taker": self.c_taker,
            "c_vol": self.c_vol,
            "energy_hours": self.energy,
            "exhaustion": self.exhaustion,
            "e_oi_declining": self.e_oi_declining,
            "e_taker_flip": self.e_taker_flip,
            "e_wick_reject": self.e_wick_reject,
            "e_price_off_hi": self.e_price_off_hi,
            "e_vol_declining": self.e_vol_declining,
            "mark_price": self.mark_price,
            "funding_rate": self.funding_rate,
            "oi_value": self.oi_value,
            "taker_buy_ratio": self.taker_buy_ratio,
            "volume_ratio": self.volume_ratio,
            "price_change_12h": self.price_change_12h,
            "price_change_24h": self.price_change_24h,
            "sma_24h": self.sma_24h,
            "premium_velocity": self.premium_velocity,
        }


# ══════════════════════════════════════════════════════════════════
#  STRATEGY ENGINE — 4 variantes simultáneas
# ══════════════════════════════════════════════════════════════════


class StrategyEngine:
    """
    Coordina todos los SymbolState y evalúa señales de entrada/salida
    para las 4 variantes simultáneamente. El estado de mercado (score,
    energía, exhaustion) se computa una sola vez por símbolo; cada
    variante usa sus propios umbrales de entrada/salida (Paper Table 5).
    """

    def __init__(self, writer, trader=None, telegram=None):
        """
        Args:
            writer: instancia de AsyncDBWriter (ya conectada).
            trader: instancia de BinanceTrader (solo en modo live, None en dry-run).
            telegram: instancia de TelegramNotifier (opcional).
        """
        self.writer = writer
        self.telegram = telegram  # TelegramNotifier o None
        self.trader = trader  # None en dry-run, BinanceTrader en live
        self.trading_mode = TRADING_MODE  # "dry-run" | "live"
        self.active_account = ACTIVE_ACCOUNT
        self.active_variant = ACTIVE_VARIANT  # variante que opera en live
        self._last_config_check = 0.0  # epoch del último chequeo
        self.states: dict[str, SymbolState] = {}

        # Per-variant state
        self.variant_trades: dict[str, dict[str, VariantTradeState]] = {}
        # ── Equity, PnL y halt SEPARADOS por modo (paper/live) ──
        self.equities: dict[str, dict[str, float]] = {"paper": {}, "live": {}}
        self.daily_pnl: dict[str, dict[str, float]] = {"paper": {}, "live": {}}
        self.halted: dict[str, dict[str, bool]] = {"paper": {}, "live": {}}
        self.daily_reset_day: int = 0

        # Inicializar estados por símbolo
        for sym in SYMBOLS:
            self.states[sym.upper()] = SymbolState(sym.upper())

        # Inicializar per-variant × per-mode
        for vname in VARIANTS:
            self.variant_trades[vname] = {}
            for mode in ("paper", "live"):
                self.equities[mode][vname] = INITIAL_CAPITAL
                self.daily_pnl[mode][vname] = 0.0
                self.halted[mode][vname] = False
            for sym in SYMBOLS:
                self.variant_trades[vname][sym.upper()] = VariantTradeState()

        vnames = ", ".join(VARIANTS.keys())
        log.info(
            f"StrategyEngine inicializado: {len(self.states)} símbolos, "
            f"{len(VARIANTS)} variantes [{vnames}], "
            f"capital=${INITIAL_CAPITAL:,.0f}/variante"
        )

        # AEPS: un calibrador por variante (aggressive usa v5, no AEPS)
        self.exit_calibrators: dict[str, AdaptiveExitCalibrator] = {}
        for vname, vparams in VARIANTS.items():
            if vname == "aggressive":
                continue
            self.exit_calibrators[vname] = AdaptiveExitCalibrator(vparams)

        # v5 exit para aggressive
        surface_path = os.path.join(os.path.dirname(__file__), "data", "pwin_surface.json")
        v5_config = {
            "prior_win": 0.566,
            "exit_pw": 0.50,
            "trail_pw": 0.70,
            "info_rate_eps": 0.005,
            "min_observe_sec": 60,
            "hard_sl": 0.08,
            "trailing_callback": 0.30,
            "output_dir": os.path.join(os.path.dirname(__file__), "logs", "v5"),
        }
        self.v5_exit = V5Exit(surface_path, v5_config)

    def get_state(self, symbol: str) -> SymbolState:
        sym = symbol.upper()
        if sym not in self.states:
            self.states[sym] = SymbolState(sym)
            for vname in VARIANTS:
                if sym not in self.variant_trades[vname]:
                    self.variant_trades[vname][sym] = VariantTradeState()
        return self.states[sym]

    # ══════════════════════════════════════════════════════════════
    #  MFE/MAE tracking en tiempo real (cada tick de @markPrice)
    # ══════════════════════════════════════════════════════════════

    def update_trade_mfe(self, symbol: str, mark_price: float):
        """
        Actualiza MFE/MAE de todos los trades abiertos en este símbolo.
        Debe llamarse con cada tick de @markPrice (~1/s) para que
        el trailing stop se active con la excursión real, no muestreada.
        """
        sym = symbol.upper()
        for vname in VARIANTS:
            vtrade = self.variant_trades[vname].get(sym)
            if vtrade is None or vtrade.open_trade_id is None:
                continue
            if vtrade.entry_price <= 0:
                continue
            # SHORT PnL: (entry - current) / entry
            pnl_pct = (vtrade.entry_price - mark_price) / vtrade.entry_price
            if pnl_pct > vtrade.mfe:
                vtrade.mfe = pnl_pct
                vtrade.mfe_timestamp = time.time()
            if pnl_pct < vtrade.mae:
                vtrade.mae = pnl_pct

    # ══════════════════════════════════════════════════════════════
    #  Evaluación periódica (cada ~5s desde el loop principal)
    # ══════════════════════════════════════════════════════════════

    async def _check_config_reload(self, now: float):
        """
        Cada ~10s lee trading_config.json y si el modo o cuenta cambiaron,
        crea/destruye el BinanceTrader.
        """
        CHECK_INTERVAL = 10  # seconds
        if now - self._last_config_check < CHECK_INTERVAL:
            return
        self._last_config_check = now

        try:
            tc = load_trading_config()
        except Exception:
            return

        new_mode = tc.get("trading_mode", "dry-run")
        new_account = tc.get("active_account", "principal")
        new_variant = tc.get("active_variant", "base")

        if (new_mode == self.trading_mode
                and new_account == self.active_account
                and new_variant == self.active_variant):
            return  # sin cambios

        log.info(
            f"⚙️  Config change detectado: "
            f"{self.trading_mode}/{self.active_account}/{self.active_variant} "
            f"→ {new_mode}/{new_account}/{new_variant}"
        )

        # ── Cerrar trader previo si existe ──
        if self.trader is not None:
            try:
                await self.trader.close()
            except Exception:
                pass
            self.trader = None
            log.info("🔌 Trader anterior desconectado")

        # ── Crear nuevo trader si modo live ──
        if new_mode == "live":
            acct = BINANCE_ACCOUNTS.get(new_account)
            if acct and acct["api_key"] and acct["api_secret"]:
                try:
                    from binance_trader import BinanceTrader
                    trader = BinanceTrader(
                        api_key=acct["api_key"],
                        api_secret=acct["api_secret"],
                        account_name=new_account,
                    )
                    await trader.connect()
                    bal = await trader.get_account_balance()
                    live_bal = bal.get("available", 0.0)
                    log.info(
                        f"✅ Trader LIVE conectado a [{new_account}] — "
                        f"balance=${bal.get('balance', 0):.2f} "
                        f"available=${live_bal:.2f}"
                    )
                    self.trader = trader
                    # Inicializar equity live con balance real de Binance
                    if live_bal > 0:
                        for vn in VARIANTS:
                            self.equities["live"][vn] = live_bal
                except Exception as e:
                    log.error(f"❌ No se pudo conectar trader live: {e}")
                    new_mode = "dry-run"  # fallback a dry-run
            else:
                log.error(f"❌ Cuenta '{new_account}' sin API keys — fallback dry-run")
                new_mode = "dry-run"
        else:
            log.info("📋 Modo dry-run activado — sin trader live")

        if self.telegram:
            try:
                await self.telegram.notify_config_change(
                    self.trading_mode, new_mode,
                    self.active_account, new_account,
                    self.active_variant, new_variant,
                )
            except Exception:
                pass

        self.trading_mode = new_mode
        self.active_account = new_account
        self.active_variant = new_variant

    async def evaluate_all(self, now: float):
        """
        Evalúa entrada/salida para todos los símbolos × todas las variantes.
        El mercado se computa una sola vez por símbolo.
        """
        t0 = time.time()

        # Hot-reload de trading_config.json
        await self._check_config_reload(now)

        # Reset diario del circuit breaker
        current_day = int(now) // 86400
        if current_day != self.daily_reset_day:
            self.daily_reset_day = current_day
            for mode in ("paper", "live"):
                for vname in VARIANTS:
                    self.daily_pnl[mode][vname] = 0.0
                    if self.halted[mode][vname]:
                        self.halted[mode][vname] = False
                        log.info(f"🔄 [{vname}][{mode.upper()}] Daily halt reseteado.")

        for symbol, state in self.states.items():
            if not state._initialized:
                continue

            # Recalcular métricas UNA SOLA VEZ (compartido)
            state.compute_all(now)

            # Evaluar cada variante
            for vname, vparams in VARIANTS.items():
                vtrade = self.variant_trades[vname].get(symbol)
                if vtrade is None:
                    continue

                if vtrade.open_trade_id is not None:
                    # Exit: usar el modo con que se abrió el trade
                    if self.halted[vtrade.trade_mode][vname]:
                        continue
                    await self._evaluate_exit(state, vtrade, vname, vparams, now)
                else:
                    # Entry: determinar el modo que tendría el nuevo trade
                    entry_mode = ("live" if (self.trading_mode == "live"
                                             and self.trader is not None
                                             and vname == self.active_variant)
                                  else "paper")
                    if self.halted[entry_mode][vname]:
                        continue
                    await self._evaluate_entry(state, vtrade, vname, vparams, now)

        elapsed = time.time() - t0
        if elapsed > 30:
            log.warning(f"evaluate_all took {elapsed:.1f}s (>30s)")

    # ══════════════════════════════════════════════════════════════
    #  ENTRADA
    # ══════════════════════════════════════════════════════════════

    async def _evaluate_entry(self, state: SymbolState,
                              vtrade: VariantTradeState,
                              vname: str, vparams: dict, now: float):
        """Evalúa condiciones de entrada SHORT para una variante específica."""

        # Aggressive mirrors base entries (v5)
        if vname == "aggressive":
            return

        # ── Cooldown ──
        cooldown_secs = vparams["cooldown_hours"] * 3600
        if now - vtrade.last_trade_close_time < cooldown_secs:
            return

        # ── Energía suficiente (compartida, umbral por variante) ──
        if state.energy < vparams["energy_min_hours"]:
            return

        # ── Score residual (compartido) ──
        if state.score < vparams["entry_score_min"]:
            return

        # ── Exhaustion mínimo (compartido) ──
        if state.exhaustion < vparams["entry_exhaustion_min"]:
            return

        # ── v2: Funding score hard filter ──
        # Paper Eq.5: F_fund = -αf·OI·r. If r≈0, no spring tension → no cascade.
        funding_score_min = vparams.get("entry_funding_score_min", 0)
        if funding_score_min > 0 and state.c_fund < funding_score_min:
            return

        # ── Premium velocity gate (Paper §10.2): ẋ ≤ 0 → cascade initiating ──
        if len(state._premium_ticks) >= 20 and state.premium_velocity > 0:
            return

        # ── Condiciones de entrada por variante (Paper Table 5) ──
        if state.funding_rate < vparams["entry_funding_min"]:
            return

        if state.oi_change_24h < vparams["entry_oi_growth_min"]:
            return

        if state.price_change_12h < vparams["entry_price_pump_min"]:
            return

        if state.volume_ratio < vparams["entry_vol_ratio_min"]:
            return

        # ── Precio > SMA_24h (no shortear en dip) ──
        if state.sma_24h > 0 and state.mark_price <= state.sma_24h:
            return

        # ── Filtro Régimen A (Paper Eq. 18, compartido) ──
        p = STRATEGY
        if (state.oi_change_24h > p["regime_a_oi_growth"]
                and state.taker_buy_ratio > p["regime_a_taker_buy"]
                and state.volume_ratio > p["regime_a_vol_spike"]):
            log.info(
                f"⚠️  [{vname}] {state.symbol} Régimen A detectado — "
                f"entrada suprimida (ΔOI={state.oi_change_24h:.1%}, "
                f"η={state.taker_buy_ratio:.1%}, V/V̄={state.volume_ratio:.1f}x)"
            )
            return

        # ── v2: Concurrent entry throttle ──
        # If too many entries in a short window, it's likely a correlated
        # market-wide event where F_ext dominates (Paper §8.5 point 4).
        max_concurrent = STRATEGY.get("concurrent_entry_max", 0)
        if max_concurrent > 0:
            window_h = STRATEGY.get("concurrent_entry_window_h", 2)
            window_secs = window_h * 3600
            # Count unique symbols with recent entries across ALL variants
            # (not just current) — Paper §8.5: correlation is market-wide
            recent_symbols = set()
            for _vn in self.variant_trades:
                for sym, vt in self.variant_trades[_vn].items():
                    if (vt.open_trade_id is not None
                            and sym != state.symbol
                            and (now - vt.entry_time) < window_secs):
                        recent_symbols.add(sym)
            recent_entries = len(recent_symbols)
            if recent_entries >= max_concurrent:
                log.info(
                    f"⏸️  [{vname}] {state.symbol} throttled — "
                    f"{recent_entries} concurrent entries in {window_h}h window"
                )
                return

        # ══ TODAS LAS CONDICIONES CUMPLIDAS → ABRIR SHORT ══
        await self._open_trade(state, vtrade, vname, vparams, now)

    async def _open_trade(self, state: SymbolState,
                          vtrade: VariantTradeState,
                          vname: str, vparams: dict, now: float):
        """Abre un trade SHORT para una variante (dry-run o live)."""
        price = state.mark_price

        # Determinar modo del trade ANTES de sizing (se fija al abrir)
        will_be_live = (self.trading_mode == "live" and self.trader is not None
                        and vname == self.active_variant)
        trade_mode = "live" if will_be_live else "paper"

        # Position sizing: f * K * L (Paper Eq. 13)
        # Live: usa balance real de Binance; Paper: usa equity simulado
        cap_frac = vparams["capital_fraction"]
        cap_frac_high = vparams.get("capital_fraction_high", 0)
        if cap_frac_high > 0 and state.score >= 4.5:
            cap_frac = cap_frac_high

        if will_be_live:
            try:
                bal = await self.trader.get_account_balance()
                live_equity = bal.get("available", 0.0)
                if live_equity <= 0:
                    log.warning(f"⚠️ [{vname}] Binance available balance = ${live_equity:.2f}, skip entry")
                    return
                self.equities["live"][vname] = live_equity
            except Exception as e:
                log.error(f"❌ [{vname}] Failed to fetch Binance balance: {e}")
                return
            notional = cap_frac * live_equity * vparams["leverage"]
        else:
            notional = cap_frac * self.equities[trade_mode][vname] * vparams["leverage"]

        # ── LIVE MODE: ejecutar orden real en Binance ──
        binance_order_id = None
        actual_entry_price = price
        actual_qty = 0.0
        entry_commission = 0.0

        if will_be_live:
            try:
                qty = await self.trader.calc_quantity(
                    state.symbol, notional, price
                )
                # Calcular precios TP/SL (usar AEPS si disponible)
                calibrator = self.exit_calibrators.get(vname)
                if calibrator:
                    ap = calibrator.get_params(current_atr_pct=state.atr_pct)
                    sl_pct = ap.stop_loss_pct
                else:
                    sl_pct = vparams["stop_loss_pct"]
                tp_pct = vparams["take_profit_pct"]
                sl_price = price * (1 + sl_pct)   # SHORT: SL arriba
                tp_price = price * (1 - tp_pct)   # SHORT: TP abajo

                result = await self.trader.open_short(
                    symbol=state.symbol,
                    quantity=qty,
                    leverage=vparams["leverage"],
                    take_profit=tp_price,
                    stop_loss=sl_price,
                )
                binance_order_id = result.get("orderId")
                actual_qty = float(result.get("executedQty", qty))
                entry_commission = float(result.get("totalCommission", 0))
                avg_px = result.get("avgPrice", 0)
                if avg_px and float(avg_px) > 0:
                    actual_entry_price = float(avg_px)

                log.warning(
                    f"💰 [{vname.upper()}] LIVE SHORT FILLED "
                    f"| {state.symbol} | orderId={binance_order_id} "
                    f"| qty={actual_qty} @ ${actual_entry_price:.6f} "
                    f"| SL=${sl_price:.6f} TP=${tp_price:.6f}"
                )
            except Exception as e:
                log.error(
                    f"❌ [{vname.upper()}] LIVE SHORT FAILED | "
                    f"{state.symbol} | {e}"
                )
                return  # No abrir trade virtual si la orden real falla

        snapshot = state.to_snapshot(now)

        # Confirmar modo live solo si Binance llenó la orden
        if will_be_live and binance_order_id is None:
            trade_mode = "paper"  # fallback: Binance no respondió orderId

        trade_data = {
            "symbol": state.symbol,
            "variant": vname,
            "entry_time": now,
            "entry_price": actual_entry_price,
            "entry_score": state.score,
            "entry_energy": state.energy,
            "entry_exhaustion": state.exhaustion,
            "leverage": vparams["leverage"],
            "position_size": notional,
            "entry_snapshot": snapshot,
            "trading_mode": trade_mode,
        }
        if vname == "aggressive":
            trade_data["strategy_version"] = "v5"

        trade_id = await self.writer.open_virtual_trade(trade_data)

        vtrade.open_trade_id = trade_id
        vtrade.entry_price = actual_entry_price
        vtrade.entry_time = now
        vtrade.entry_oi = state.oi_value
        vtrade.entry_notional = notional
        vtrade.mfe = 0.0
        vtrade.mae = 0.0
        vtrade.funding_collected = 0.0
        vtrade.last_funding_collection = now
        vtrade.trade_mode = trade_mode
        vtrade.binance_order_id = binance_order_id
        vtrade.live_qty = actual_qty
        vtrade.entry_commission = entry_commission if will_be_live else 0.0
        vtrade.partial_tp_taken = False
        vtrade.original_qty = actual_qty
        vtrade.zombie_checked = False
        vtrade.mfe_2min_snapshot = None

        # v5: register trade opening for aggressive variant
        if vname == "aggressive":
            self.v5_exit.open(
                str(trade_id), state.symbol, now, actual_entry_price
            )

        mode_tag = trade_mode.upper()
        log.warning(
            f"🔴 [{vname.upper()}][{mode_tag}] SHORT #{trade_id} | {state.symbol} "
            f"@ ${actual_entry_price:.6f} | Ŝ={state.score:.1f} E={state.energy:.1f}h "
            f"Ê={state.exhaustion} | ΔP12h={state.price_change_12h:.1%} "
            f"V/V̄={state.volume_ratio:.1f}x | nocional=${notional:,.0f} "
            f"({vparams['leverage']}x)"
        )

        # Notificación Telegram
        if self.telegram:
            try:
                await self.telegram.notify_trade_open(
                    trade_id=trade_id, symbol=state.symbol, variant=vname,
                    entry_price=actual_entry_price, notional=notional,
                    leverage=vparams["leverage"], score=state.score,
                    energy=state.energy, exhaustion=state.exhaustion,
                    price_change_12h=state.price_change_12h,
                    volume_ratio=state.volume_ratio, mode=trade_mode.upper(),
                )
            except Exception:
                pass

        # ── Aggressive mirrors base: open aggressive when base opens ──
        if vname == "base":
            agg_vtrade = self.variant_trades["aggressive"].get(state.symbol)
            agg_params = VARIANTS["aggressive"]
            if agg_vtrade is not None and agg_vtrade.open_trade_id is None:
                cooldown = agg_params["cooldown_hours"] * 3600
                if now - agg_vtrade.last_trade_close_time >= cooldown:
                    entry_mode = ("live" if (self.trading_mode == "live"
                                             and self.trader is not None
                                             and "aggressive" == self.active_variant)
                                  else "paper")
                    if not self.halted[entry_mode]["aggressive"]:
                        await self._open_trade(
                            state, agg_vtrade, "aggressive", agg_params, now
                        )

        # Grabar snapshot de entrada
        await self.writer.insert_snapshot(snapshot)

    # ══════════════════════════════════════════════════════════════
    #  SALIDA
    # ══════════════════════════════════════════════════════════════

    async def _evaluate_exit(self, state: SymbolState,
                             vtrade: VariantTradeState,
                             vname: str, vparams: dict, now: float):
        """Evalúa condiciones de salida para un trade abierto.
        Usa parámetros adaptativos (AEPS) para todos los thresholds de salida.
        Los parámetros de ENTRADA siguen usando vparams[].
        Aggressive variante usa v5 en vez de AEPS.
        """
        # Aggressive → v5 exit path (isolated to not crash base exits)
        if vname == "aggressive":
            try:
                await self._evaluate_exit_v5(state, vtrade, vparams, now)
            except Exception as e:
                log.error(f"v5 exit error [{state.symbol}]: {e}", exc_info=True)
            return

        price = state.mark_price
        entry = vtrade.entry_price

        if entry <= 0 or price <= 0:
            return

        # PnL % del SHORT (sin leverage): (entry - price) / entry
        pnl_pct = (entry - price) / entry
        hold_secs = now - vtrade.entry_time
        hold_hours = hold_secs / 3600.0

        # Actualizar MFE / MAE
        if pnl_pct > vtrade.mfe:
            vtrade.mfe = pnl_pct
        if pnl_pct < vtrade.mae:
            vtrade.mae = pnl_pct

        # Funding collection (intervalo dinámico, default 8h)
        funding_interval = state.funding_interval_secs
        if now - vtrade.last_funding_collection >= funding_interval:
            if state.funding_rate > 0 and vtrade.entry_notional > 0:
                current_notional = vtrade.entry_notional * state.mark_price / vtrade.entry_price if vtrade.entry_price > 0 else vtrade.entry_notional
                funding_payment = state.funding_rate * current_notional
                vtrade.funding_collected += funding_payment
            vtrade.last_funding_collection = now

        # ══════════════════════════════════════════════════════════
        #  BASE variant: simplified fixed exit rules
        # ══════════════════════════════════════════════════════════
        if vname == "base":
            exit_reason = None

            # 1. Hard Stop Loss 5%
            if pnl_pct <= -0.05:
                exit_reason = "stop_loss"

            # 2. MFE snapshot at 4-min mark
            if vtrade.mfe_2min_snapshot is None and hold_secs >= 240:
                vtrade.mfe_2min_snapshot = vtrade.mfe
                vtrade.mfe_2min_snapshot_dirty = True  # mark for batch persist

            # 3. Timer exit based on snapshot
            if not exit_reason and vtrade.mfe_2min_snapshot is not None:
                if vtrade.mfe_2min_snapshot >= 0.008:
                    # Good MFE → wait total 10 min (600s)
                    if hold_secs >= 600:
                        exit_reason = "exit_10min"
                else:
                    # Low MFE → wait total 5 min (300s)
                    if hold_secs >= 300:
                        exit_reason = "exit_5min"

            if exit_reason:
                await self._close_trade(state, vtrade, vname, vparams, now,
                                        price, pnl_pct, exit_reason)
            return

        # ══════════════════════════════════════════════════════════
        #  Non-base variants: AEPS adaptive exit (unchanged)
        # ══════════════════════════════════════════════════════════

        # ── AEPS: obtener parámetros adaptativos ──
        calibrator = self.exit_calibrators[vname]
        ap = calibrator.get_params(current_atr_pct=state.atr_pct)

        exit_reason = None

        # ── Early Abort (adaptive) ──
        if (ap.early_abort_hours > 0
              and hold_hours > ap.early_abort_hours
              and vtrade.mfe < ap.early_abort_max_mfe):
            exit_reason = "early_abort"

        # ── Zombie Kill: post-abort MFE filter (one-time check) ──
        if (not exit_reason
              and not vtrade.zombie_checked
              and ap.early_abort_hours > 0
              and hold_hours > ap.early_abort_hours):
            vtrade.zombie_checked = True
            zombie_thresh = STRATEGY.get("mfe_zombie_threshold", 0.012)
            if vtrade.mfe < zombie_thresh:
                exit_reason = "zombie_kill"
                log.warning(
                    f"🧟 [{vname.upper()}] Zombie kill {state.symbol} | "
                    f"MFE {vtrade.mfe:.2%} < {zombie_thresh:.2%} "
                    f"después de {hold_hours*60:.0f}min"
                )

        # ── Partial TP + Profit Lock (adaptive) ──
        if not exit_reason and ap.partial_tp_mfe_pct > 0 and ap.partial_tp_fraction > 0 and not vtrade.partial_tp_taken:
            if vtrade.mfe >= ap.partial_tp_mfe_pct:
                await self._partial_close_trade(
                    state, vtrade, vname, vparams, now, price, ap.partial_tp_fraction,
                )

        if not exit_reason and vtrade.partial_tp_taken and ap.profit_lock_pct > 0:
            if pnl_pct <= ap.profit_lock_pct:
                exit_reason = "profit_lock"

        # ── Trailing continuo con floor dinámico (adaptive) ──
        if ap.breakeven_trigger_pct > 0 and vtrade.mfe >= ap.breakeven_trigger_pct:
            if ap.trailing_activation_pct > 0 and vtrade.mfe >= ap.trailing_activation_pct:
                trail_floor = vtrade.mfe * (1.0 - ap.trailing_callback_pct)
            else:
                range_width = max(ap.trailing_activation_pct - ap.breakeven_trigger_pct, 0.001)
                progress = min((vtrade.mfe - ap.breakeven_trigger_pct) / range_width, 1.0)
                max_floor = vtrade.mfe * (1.0 - ap.trailing_callback_pct)
                trail_floor = progress * max_floor

            if pnl_pct < trail_floor:
                exit_reason = "trailing_stop"

        elif ap.trailing_activation_pct > 0 and vtrade.mfe >= ap.trailing_activation_pct:
            trail_floor = vtrade.mfe * (1.0 - ap.trailing_callback_pct)
            if pnl_pct < trail_floor:
                exit_reason = "trailing_stop"

        # ── (i) Stop Loss (adaptive) ──
        if not exit_reason and pnl_pct <= -ap.stop_loss_pct:
            exit_reason = "stop_loss"

        # ── (ii) Take Profit (static) ──
        if not exit_reason and pnl_pct >= vparams["take_profit_pct"]:
            exit_reason = "take_profit"

        # ── OI Circuit Breaker (static) ──
        if not exit_reason and vtrade.entry_oi > 0 and state.oi_value > 0:
            oi_change = (state.oi_value - vtrade.entry_oi) / vtrade.entry_oi
            if oi_change > vparams["oi_abort_pct"]:
                exit_reason = "oi_abort"

        # ── (iii) Maximum Hold (static) ──
        if not exit_reason and hold_hours >= vparams["max_hold_hours"]:
            exit_reason = "max_hold"

        # ── (iv) Pump Capture ──
        if not exit_reason and (vtrade.mfe >= 0.5 * abs(state.price_change_24h)
              and vtrade.mfe > 0.01
              and state.exhaustion >= 2):
            exit_reason = "pump_capture"

        # ── (v) Reversal Signal ──
        if not exit_reason and (hold_hours >= vparams["min_hold_hours"]
              and pnl_pct > 0.01
              and state.oi_change_1h > 0
              and state.taker_buy_ratio > 0.55):
            exit_reason = "reversal"

        if exit_reason:
            await self._close_trade(state, vtrade, vname, vparams, now,
                                    price, pnl_pct, exit_reason)

    async def _evaluate_exit_v5(self, state: SymbolState,
                                vtrade: VariantTradeState,
                                vparams: dict, now: float):
        """v5 exit evaluation for aggressive variant."""
        price = state.mark_price
        entry = vtrade.entry_price
        vname = "aggressive"

        if entry <= 0 or price <= 0:
            return

        pnl_pct = (entry - price) / entry
        hold_hours = (now - vtrade.entry_time) / 3600.0

        # Update MFE / MAE (aggressive skips the AEPS path that does this)
        if pnl_pct > vtrade.mfe:
            vtrade.mfe = pnl_pct
            vtrade.mfe_timestamp = now
        if pnl_pct < vtrade.mae:
            vtrade.mae = pnl_pct

        # Funding collection
        funding_interval = state.funding_interval_secs
        if now - vtrade.last_funding_collection >= funding_interval:
            if state.funding_rate > 0 and vtrade.entry_notional > 0:
                current_notional = vtrade.entry_notional * state.mark_price / vtrade.entry_price if vtrade.entry_price > 0 else vtrade.entry_notional
                funding_payment = state.funding_rate * current_notional
                vtrade.funding_collected += funding_payment
            vtrade.last_funding_collection = now

        mfe_pct = vtrade.mfe
        mae_pct = vtrade.mae

        # v5 tick
        trade_id_str = str(vtrade.open_trade_id)
        action = self.v5_exit.tick(
            trade_id_str, now, pnl_pct, mfe_pct, mae_pct
        )

        exit_reason = None

        # v5 decision → exit
        if action == "EXIT":
            if pnl_pct <= -self.v5_exit.hard_sl:
                exit_reason = "stop_loss"
            elif self.v5_exit.trades.get(trade_id_str) and \
                 self.v5_exit.trades[trade_id_str].trailing_active:
                exit_reason = "trailing_stop"
            else:
                exit_reason = "v5_exit"

        # Static safety nets (override v5 HOLD)
        # ── Take Profit 20% ──
        if not exit_reason and pnl_pct >= vparams["take_profit_pct"]:
            exit_reason = "take_profit"

        # ── OI Circuit Breaker ──
        if not exit_reason and vtrade.entry_oi > 0 and state.oi_value > 0:
            oi_change = (state.oi_value - vtrade.entry_oi) / vtrade.entry_oi
            if oi_change > vparams["oi_abort_pct"]:
                exit_reason = "oi_abort"

        # ── Maximum Hold 72h ──
        if not exit_reason and hold_hours >= vparams["max_hold_hours"]:
            exit_reason = "max_hold"

        # ── Pump Capture ──
        if not exit_reason and (mfe_pct >= 0.5 * abs(state.price_change_24h)
              and mfe_pct > 0.01
              and state.exhaustion >= 2):
            exit_reason = "pump_capture"

        if exit_reason:
            # Close v5 state first
            self.v5_exit.close(trade_id_str, pnl_pct, exit_reason)

            # Close the actual trade
            await self._close_trade(state, vtrade, vname, vparams, now,
                                    price, pnl_pct, exit_reason)

            # NOTE: v5 surface rebuild disabled — too resource-heavy,
            # risks degrading eval loop performance during open trades.
            # Use scripts/bootstrap_surface_from_db.py manually if needed.
            # asyncio.ensure_future(self._rebuild_v5_surface_safe())

    async def _rebuild_v5_surface_safe(self):
        """Non-blocking wrapper for v5 surface rebuild."""
        if getattr(self, '_v5_rebuild_running', False):
            return
        self._v5_rebuild_running = True
        try:
            await self._rebuild_v5_surface()
        except Exception as e:
            log.warning(f"v5 surface rebuild failed: {e}")
        finally:
            self._v5_rebuild_running = False

    async def _rebuild_v5_surface(self):
        """Rebuild P(win) surface after an aggressive trade closes.

        Uses aggressive's own trades once it has >= 30 closed trades,
        otherwise bootstraps from base variant trades.
        """
        import asyncpg
        from scripts.bootstrap_surface_from_db import build_surface_from_trades

        MIN_SELF_TRADES = 30  # minimum aggressive trades before self-calibration

        pool = await asyncpg.create_pool(
            host='localhost', port=5432, database='binance_futures',
            user='recorder', password='K32CzfnWtWtLoj98n6R5QTqEx3jLYLv5',
            min_size=1, max_size=2,
        )
        try:
            # Check how many aggressive trades we have
            async with pool.acquire() as conn:
                agg_count = await conn.fetchval("""
                    SELECT count(*)
                    FROM virtual_trades
                    WHERE variant = 'aggressive'
                      AND status = 'closed'
                      AND exit_time IS NOT NULL
                """)

            # Pick source: aggressive if enough trades, else bootstrap from base
            if agg_count >= MIN_SELF_TRADES:
                source_variant = 'aggressive'
            else:
                source_variant = 'base'

            async with pool.acquire() as conn:
                trades = await conn.fetch("""
                    SELECT id, symbol, entry_price, entry_time, exit_time,
                           pnl_pct
                    FROM virtual_trades
                    WHERE variant = $1
                      AND status = 'closed'
                      AND exit_time IS NOT NULL
                """, source_variant)
            if not trades:
                return

            import asyncio
            sem = asyncio.Semaphore(8)
            all_candles = {}

            async def _fetch(t):
                async with sem:
                    async with pool.acquire() as conn:
                        rows = await conn.fetch("""
                            SELECT bucket, open, high, low, close
                            FROM ohlcv_1m
                            WHERE symbol = $1
                              AND bucket >= to_timestamp($2) - interval '1 minute'
                              AND bucket <= to_timestamp($3) + interval '1 minute'
                            ORDER BY bucket
                        """, t['symbol'], float(t['entry_time']),
                            float(t['exit_time']))
                    return t['id'], rows

            results = await asyncio.gather(*[_fetch(t) for t in trades])
            for tid, rows in results:
                all_candles[tid] = rows

            surface = build_surface_from_trades(trades, all_candles)
            surface_path = os.path.join(
                os.path.dirname(__file__), "data", "pwin_surface.json"
            )
            import json as _json
            tmp = surface_path + ".tmp"
            with open(tmp, "w") as f:
                _json.dump(surface, f)
            os.replace(tmp, surface_path)

            self.v5_exit.reload_surface()
            log.info(
                f"v5 surface rebuilt: {len(surface)} cells "
                f"from {len(trades)} {source_variant} trades"
                f"{'' if source_variant == 'aggressive' else ' (bootstrap)'}"
            )
        finally:
            await pool.close()

    async def _partial_close_trade(self, state: SymbolState,
                                   vtrade: VariantTradeState,
                                   vname: str, vparams: dict,
                                   now: float, price: float,
                                   fraction: float):
        """
        Cierra una fracción de la posición (TP parcial).
        En live: cierre parcial en Binance.
        En paper: solo actualiza notional tracking.
        """
        if vtrade.partial_tp_taken:
            return  # ya se tomó

        close_qty = vtrade.live_qty * fraction if vtrade.live_qty > 0 else 0
        actual_price = price
        partial_notional = vtrade.entry_notional * fraction
        result = None

        # Safeguard: if live_qty=0 (copy-trading executedQty bug), query Binance
        if vtrade.trade_mode == "live" and self.trader and close_qty == 0:
            try:
                positions = await self.trader.get_positions(state.symbol)
                if positions:
                    pos_qty = abs(positions[0]["position_amt"])
                    if pos_qty > 0:
                        vtrade.live_qty = pos_qty
                        close_qty = pos_qty * fraction
                        log.warning(
                            f"⚠️ [{vname.upper()}] live_qty was 0, "
                            f"reconciled from Binance: {pos_qty}"
                        )
            except Exception as e:
                log.warning(f"⚠️ [{vname.upper()}] Could not reconcile live_qty: {e}")

        # ── LIVE: cierre parcial en Binance ──
        if vtrade.trade_mode == "live" and self.trader and close_qty > 0:
            try:
                # Cancelar TP/SL existentes (se van a recalcular con qty reducida)
                await self.trader.cancel_all_orders(state.symbol)
                result = await self.trader.close_position(
                    symbol=state.symbol,
                    quantity=close_qty,
                )
                avg_px = result.get("avgPrice", 0)
                if avg_px and float(avg_px) > 0:
                    actual_price = float(avg_px)
                vtrade.original_qty = vtrade.live_qty
                vtrade.live_qty = vtrade.live_qty - close_qty
                log.warning(
                    f"📐 [{vname.upper()}] PARTIAL TP {fraction:.0%} FILLED "
                    f"| {state.symbol} | closed {close_qty:.4f} @ ${actual_price:.6f} "
                    f"| remaining qty={vtrade.live_qty:.4f}"
                )
            except Exception as e:
                log.error(
                    f"❌ [{vname.upper()}] PARTIAL TP FAILED | "
                    f"{state.symbol} | {e}"
                )
                return  # no marcar como tomado si falló

        # Calcular PnL parcial realizado
        partial_pnl_pct = (vtrade.entry_price - actual_price) / vtrade.entry_price
        partial_pnl_usd = partial_pnl_pct * partial_notional
        # Fees: real exit commission + proportional entry commission
        exit_commission = float(result.get("totalCommission", 0)) if vtrade.trade_mode == "live" and result else 0.0
        entry_fee_portion = vtrade.entry_commission * fraction if vtrade.trade_mode == "live" else 0.0
        partial_fees = (entry_fee_portion + exit_commission) if vtrade.trade_mode == "live" else 2 * TAKER_FEE * partial_notional

        # Actualizar notional restante y PnL parcial realizado
        vtrade.entry_notional -= partial_notional
        vtrade.partial_tp_taken = True
        vtrade.partial_tp_pnl_usd = partial_pnl_usd - partial_fees
        if vtrade.original_qty == 0:
            vtrade.original_qty = vtrade.live_qty / (1 - fraction) if vtrade.live_qty > 0 else 0

        tmode = vtrade.trade_mode.upper()
        log.warning(
            f"📐 [{vname.upper()}][{tmode}] PARTIAL TP {fraction:.0%} | {state.symbol} "
            f"| PnL parcial={partial_pnl_pct:+.2%} ${partial_pnl_usd:+.2f} "
            f"| notional restante=${vtrade.entry_notional:,.0f}"
        )

        # Telegram notification
        if self.telegram:
            try:
                await self.telegram.send_message(
                    f"📐 *PARTIAL TP {fraction:.0%}* | {state.symbol} [{vname.upper()}]\n"
                    f"PnL parcial: {partial_pnl_pct:+.2%} (${partial_pnl_usd:+.2f})\n"
                    f"Remaining: ${vtrade.entry_notional:,.0f}",
                )
            except Exception:
                pass

    async def _close_trade(self, state: SymbolState,
                           vtrade: VariantTradeState,
                           vname: str, vparams: dict, now: float,
                           exit_price: float, pnl_pct: float, reason: str):
        """Cierra un trade (dry-run o live)."""
        trade_id = vtrade.open_trade_id
        notional = vtrade.entry_notional

        # ── LIVE MODE: cerrar posición real en Binance ──
        # Usa el modo con que se ABRIÓ el trade (vtrade.trade_mode), NO el modo actual.
        # Esto evita intentar cerrar en Binance un trade que fue paper, o dejar
        # abierto en Binance un trade live si el modo cambió a dry-run.
        actual_exit_price = exit_price
        result = None
        if vtrade.trade_mode == "live" and self.trader:
            try:
                # Cancelar órdenes condicionales (TP/SL) antes de cerrar
                await self.trader.cancel_all_orders(state.symbol)
                # Cerrar la posición
                result = await self.trader.close_position(
                    symbol=state.symbol,
                    quantity=vtrade.live_qty or None,
                )
                avg_px = result.get("avgPrice", 0)
                if avg_px and float(avg_px) > 0:
                    actual_exit_price = float(avg_px)
                log.warning(
                    f"💰 [{vname.upper()}] LIVE CLOSE FILLED "
                    f"| {state.symbol} | orderId={result.get('orderId')} "
                    f"| @ ${actual_exit_price:.6f} | reason={reason}"
                )
            except Exception as e:
                log.error(
                    f"❌ [{vname.upper()}] LIVE CLOSE FAILED | "
                    f"{state.symbol} | {e} — recording virtual close anyway"
                )

        # Recalcular PnL con el precio real de cierre (live) o el original (dry-run)
        pnl_pct = (vtrade.entry_price - actual_exit_price) / vtrade.entry_price

        # Fees: real Binance commission (live) or estimated (paper)
        if vtrade.trade_mode == "live":
            exit_commission = float(result.get("totalCommission", 0)) if result else 0.0
            # Entry commission proportional to remaining notional
            if vtrade.partial_tp_taken:
                remaining_frac = 1.0 - VARIANTS.get(vname, {}).get("partial_tp_fraction", 0)
                entry_fee_remaining = vtrade.entry_commission * remaining_frac
            else:
                entry_fee_remaining = vtrade.entry_commission
            fees = entry_fee_remaining + exit_commission
        else:
            fees = 2 * TAKER_FEE * notional

        # PnL con leverage
        pnl_leveraged = pnl_pct * vparams["leverage"]
        pnl_usd = pnl_pct * notional + vtrade.funding_collected - fees

        # Si hubo TP parcial, sumar el PnL realizado de la porción cerrada
        if vtrade.partial_tp_taken:
            pnl_usd += vtrade.partial_tp_pnl_usd

        hold_hours = (now - vtrade.entry_time) / 3600.0

        exit_data = {
            "exit_time": now,
            "exit_price": actual_exit_price,
            "exit_reason": reason,
            "pnl_pct": pnl_pct,
            "pnl_leveraged": pnl_leveraged,
            "pnl_usd": pnl_usd,
            "funding_collected": vtrade.funding_collected,
            "fees_paid": fees,
            "mfe_pct": vtrade.mfe,
            "mae_pct": vtrade.mae,
            "hold_hours": hold_hours,
            # AEPS fields
            "etd_pct": vtrade.mfe - pnl_pct,
            "atr_at_entry": state.atr_pct,
            "time_to_mfe_secs": (vtrade.mfe_timestamp - vtrade.entry_time)
                                if vtrade.mfe_timestamp > 0 else 0,
            "partial_tp_triggered": vtrade.partial_tp_taken,
        }

        await self.writer.close_virtual_trade(trade_id, exit_data)

        # ── AEPS: registrar trade en calibrador (skip aggressive — uses v5) ──
        if vname in self.exit_calibrators:
            try:
                record = TradeRecord(
                    mfe_pct=vtrade.mfe,
                    mae_pct=vtrade.mae,
                    etd_pct=vtrade.mfe - pnl_pct,
                    pnl_pct=pnl_pct,
                    atr_at_entry=state.atr_pct,
                    time_to_mfe_secs=(vtrade.mfe_timestamp - vtrade.entry_time)
                                      if vtrade.mfe_timestamp > 0 else 0,
                    hold_secs=(now - vtrade.entry_time),
                    exit_reason=reason,
                    score_at_entry=state.score,
                    is_winner=(pnl_pct > 0),
                )
                cal = self.exit_calibrators[vname]
                prev_cal_count = cal._calibration_count
                cal.add_trade(record)
                # Telegram notification on recalibration
                if cal._calibration_count > prev_cal_count and self.telegram:
                    try:
                        ap = cal.current
                        await self.telegram.send_message(
                            f"🔧 *AEPS Recalibrado* [{vname}]\n"
                            f"SL: {ap.stop_loss_pct:.2%} | "
                            f"Trailing: {ap.trailing_activation_pct:.2%}/{ap.trailing_callback_pct:.0%}\n"
                            f"PTP: {ap.partial_tp_mfe_pct:.2%} | "
                            f"Abort: {ap.early_abort_hours:.1f}h/{ap.early_abort_max_mfe:.2%}\n"
                            f"Basado en últimos {ap.calibration_n} trades"
                        )
                    except Exception:
                        pass
            except Exception as e:
                log.warning(f"AEPS feed error [{vname}]: {e}")

        # Actualizar equity de la variante (separado por modo del trade)
        tmode = vtrade.trade_mode
        self.equities[tmode][vname] += pnl_usd
        self.daily_pnl[tmode][vname] += pnl_usd

        # Emoji según resultado
        emoji = "🟢" if pnl_usd >= 0 else "🔻"
        mode_tag = tmode.upper()

        log.warning(
            f"{emoji} [{vname.upper()}][{mode_tag}] CERRADO #{trade_id} | {state.symbol} | "
            f"razón={reason} | PnL={pnl_pct:+.2%} "
            f"(x{vparams['leverage']}={pnl_leveraged:+.2%}) | "
            f"${pnl_usd:+,.2f} | hold={hold_hours:.1f}h | "
            f"MFE={vtrade.mfe:.2%} MAE={vtrade.mae:.2%} | "
            f"equity[{tmode}]=${self.equities[tmode][vname]:,.2f}"
        )

        # Notificación Telegram
        if self.telegram:
            try:
                await self.telegram.notify_trade_close(
                    trade_id=trade_id, symbol=state.symbol, variant=vname,
                    reason=reason, pnl_pct=pnl_pct, pnl_leveraged=pnl_leveraged,
                    pnl_usd=pnl_usd, hold_hours=hold_hours,
                    mfe=vtrade.mfe, mae=vtrade.mae,
                    equity=self.equities[tmode][vname], mode=mode_tag,
                )
            except Exception:
                pass

        # Circuit breaker diario (por modo)
        # Live: usar equity real; Paper: usar INITIAL_CAPITAL
        ref_capital = self.equities[tmode][vname] if tmode == "live" else INITIAL_CAPITAL
        if ref_capital > 0 and self.daily_pnl[tmode][vname] / ref_capital < -STRATEGY["max_daily_loss_pct"]:
            self.halted[tmode][vname] = True
            log.error(
                f"🛑 [{vname.upper()}][{mode_tag}] HALT DIARIO ACTIVADO — "
                f"pérdida diaria ${self.daily_pnl[tmode][vname]:,.2f} "
                f"({self.daily_pnl[tmode][vname]/ref_capital:.1%} de ${ref_capital:,.2f})"
            )
            if self.telegram:
                try:
                    await self.telegram.notify_halt(
                        vname, self.daily_pnl[tmode][vname],
                        self.daily_pnl[tmode][vname] / INITIAL_CAPITAL,
                    )
                except Exception:
                    pass

        # Capture values needed for post-trade reconciliation before reset
        vtrade_trade_mode = vtrade.trade_mode
        entry_order_id = vtrade.binance_order_id
        exit_order_id = result.get("orderId") if result else None
        funding_collected = vtrade.funding_collected
        partial_tp_taken = vtrade.partial_tp_taken
        partial_tp_pnl = vtrade.partial_tp_pnl_usd

        # Reset trade state
        vtrade.open_trade_id = None
        vtrade.entry_price = 0.0
        vtrade.entry_time = 0.0
        vtrade.entry_oi = 0.0
        vtrade.entry_notional = 0.0
        vtrade.mfe = 0.0
        vtrade.mae = 0.0
        vtrade.funding_collected = 0.0
        vtrade.last_trade_close_time = now
        vtrade.trade_mode = "paper"
        vtrade.binance_order_id = None
        vtrade.live_qty = 0.0
        vtrade.entry_commission = 0.0
        vtrade.partial_tp_taken = False
        vtrade.original_qty = 0.0
        vtrade.partial_tp_pnl_usd = 0.0
        vtrade.mfe_timestamp = 0.0
        vtrade.zombie_checked = False

        # NO resetear energy — se disipa naturalmente cuando el score baja
        # (la energía es estado de mercado compartido, no de un trade)

        # Grabar snapshot de cierre
        await self.writer.insert_snapshot(state.to_snapshot(now))

        # ── Post-trade reconciliation: correct prices from Binance ──
        # The immediate order response may not include avgPrice for
        # copy-trading accounts.  Query the settled order data and
        # update DB with the real fill prices / fees.
        if vtrade_trade_mode == "live" and self.trader and entry_order_id:
            asyncio.ensure_future(
                self._reconcile_trade_prices(
                    trade_id, state.symbol, entry_order_id,
                    exit_order_id, notional, vparams, vname,
                    funding_collected, partial_tp_taken, partial_tp_pnl,
                )
            )

    async def _reconcile_trade_prices(
        self, trade_id: int, symbol: str,
        entry_order_id: int, exit_order_id: Optional[int],
        notional: float, vparams: dict, vname: str,
        funding_collected: float, partial_tp_taken: bool,
        partial_tp_pnl: float,
    ):
        """Query Binance for settled order data and correct DB prices/PnL.

        Runs as a fire-and-forget task after the trade is already closed and
        recorded, so it adds zero latency to the critical path.
        """
        try:
            # Small delay to let Binance settle the order data
            await asyncio.sleep(2)

            real_entry_price = None
            real_exit_price = None
            entry_fee = 0.0
            exit_fee = 0.0

            # ── Query entry order ──
            try:
                entry_ord = await self.trader.get_order(symbol, entry_order_id)
                avg = entry_ord.get("avgPrice", "0")
                if avg and float(avg) > 0:
                    real_entry_price = float(avg)
                cum = float(entry_ord.get("cumQuote", 0))
                if cum > 0:
                    # Fee = taker_rate * cumQuote (notional traded)
                    entry_fee = 0.0005 * cum
            except Exception as e:
                log.warning(f"♻️ Reconcile #{trade_id}: entry order query failed: {e}")

            # ── Query exit order ──
            if exit_order_id:
                try:
                    exit_ord = await self.trader.get_order(symbol, exit_order_id)
                    avg = exit_ord.get("avgPrice", "0")
                    if avg and float(avg) > 0:
                        real_exit_price = float(avg)
                    cum = float(exit_ord.get("cumQuote", 0))
                    if cum > 0:
                        exit_fee = 0.0005 * cum
                except Exception as e:
                    log.warning(f"♻️ Reconcile #{trade_id}: exit order query failed: {e}")

            if real_entry_price is None and real_exit_price is None:
                return  # nothing to fix

            # ── Recalculate PnL with real prices ──
            ep = real_entry_price or 0
            xp = real_exit_price or 0
            if ep > 0 and xp > 0:
                pnl_pct = (ep - xp) / ep  # short
            elif ep > 0:
                # Only entry corrected — can't recalc properly, skip
                return
            else:
                return

            fees = entry_fee + exit_fee
            pnl_leveraged = pnl_pct * vparams["leverage"]
            pnl_usd = pnl_pct * notional + funding_collected - fees
            if partial_tp_taken:
                pnl_usd += partial_tp_pnl

            # ── Update DB ──
            async with self.writer._pool.acquire() as conn:
                await conn.execute(
                    "UPDATE virtual_trades SET "
                    "entry_price=$1, exit_price=$2, "
                    "pnl_pct=$3, pnl_leveraged=$4, pnl_usd=$5, fees_paid=$6 "
                    "WHERE id=$7",
                    ep, xp, pnl_pct, pnl_leveraged, pnl_usd, fees, trade_id,
                )

            log.info(
                f"♻️ [{vname.upper()}] Reconciled #{trade_id} {symbol} | "
                f"entry {ep:.6f} exit {xp:.6f} | "
                f"PnL ${pnl_usd:+.4f} fees ${fees:.4f}"
            )

        except Exception as e:
            log.warning(f"♻️ Reconcile #{trade_id} failed: {e}")

    # ══════════════════════════════════════════════════════════════
    #  Restaurar trades abiertos tras reinicio
    # ══════════════════════════════════════════════════════════════

    async def restore_open_trades(self):
        """
        Al arrancar, restaura trades abiertos de la DB para todas las variantes.
        """
        open_trades = await self.writer.get_open_trades()
        for t in open_trades:
            sym = t["symbol"]
            variant = t["variant"]
            if variant not in self.variant_trades:
                log.warning(
                    f"Trade #{t['id']} variante '{variant}' desconocida — ignorando"
                )
                continue
            vtrade = self.variant_trades[variant].get(sym)
            if vtrade is None:
                self.variant_trades[variant][sym] = VariantTradeState()
                vtrade = self.variant_trades[variant][sym]

            vtrade.open_trade_id = t["id"]
            vtrade.entry_price = t["entry_price"]
            vtrade.entry_time = t["entry_time"]
            vtrade.entry_notional = t.get("position_size", 0.0) or 0.0
            vtrade.mfe = t.get("mfe_pct", 0.0) or 0.0
            vtrade.mae = t.get("mae_pct", 0.0) or 0.0
            vtrade.trade_mode = t.get("trading_mode", "paper") or "paper"
            vtrade.funding_collected = t.get("funding_collected", 0.0) or 0.0
            vtrade.last_funding_collection = time.time()  # evitar double-collect
            # OI de entrada del snapshot
            snap = json.loads(t.get("entry_snapshot", "{}") or "{}")
            vtrade.entry_oi = snap.get("oi_value", 0.0) or 0.0
            # Restaurar mfe_2min_snapshot para base simplified exit
            raw_snap = snap.get("mfe_2min_snapshot")
            if raw_snap is not None:
                vtrade.mfe_2min_snapshot = float(raw_snap)
            # Restaurar live_qty desde Binance si es trade live
            if vtrade.trade_mode == "live" and self.trader:
                try:
                    positions = await self.trader.get_positions(sym)
                    if positions:
                        vtrade.live_qty = positions[0]["position_amt"]
                        # Detectar si partial TP ya fue tomado:
                        # Si la posición actual es menor que la esperada por el notional,
                        # es que ya se cerró una fracción.
                        # SKIP for base: base never executes partial TP, so any
                        # qty difference is just Binance rounding.  Detecting a
                        # false partial TP here would reduce entry_notional by
                        # ~33 %, corrupting PnL for winning trades.
                        vparams = VARIANTS.get(variant, {})
                        expected_notional = t.get("position_size", 0.0) or 0.0
                        if variant != "base" and expected_notional > 0 and vtrade.entry_price > 0:
                            expected_qty = expected_notional / vtrade.entry_price
                            pt_frac = vparams.get("partial_tp_fraction", 0)
                            if pt_frac > 0 and vtrade.live_qty < expected_qty * (1 - pt_frac / 2):
                                vtrade.partial_tp_taken = True
                                vtrade.original_qty = expected_qty
                                vtrade.entry_notional = expected_notional * (1 - pt_frac)
                                log.info(
                                    f"📐 [{variant}] Partial TP detectado en restore "
                                    f"| {sym} | qty actual={vtrade.live_qty:.4f} "
                                    f"vs esperada={expected_qty:.4f}"
                                )
                except Exception as e:
                    log.warning(f"\u26a0\ufe0f No se pudo restaurar live_qty #{t['id']}: {e}")
            tmode = vtrade.trade_mode.upper()
            log.info(
                f"♻️  [{variant}][{tmode}] Trade restaurado #{t['id']} | {sym} "
                f"@ ${t['entry_price']:.6f} "
                f"(abierto hace {(time.time() - t['entry_time'])/3600:.1f}h)"
            )

        # Restaurar AEPS calibrators desde disco
        self.restore_calibrators()

    # ══════════════════════════════════════════════════════════════
    #  GRABACIÓN CONDICIONAL
    # ══════════════════════════════════════════════════════════════

    def should_record(self, symbol: str) -> bool:
        """
        ¿Debe grabarse datos pesados (depth, aggTrades, bookTickers)
        para este símbolo?

        Graba si CUALQUIER variante cumple:
          1. Tiene un trade virtual abierto
          2. Score >= pre_record_score (se está calentando)
          3. Alguna variante cerró un trade hace menos de post_close_tail_secs
        """
        state = self.states.get(symbol.upper())
        if state is None or not state._initialized:
            return False

        now = time.time()
        rec = RECORDING
        sym = symbol.upper()

        # (1) Cualquier variante tiene trade abierto → grabar siempre
        for vname in VARIANTS:
            vtrade = self.variant_trades[vname].get(sym)
            if vtrade and vtrade.open_trade_id is not None:
                if not state.recording:
                    state.recording = True
                    state._record_reason = f"trade_open({vname})"
                    log.info(f"🔴 REC ON  {symbol} — {vname} trade abierto")
                return True

        # (2) Score alto → pre-grabación (compartido)
        if state.score >= rec["pre_record_score"]:
            if not state.recording:
                state.recording = True
                state._record_reason = "pre_record"
                log.info(
                    f"🟡 REC ON  {symbol} — pre-record (Ŝ={state.score:.1f})"
                )
            return True

        # (3) Cola post-cierre (cualquier variante)
        for vname in VARIANTS:
            vtrade = self.variant_trades[vname].get(sym)
            if (vtrade and vtrade.last_trade_close_time > 0
                    and now - vtrade.last_trade_close_time < rec["post_close_tail_secs"]):
                if not state.recording:
                    state.recording = True
                    state._record_reason = f"post_close({vname})"
                    log.info(f"🟠 REC ON  {symbol} — post-close tail ({vname})")
                return True

        # No cumple ninguna condición → dejar de grabar
        if state.recording:
            state.recording = False
            log.info(
                f"⬜ REC OFF {symbol} — "
                f"(fue: {state._record_reason}, Ŝ={state.score:.1f})"
            )
            state._record_reason = ""
        return False

    # ══════════════════════════════════════════════════════════════
    #  Snapshot periódico de TODOS los símbolos
    # ══════════════════════════════════════════════════════════════

    async def persist_open_mfe(self):
        """Persiste MFE/MAE y funding de todos los trades abiertos en DB."""
        for vname in VARIANTS:
            for sym, vtrade in self.variant_trades[vname].items():
                if vtrade.open_trade_id is not None and vtrade.entry_price > 0:
                    try:
                        snapshot_val = None
                        if getattr(vtrade, 'mfe_2min_snapshot_dirty', False):
                            snapshot_val = vtrade.mfe_2min_snapshot
                        await self.writer.update_open_trade_mfe(
                            vtrade.open_trade_id, vtrade.mfe, vtrade.mae,
                            vtrade.funding_collected,
                            mfe_2min_snapshot=snapshot_val,
                        )
                        if snapshot_val is not None:
                            vtrade.mfe_2min_snapshot_dirty = False
                    except Exception as e:
                        log.debug(f"MFE persist error #{vtrade.open_trade_id}: {e}")

    async def snapshot_all(self, now: float):
        """Graba snapshot de cada símbolo (estado de mercado compartido)."""
        for sym, state in self.states.items():
            if state._initialized:
                await self.writer.insert_snapshot(state.to_snapshot(now))

    # ══════════════════════════════════════════════════════════════
    #  AEPS persistence
    # ══════════════════════════════════════════════════════════════

    def persist_calibrators(self):
        """Guarda estado del AEPS en disco (JSON por variante)."""
        for vname, cal in self.exit_calibrators.items():
            path = os.path.join(os.path.dirname(__file__), f"aeps_{vname}.json")
            tmp = path + ".tmp"
            try:
                with open(tmp, "w") as f:
                    json.dump(cal.to_dict(), f)
                os.replace(tmp, path)
            except Exception as e:
                log.warning(f"AEPS persist error [{vname}]: {e}")

    def restore_calibrators(self):
        """Restaura AEPS al arrancar (skip aggressive — uses v5)."""
        for vname, vparams in VARIANTS.items():
            if vname == "aggressive":
                continue
            path = os.path.join(os.path.dirname(__file__), f"aeps_{vname}.json")
            if os.path.exists(path):
                try:
                    with open(path) as f:
                        data = json.load(f)
                    self.exit_calibrators[vname] = (
                        AdaptiveExitCalibrator.from_dict(data, vparams)
                    )
                    # Force recalibration on startup to pick up any AEPS logic changes
                    cal = self.exit_calibrators[vname]
                    if len(cal.history) >= cal.MIN_TRADES_TO_CALIBRATE:
                        cal._recalibrate()
                    log.info(f"♻️ AEPS [{vname}] restaurado: "
                             f"{self.exit_calibrators[vname].status()}")
                except Exception as e:
                    log.warning(f"AEPS restore error [{vname}]: {e}")

    # ══════════════════════════════════════════════════════════════
    #  Status (para logging periódico)
    # ══════════════════════════════════════════════════════════════

    def status_summary(self) -> str:
        """Resumen de una línea del estado actual de todas las variantes."""
        total = len(self.states)
        initialized = sum(1 for s in self.states.values() if s._initialized)

        # Per-variant × per-mode summary
        # PAPER: C=$10k(0) B=$10k(1) …  |  LIVE: B=$10k(0)
        paper_parts = []
        live_parts = []
        for vname in VARIANTS:
            n_open_paper = sum(
                1 for vts in self.variant_trades[vname].values()
                if vts.open_trade_id is not None and vts.trade_mode == "paper"
            )
            n_open_live = sum(
                1 for vts in self.variant_trades[vname].values()
                if vts.open_trade_id is not None and vts.trade_mode == "live"
            )
            eq_p = self.equities["paper"][vname]
            eq_l = self.equities["live"][vname]
            hp = "🛑" if self.halted["paper"][vname] else ""
            hl = "🛑" if self.halted["live"][vname] else ""
            paper_parts.append(f"{vname[0].upper()}=${eq_p:,.0f}({n_open_paper}){hp}")
            live_parts.append(f"{vname[0].upper()}=${eq_l:,.0f}({n_open_live}){hl}")

        recording = [
            s.symbol for s in self.states.values() if s.recording
        ]
        hot_symbols = [
            f"{s.symbol}({s.score:.1f})"
            for s in self.states.values()
            if s.score >= 2.0 and s._initialized
        ]

        rec_str = ", ".join(recording) if recording else "ninguno"

        # AEPS status (+ v5 for aggressive)
        aeps_parts = []
        for vname in VARIANTS:
            if vname == "aggressive":
                st = self.v5_exit.status()
                stats = st.get("stats", {})
                n = stats.get("total", 0)
                wr = stats.get("win_rate", 0)
                aeps_parts.append(
                    f"A:v5(n={n},WR={wr:.0%})"
                )
            else:
                cal = self.exit_calibrators[vname]
                aeps_parts.append(
                    f"{vname[0].upper()}: "
                    f"SL={cal.current.stop_loss_pct:.1%}/"
                    f"T={cal.current.trailing_activation_pct:.1%}"
                )
        aeps_str = " | ".join(aeps_parts)

        return (
            f"📡 {initialized}/{total} | "
            f"📋PAPER {' '.join(paper_parts)} | "
            f"💰LIVE {' '.join(live_parts)} | "
            f"🔴 REC [{len(recording)}]: {rec_str} | "
            f"hot: {', '.join(hot_symbols) if hot_symbols else '-'} | "
            f"🔧AEPS [{aeps_str}]"
        )

    def detailed_status(self) -> list[str]:
        """Status detallado por símbolo (para logging cada ~60s)."""
        lines = []
        for sym in sorted(self.states.keys()):
            s = self.states[sym]
            if not s._initialized:
                continue

            # Check if any variant has an open trade (with mode tag)
            trades = []
            for vname in VARIANTS:
                vtrade = self.variant_trades[vname].get(sym)
                if vtrade and vtrade.open_trade_id is not None:
                    m = "L" if vtrade.trade_mode == "live" else "P"
                    trades.append(f"{vname[0].upper()}{m}")
            marker = f"📍{''.join(trades)}" if trades else "  "

            lines.append(
                f"{marker} {sym:>14s} | "
                f"P=${s.mark_price:<12.6f} | "
                f"Ŝ={s.score:.1f} [{s.c_fund:.1f},{s.c_oi:.1f},"
                f"{s.c_price:.1f},{s.c_taker:.1f},{s.c_vol:.1f}] | "
                f"E={s.energy:.1f}h | Ê={s.exhaustion} | "
                f"r={s.funding_rate:.4%} | "
                f"ΔOI={s.oi_change_24h:.1%} | "
                f"ΔP={s.price_change_12h:.1%} | "
                f"η={s.taker_buy_ratio:.1%} | "
                f"V/V̄={s.volume_ratio:.1f}x"
            )
        return lines

    # ── Status file for API/dashboard ──

    def write_status_file(self):
        """Write a JSON status file so the API process can serve strategy state."""
        now = time.time()
        initialized = sum(1 for s in self.states.values() if s._initialized)

        # Hot candidates: score >= 2.0
        hot = []
        for s in self.states.values():
            if not s._initialized or s.score < 2.0:
                continue
            hot.append({
                "symbol": s.symbol,
                "score": round(s.score, 2),
                "c_fund": round(s.c_fund, 1),
                "c_oi": round(s.c_oi, 1),
                "c_price": round(s.c_price, 1),
                "c_taker": round(s.c_taker, 1),
                "c_vol": round(s.c_vol, 1),
                "energy": round(s.energy, 2),
                "exhaustion": s.exhaustion,
                "funding_rate": round(s.funding_rate, 6),
                "mark_price": s.mark_price,
                "oi_change_24h": round(s.oi_change_24h, 4),
                "price_change_12h": round(s.price_change_12h, 4),
                "taker_buy_ratio": round(s.taker_buy_ratio, 4),
                "volume_ratio": round(s.volume_ratio, 2),
                "recording": s.recording,
            })
        hot.sort(key=lambda x: x["score"], reverse=True)

        # Recording symbols
        recording = [s.symbol for s in self.states.values() if s.recording]

        # Open trades
        open_trades = []
        for vname in VARIANTS:
            for sym, vts in self.variant_trades[vname].items():
                if vts.open_trade_id is None:
                    continue
                s = self.states.get(sym)
                mp = s.mark_price if s else 0
                pnl_pct = (vts.entry_price - mp) / vts.entry_price if vts.entry_price > 0 and mp > 0 else 0
                open_trades.append({
                    "symbol": sym,
                    "variant": vname,
                    "mode": vts.trade_mode,
                    "entry_price": vts.entry_price,
                    "mark_price": mp,
                    "pnl_pct": round(pnl_pct * 100, 3),
                    "mfe_pct": round(vts.mfe * 100, 3) if vts.mfe else 0,
                    "mae_pct": round(vts.mae * 100, 3) if vts.mae else 0,
                    "hold_hours": round((now - vts.entry_time) / 3600, 2) if vts.entry_time else 0,
                })

        # Equities
        equities = {
            "paper": {v: round(e, 2) for v, e in self.equities["paper"].items()},
            "live": {v: round(e, 2) for v, e in self.equities["live"].items()},
        }
        daily_pnl = {
            "paper": {v: round(p, 2) for v, p in self.daily_pnl["paper"].items()},
            "live": {v: round(p, 2) for v, p in self.daily_pnl["live"].items()},
        }

        # AEPS status (+ v5 for aggressive)
        aeps_status = {}
        for vname, cal in self.exit_calibrators.items():
            ap = cal.current
            aeps_status[vname] = {
                "calibration_n": ap.calibration_n,
                "calibration_count": cal._calibration_count,
                "history_size": len(cal.history),
                "stop_loss_pct": round(ap.stop_loss_pct, 4),
                "partial_tp_mfe_pct": round(ap.partial_tp_mfe_pct, 4),
                "profit_lock_pct": round(ap.profit_lock_pct, 4),
                "breakeven_trigger_pct": round(ap.breakeven_trigger_pct, 4),
                "trailing_activation_pct": round(ap.trailing_activation_pct, 4),
                "trailing_callback_pct": round(ap.trailing_callback_pct, 4),
                "early_abort_hours": round(ap.early_abort_hours, 2),
                "early_abort_max_mfe": round(ap.early_abort_max_mfe, 4),
                "early_abort_max_loss": round(ap.early_abort_max_loss, 4),
            }

        # v5 status for aggressive
        v5_status = self.v5_exit.status()

        status = {
            "timestamp": now,
            "strategy_version": STRATEGY_VERSION,
            "initialized": initialized,
            "total_symbols": len(self.states),
            "trading_mode": self.trading_mode,
            "active_variant": self.active_variant,
            "hot_candidates": hot,
            "recording": recording,
            "open_trades": open_trades,
            "equities": equities,
            "daily_pnl": daily_pnl,
            "aeps": aeps_status,
            "v5": v5_status,
        }

        path = os.path.join(os.path.dirname(__file__), "strategy_status.json")
        tmp = path + ".tmp"
        try:
            with open(tmp, "w") as f:
                json.dump(status, f)
            os.replace(tmp, path)
        except Exception as e:
            log.warning("Error writing status file: %s", e)
