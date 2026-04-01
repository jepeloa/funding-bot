"""
Adaptive Exit Parameter System (AEPS) — α_f Bifurcation Short Strategy

Recalibra automáticamente los parámetros de salida basándose en la
distribución empírica de MFE/MAE/ETD de los últimos N trades cerrados.

Principio: "Fijar las entradas, adaptar las salidas."

Refs:
  - Leung & Zhang (2021), "Optimal Trading with a Trailing Stop"
  - arXiv:1609.00869, Bayesian Max Drawdown Analysis
  - Zhang et al. (2021), "TradeBot: Bandit learning for hyper-parameters"
"""

import logging
import time
from collections import deque
from dataclasses import dataclass

log = logging.getLogger("adaptive_exit")


# ══════════════════════════════════════════════════════════════════
#  Data structures
# ══════════════════════════════════════════════════════════════════

@dataclass
class TradeRecord:
    """Registro mínimo de un trade cerrado para calibración."""
    mfe_pct: float
    mae_pct: float
    etd_pct: float            # MFE - PnL (end trade drawdown)
    pnl_pct: float
    atr_at_entry: float
    time_to_mfe_secs: float
    hold_secs: float
    exit_reason: str
    score_at_entry: float
    is_winner: bool            # pnl_pct > 0


@dataclass
class AdaptiveParams:
    """Parámetros de salida calibrados. Reemplazan a los fijos de VARIANTS."""
    stop_loss_pct: float
    partial_tp_mfe_pct: float
    partial_tp_fraction: float
    profit_lock_pct: float
    breakeven_trigger_pct: float
    trailing_activation_pct: float
    trailing_callback_pct: float
    early_abort_hours: float
    early_abort_max_mfe: float
    early_abort_max_loss: float
    # Metadata
    calibration_n: int         # trades usados para calibrar
    calibration_time: float    # epoch de la última calibración


# ══════════════════════════════════════════════════════════════════
#  Calibrator
# ══════════════════════════════════════════════════════════════════

class AdaptiveExitCalibrator:
    """
    Recalibra parámetros de salida cada N trades usando
    distribución empírica de MFE/MAE/ETD.

    Filosofía: los percentiles de la distribución histórica
    definen los thresholds naturales del mercado actual.
    No asume distribución paramétrica, usa estadística descriptiva robusta.
    """

    WINDOW_SIZE = 25           # trades en la rolling window
    MIN_TRADES_TO_CALIBRATE = 15  # mínimo para primera calibración

    # Guardrails: límites duros para evitar parámetros degenerados
    BOUNDS = {
        "stop_loss_pct":           (0.02, 0.08),    # 2% - 8%
        "partial_tp_mfe_pct":      (0.008, 0.04),   # 0.8% - 4%
        "partial_tp_fraction":     (0.25, 0.50),     # 25% - 50%
        "profit_lock_pct":         (0.003, 0.015),   # 0.3% - 1.5%
        "breakeven_trigger_pct":   (0.01, 0.04),     # 1% - 4%
        "trailing_activation_pct": (0.02, 0.08),     # 2% - 8%
        "trailing_callback_pct":   (0.15, 0.65),     # 15% - 65%
        "early_abort_hours":       (0.75, 3.0),      # 45min - 3h
        "early_abort_max_mfe":     (0.003, 0.01),    # 0.3% - 1.0%
        "early_abort_max_loss":    (-0.03, -0.01),   # -3% a -1%
    }

    def __init__(self, base_params: dict):
        """
        Args:
            base_params: parámetros estáticos de VARIANTS[x] como fallback.
        """
        self.base_params = base_params
        self.history: deque[TradeRecord] = deque(maxlen=self.WINDOW_SIZE)
        self.current: AdaptiveParams = self._from_static(base_params)
        self._calibration_count = 0

    def _from_static(self, p: dict) -> AdaptiveParams:
        """Construye AdaptiveParams desde la config estática (fallback)."""
        return AdaptiveParams(
            stop_loss_pct=p.get("stop_loss_pct", 0.035),
            partial_tp_mfe_pct=p.get("partial_tp_mfe_pct", 0.015),
            partial_tp_fraction=p.get("partial_tp_fraction", 0.33),
            profit_lock_pct=p.get("profit_lock_pct", 0.005),
            breakeven_trigger_pct=p.get("breakeven_trigger_pct", 0.02),
            trailing_activation_pct=p.get("trailing_activation_pct", 0.05),
            trailing_callback_pct=p.get("trailing_callback_pct", 0.50),
            early_abort_hours=p.get("early_abort_hours", 2.0),
            early_abort_max_mfe=p.get("early_abort_max_mfe", 0.005),
            early_abort_max_loss=p.get("early_abort_max_loss", -0.02),
            calibration_n=0,
            calibration_time=0.0,
        )

    # ──────────────────────────────────────────────────────────
    #  Public API
    # ──────────────────────────────────────────────────────────

    def add_trade(self, record: TradeRecord):
        """Agrega un trade cerrado y recalibra si toca."""
        self.history.append(record)
        if len(self.history) >= self.MIN_TRADES_TO_CALIBRATE:
            self._recalibrate()

    def get_params(self, current_atr_pct: float = 0.0) -> AdaptiveParams:
        """
        Devuelve los parámetros actuales con warmup blend + ATR scaling.

        Warmup: antes de MIN_TRADES → 100% estático.
        Después: blend progresivo hasta WINDOW_SIZE → 100% adaptativo.
        Si current_atr_pct > 0, escala SL y trailing proporcionalmente.
        """
        n = len(self.history)

        if n < self.MIN_TRADES_TO_CALIBRATE:
            base = self._from_static(self.base_params)
            return self._apply_atr_scaling(base, current_atr_pct)

        # Blend: peso del adaptativo crece con la cantidad de trades
        alpha = min(1.0, (n - self.MIN_TRADES_TO_CALIBRATE) /
                          max(self.WINDOW_SIZE - self.MIN_TRADES_TO_CALIBRATE, 1))

        if alpha >= 1.0:
            return self._apply_atr_scaling(self.current, current_atr_pct)

        static = self._from_static(self.base_params)
        blended = AdaptiveParams(
            stop_loss_pct=self._lerp(static.stop_loss_pct, self.current.stop_loss_pct, alpha),
            partial_tp_mfe_pct=self._lerp(static.partial_tp_mfe_pct, self.current.partial_tp_mfe_pct, alpha),
            partial_tp_fraction=self._lerp(static.partial_tp_fraction, self.current.partial_tp_fraction, alpha),
            profit_lock_pct=self._lerp(static.profit_lock_pct, self.current.profit_lock_pct, alpha),
            breakeven_trigger_pct=self._lerp(static.breakeven_trigger_pct, self.current.breakeven_trigger_pct, alpha),
            trailing_activation_pct=self._lerp(static.trailing_activation_pct, self.current.trailing_activation_pct, alpha),
            trailing_callback_pct=self._lerp(static.trailing_callback_pct, self.current.trailing_callback_pct, alpha),
            early_abort_hours=self._lerp(static.early_abort_hours, self.current.early_abort_hours, alpha),
            early_abort_max_mfe=self._lerp(static.early_abort_max_mfe, self.current.early_abort_max_mfe, alpha),
            early_abort_max_loss=self._lerp(static.early_abort_max_loss, self.current.early_abort_max_loss, alpha),
            calibration_n=self.current.calibration_n,
            calibration_time=self.current.calibration_time,
        )
        return self._apply_atr_scaling(blended, current_atr_pct)

    # ──────────────────────────────────────────────────────────
    #  Regime shift detection
    # ──────────────────────────────────────────────────────────

    def _detect_regime_shift(self, trades: list) -> list:
        """
        Split-window Z-test on win rate + mean PnL.
        If a significant shift is detected, return only the recent
        (post-shift) trades so calibration adapts faster.

        With small N the test has low power — this is intentional:
        we only react to *large* regime changes to avoid noise.
        """
        n = len(trades)
        if n < self.MIN_TRADES_TO_CALIBRATE + 4:
            return trades          # not enough data to split

        mid = n // 2
        first, second = trades[:mid], trades[mid:]

        # ── Win rate Z-test ──
        w1 = sum(1 for t in first if t.is_winner)
        w2 = sum(1 for t in second if t.is_winner)
        n1, n2 = len(first), len(second)
        p_pool = (w1 + w2) / n
        se = (p_pool * (1 - p_pool) * (1/n1 + 1/n2)) ** 0.5 if 0 < p_pool < 1 else 0

        wr_shift = False
        if se > 0:
            z_wr = (w2/n2 - w1/n1) / se
            if abs(z_wr) > 1.96:                   # 95% two-sided
                wr_shift = True

        # ── Mean PnL comparison (Welch's t-like) ──
        pnl1 = [t.pnl_pct for t in first]
        pnl2 = [t.pnl_pct for t in second]
        m1 = sum(pnl1) / n1
        m2 = sum(pnl2) / n2
        v1 = sum((x - m1)**2 for x in pnl1) / max(n1 - 1, 1)
        v2 = sum((x - m2)**2 for x in pnl2) / max(n2 - 1, 1)
        se_pnl = (v1/n1 + v2/n2) ** 0.5 if (v1 + v2) > 0 else 0

        pnl_shift = False
        if se_pnl > 0:
            z_pnl = (m2 - m1) / se_pnl
            if abs(z_pnl) > 1.96:
                pnl_shift = True

        if wr_shift or pnl_shift:
            # Ensure enough data post-shift for valid calibration
            w_recent = sum(1 for t in second if t.is_winner)
            l_recent = sum(1 for t in second if not t.is_winner)
            if w_recent >= 5 and l_recent >= 3:
                log.warning(
                    f"⚠️  AEPS REGIME SHIFT detected | "
                    f"WR: {w1/n1:.0%}→{w2/n2:.0%} "
                    f"PnL: {m1:+.2%}→{m2:+.2%} | "
                    f"using last {n2} trades only"
                )
                return second
            else:
                log.info(
                    f"AEPS regime shift signal but insufficient recent data "
                    f"(W={w_recent}, L={l_recent}), using full window"
                )

        return trades

    # ──────────────────────────────────────────────────────────
    #  CORE: Recalibración basada en distribución empírica
    # ──────────────────────────────────────────────────────────

    def _recalibrate(self):
        """
        Recalcula todos los parámetros de salida usando la
        distribución empírica de MFE/MAE/ETD de la rolling window.

        Fórmulas:
          stop_loss       = P75(|MAE| losers) + 0.5%
          partial_tp_mfe  = P25(MFE winners)
          profit_lock     = partial_tp × 0.5
          breakeven_trig  = partial_tp × 1.2
          trailing_activ  = P50(MFE winners)
          trailing_cb     = median(ETD/MFE) winners
          early_abort_h   = P75(time_to_mfe losers) → horas
          early_abort_mfe = P90(MFE losers)
          early_abort_loss= −P50(|MAE| losers) × 0.5
        """
        trades = list(self.history)
        trades = self._detect_regime_shift(trades)   # may shrink window
        winners = [t for t in trades if t.is_winner]
        losers = [t for t in trades if not t.is_winner]
        # Exclude aborted trades from SL/trailing calibration to prevent
        # feedback loop: aborted trades have truncated MAE/hold, which would
        # shrink SL and abort_hours progressively (degeneracy).
        natural_losers = [t for t in losers if t.exit_reason != "early_abort"]

        if len(winners) < 5 or len(losers) < 3:
            log.info(
                f"AEPS: no recalibra — insuficientes trades "
                f"(W={len(winners)}, L={len(losers)})"
            )
            return

        # ── Extraer distribuciones ──
        w_mfe = sorted(t.mfe_pct for t in winners)
        # Use natural losers for MAE (aborted MAE is truncated)
        nl_mae = sorted(abs(t.mae_pct) for t in natural_losers) if natural_losers else sorted(abs(t.mae_pct) for t in losers)
        # Use ALL losers for MFE (MFE at abort time is real, not truncated)
        l_mfe = sorted(t.mfe_pct for t in losers)
        # Use natural losers for time_to_mfe (aborted ttm is truncated)
        l_ttm = sorted(t.time_to_mfe_secs for t in natural_losers if t.time_to_mfe_secs > 0)

        # Capture ratios de winners (ETD/MFE)
        w_capture = []
        for t in winners:
            if t.mfe_pct > 0.001 and t.exit_reason != "profit_lock":
                w_capture.append(t.etd_pct / t.mfe_pct)

        # ── STOP LOSS ── (uses natural losers to avoid truncated MAE)
        raw_sl = self._pctl(nl_mae, 75) + 0.005
        new_sl = self._clamp("stop_loss_pct", raw_sl)

        # ── PARTIAL TP MFE ──
        raw_ptp = self._pctl(w_mfe, 25)
        new_ptp = self._clamp("partial_tp_mfe_pct", raw_ptp)

        # ── PROFIT LOCK ──
        raw_pl = new_ptp * 0.5
        new_pl = self._clamp("profit_lock_pct", raw_pl)

        # ── BREAKEVEN TRIGGER ──
        raw_be = new_ptp * 1.2
        new_be = self._clamp("breakeven_trigger_pct", raw_be)

        # ── TRAILING ACTIVATION ──
        raw_ta = self._pctl(w_mfe, 50)
        new_ta = self._clamp("trailing_activation_pct", raw_ta)

        # ── TRAILING CALLBACK ── (P25: tighter capture, breaks autocorrelation
        #    with trailing_stop exits that have inflated ETD/MFE)
        if w_capture:
            raw_cb = self._pctl(sorted(w_capture), 25)
        else:
            raw_cb = 0.50
        new_cb = self._clamp("trailing_callback_pct", raw_cb)

        # ── EARLY ABORT HOURS ──
        if l_ttm:
            raw_eah = self._pctl(l_ttm, 75) / 3600.0
        else:
            raw_eah = 2.0
        new_eah = self._clamp("early_abort_hours", raw_eah)

        # ── EARLY ABORT MAX MFE ──
        # P50 of loser MFE: natural separator between dead trades and reversions
        raw_eam = self._pctl(l_mfe, 50)
        new_eam = self._clamp("early_abort_max_mfe", raw_eam)

        # ── EARLY ABORT MAX LOSS ── (deprecated: no longer used in condition)
        new_eal = self.current.early_abort_max_loss  # preserve last value

        # ── PARTIAL TP FRACTION (no cambia con el mercado) ──
        new_ptf = self.current.partial_tp_fraction

        # ── Actualizar ──
        self._calibration_count += 1
        old = self.current
        self.current = AdaptiveParams(
            stop_loss_pct=new_sl,
            partial_tp_mfe_pct=new_ptp,
            partial_tp_fraction=new_ptf,
            profit_lock_pct=new_pl,
            breakeven_trigger_pct=new_be,
            trailing_activation_pct=new_ta,
            trailing_callback_pct=new_cb,
            early_abort_hours=new_eah,
            early_abort_max_mfe=new_eam,
            early_abort_max_loss=new_eal,
            calibration_n=len(trades),
            calibration_time=time.time(),
        )

        log.warning(
            f"🔧 AEPS RECALIBRADO (#{self._calibration_count}) "
            f"| N={len(trades)} (W={len(winners)}/L={len(losers)}) | "
            f"SL: {old.stop_loss_pct:.3f}→{new_sl:.3f} | "
            f"PTP: {old.partial_tp_mfe_pct:.3f}→{new_ptp:.3f} | "
            f"BE: {old.breakeven_trigger_pct:.3f}→{new_be:.3f} | "
            f"TRAIL_ACT: {old.trailing_activation_pct:.3f}→{new_ta:.3f} | "
            f"TRAIL_CB: {old.trailing_callback_pct:.2f}→{new_cb:.2f} | "
            f"ABORT_H: {old.early_abort_hours:.1f}→{new_eah:.1f} | "
            f"ABORT_MFE: {old.early_abort_max_mfe:.4f}→{new_eam:.4f}"
        )

    # ──────────────────────────────────────────────────────────
    #  ATR scaling
    # ──────────────────────────────────────────────────────────

    def _apply_atr_scaling(self, params: AdaptiveParams,
                           current_atr_pct: float) -> AdaptiveParams:
        """Scale selected params by ATR ratio vs historical median."""
        if current_atr_pct <= 0 or not self.history:
            return params

        atrs = [t.atr_at_entry for t in self.history if t.atr_at_entry > 0]
        if not atrs:
            return params

        median_atr = sorted(atrs)[len(atrs) // 2]
        if median_atr <= 0:
            return params

        scale = max(0.5, min(2.0, current_atr_pct / median_atr))

        return AdaptiveParams(
            stop_loss_pct=self._clamp("stop_loss_pct",
                                      params.stop_loss_pct * scale),
            partial_tp_mfe_pct=self._clamp("partial_tp_mfe_pct",
                                           params.partial_tp_mfe_pct * scale),
            partial_tp_fraction=params.partial_tp_fraction,
            profit_lock_pct=self._clamp("profit_lock_pct",
                                        params.profit_lock_pct * scale),
            breakeven_trigger_pct=self._clamp("breakeven_trigger_pct",
                                              params.breakeven_trigger_pct * scale),
            trailing_activation_pct=self._clamp("trailing_activation_pct",
                                                params.trailing_activation_pct * scale),
            trailing_callback_pct=params.trailing_callback_pct,
            early_abort_hours=params.early_abort_hours,
            early_abort_max_mfe=params.early_abort_max_mfe,  # no ATR scaling: "dead trade" is signal quality, not vol
            early_abort_max_loss=params.early_abort_max_loss,  # deprecated, no scaling
            calibration_n=params.calibration_n,
            calibration_time=params.calibration_time,
        )

    # ──────────────────────────────────────────────────────────
    #  Helpers
    # ──────────────────────────────────────────────────────────

    @staticmethod
    def _pctl(arr: list, p: float) -> float:
        """Percentil p (0-100) de array ya ordenado."""
        if not arr:
            return 0.0
        k = (len(arr) - 1) * p / 100.0
        f = int(k)
        c = min(f + 1, len(arr) - 1)
        d = k - f
        return arr[f] + d * (arr[c] - arr[f])

    @staticmethod
    def _lerp(a: float, b: float, t: float) -> float:
        """Interpolación lineal: a*(1-t) + b*t."""
        return a * (1.0 - t) + b * t

    def _clamp(self, param: str, value: float) -> float:
        """Aplica guardrails al valor."""
        lo, hi = self.BOUNDS[param]
        return max(lo, min(hi, value))

    # ──────────────────────────────────────────────────────────
    #  Persistencia y restauración
    # ──────────────────────────────────────────────────────────

    def to_dict(self) -> dict:
        """Serializa el estado completo para guardar en JSON."""
        return {
            "current_params": self.current.__dict__,
            "history": [
                {
                    "mfe_pct": t.mfe_pct,
                    "mae_pct": t.mae_pct,
                    "etd_pct": t.etd_pct,
                    "pnl_pct": t.pnl_pct,
                    "atr_at_entry": t.atr_at_entry,
                    "time_to_mfe_secs": t.time_to_mfe_secs,
                    "hold_secs": t.hold_secs,
                    "exit_reason": t.exit_reason,
                    "score_at_entry": t.score_at_entry,
                    "is_winner": t.is_winner,
                }
                for t in self.history
            ],
            "calibration_count": self._calibration_count,
        }

    @classmethod
    def from_dict(cls, data: dict, base_params: dict) -> "AdaptiveExitCalibrator":
        """Restaura desde JSON."""
        cal = cls(base_params)
        for t in data.get("history", []):
            cal.history.append(TradeRecord(**t))
        cal._calibration_count = data.get("calibration_count", 0)
        p = data.get("current_params", {})
        if p:
            cal.current = AdaptiveParams(**p)
        return cal

    # ──────────────────────────────────────────────────────────
    #  Diagnóstico
    # ──────────────────────────────────────────────────────────

    def status(self) -> str:
        """Resumen de una línea para logging periódico."""
        n = len(self.history)
        if n == 0:
            return "AEPS: sin datos (usando estáticos)"
        w = sum(1 for t in self.history if t.is_winner)
        return (
            f"AEPS: N={n} (W={w}/L={n - w}) | "
            f"SL={self.current.stop_loss_pct:.1%} "
            f"PTP={self.current.partial_tp_mfe_pct:.1%} "
            f"BE={self.current.breakeven_trigger_pct:.1%} "
            f"TRAIL={self.current.trailing_activation_pct:.1%}/"
            f"{self.current.trailing_callback_pct:.0%} "
            f"ABORT={self.current.early_abort_hours:.1f}h/"
            f"{self.current.early_abort_max_mfe:.2%} "
            f"| cal#{self._calibration_count}"
        )
