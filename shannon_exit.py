"""
Shannon-VOI Exit System — Information-theoretic exit for aggressive variant.

Replaces AEPS for the aggressive variant only. Uses a pre-calibrated
P(win | t, MFE) surface to decide EXIT / TRAIL / HOLD via Value-of-Information.

Design:
  - Each open trade is tracked with a running (t, mfe, mae, pnl) state.
  - On each tick, the surface is queried to get P(win).
  - When P(win) < exit_pw → EXIT immediately.
  - When P(win) > trail_pw → activate trailing stop (tight callback).
  - Between exit_pw and trail_pw → HOLD (information still accruing).
  - info_rate = dP(win)/dt. If info_rate < eps for min_observe_sec, EXIT.
  - hard_sl is a static safety net (never overridden by the surface).

Surface format (pwin_surface.json):
  [[t_min, mfe_pct, p_win], ...]
  Queried with bilinear interpolation on (t, mfe) grid.

Refs:
  - Shannon (1948), "A Mathematical Theory of Communication"
  - Cover & Thomas (2006), "Elements of Information Theory"
"""

import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger("shannon_exit")


@dataclass
class ShannonTradeState:
    """Per-trade state tracked by the Shannon exit system."""
    trade_id: str
    symbol: str
    open_time: float
    entry_price: float
    # Running state
    last_pw: float = 0.5
    last_pw_time: float = 0.0
    trailing_active: bool = False
    trailing_peak: float = 0.0
    action_history: list = field(default_factory=list)


class ShannonExit:
    """
    Shannon-VOI exit engine.

    Usage:
        se = ShannonExit(surface_path, config)
        se.open(trade_id, symbol, now, entry_price)
        action = se.tick(trade_id, now, pnl_pct, mfe_pct, mae_pct)
        # action in ("EXIT", "TRAIL", "HOLD")
        se.close(trade_id, pnl_pct, reason)
    """

    def __init__(self, surface_path: str, config: dict):
        self.surface_path = surface_path
        self.config = config

        # Config params with defaults
        self.prior_win = config.get("prior_win", 0.566)
        self.exit_pw = config.get("exit_pw", 0.50)
        self.trail_pw = config.get("trail_pw", 0.70)
        self.info_rate_eps = config.get("info_rate_eps", 0.005)
        self.min_observe_sec = config.get("min_observe_sec", 60)
        self.hard_sl = config.get("hard_sl", 0.08)
        self.trailing_callback = config.get("trailing_callback", 0.30)
        self.avg_pnl_w = config.get("avg_pnl_w", 0.0254)
        self.avg_pnl_l = config.get("avg_pnl_l", -0.0308)

        # Output directory
        self.output_dir = config.get("output_dir", "./logs/shannon")
        os.makedirs(self.output_dir, exist_ok=True)

        # Surface: list of (t_min, mfe_pct, p_win)
        self.surface = []
        self.t_grid = []
        self.m_grid = []
        self._load_surface()

        # Active trades
        self.trades: dict[str, ShannonTradeState] = {}

        log.info(
            f"ShannonExit initialized | surface={len(self.surface)} points | "
            f"exit_pw={self.exit_pw} trail_pw={self.trail_pw} | "
            f"hard_sl={self.hard_sl:.1%}"
        )

    # ──────────────────────────────────────────────────────────
    #  Surface I/O
    # ──────────────────────────────────────────────────────────

    def _load_surface(self):
        """Load P(win) surface from JSON file."""
        if not os.path.exists(self.surface_path):
            log.warning(
                f"ShannonExit: surface file not found: {self.surface_path} — "
                f"using flat prior {self.prior_win:.3f}"
            )
            return

        try:
            with open(self.surface_path) as f:
                raw = json.load(f)
            self.surface = [(r[0], r[1], r[2]) for r in raw]
            self.t_grid = sorted(set(r[0] for r in self.surface))
            self.m_grid = sorted(set(r[1] for r in self.surface))
            # Build lookup dict for fast access
            self._surf_dict = {(r[0], r[1]): r[2] for r in self.surface}
            log.info(
                f"ShannonExit: loaded surface | "
                f"t_grid={self.t_grid} | m_grid={self.m_grid} | "
                f"{len(self.surface)} cells"
            )
        except Exception as e:
            log.error(f"ShannonExit: failed to load surface: {e}")

    def reload_surface(self, path: Optional[str] = None):
        """Hot-reload surface from disk (called after recalibration)."""
        if path:
            self.surface_path = path
        self._load_surface()

    def _query_pw(self, t_min: float, mfe_pct: float) -> float:
        """
        Query P(win | t, MFE) with bilinear interpolation.
        Falls back to prior_win if surface is empty.
        """
        if not self.t_grid or not self.m_grid:
            return self.prior_win

        # Clamp to grid bounds
        t = max(self.t_grid[0], min(self.t_grid[-1], t_min))
        m = max(self.m_grid[0], min(self.m_grid[-1], mfe_pct))

        # Find surrounding grid points
        t_lo = self.t_grid[0]
        t_hi = self.t_grid[-1]
        for i in range(len(self.t_grid) - 1):
            if self.t_grid[i] <= t <= self.t_grid[i + 1]:
                t_lo = self.t_grid[i]
                t_hi = self.t_grid[i + 1]
                break

        m_lo = self.m_grid[0]
        m_hi = self.m_grid[-1]
        for i in range(len(self.m_grid) - 1):
            if self.m_grid[i] <= m <= self.m_grid[i + 1]:
                m_lo = self.m_grid[i]
                m_hi = self.m_grid[i + 1]
                break

        # Get corner values
        def _get(tt, mm):
            return self._surf_dict.get((tt, mm), self.prior_win)

        p00 = _get(t_lo, m_lo)
        p01 = _get(t_lo, m_hi)
        p10 = _get(t_hi, m_lo)
        p11 = _get(t_hi, m_hi)

        # Bilinear interpolation
        dt = (t - t_lo) / (t_hi - t_lo) if t_hi != t_lo else 0.0
        dm = (m - m_lo) / (m_hi - m_lo) if m_hi != m_lo else 0.0

        p0 = p00 * (1 - dm) + p01 * dm
        p1 = p10 * (1 - dm) + p11 * dm
        pw = p0 * (1 - dt) + p1 * dt

        return max(0.0, min(1.0, pw))

    # ──────────────────────────────────────────────────────────
    #  Trade lifecycle
    # ──────────────────────────────────────────────────────────

    def open(self, trade_id: str, symbol: str, now: float,
             entry_price: float):
        """Register a new trade."""
        self.trades[trade_id] = ShannonTradeState(
            trade_id=trade_id,
            symbol=symbol,
            open_time=now,
            entry_price=entry_price,
            last_pw=self.prior_win,
            last_pw_time=now,
        )
        log.info(
            f"Shannon OPEN | {symbol} #{trade_id} "
            f"@ ${entry_price:.6f} | π_W={self.prior_win:.3f}"
        )

    def tick(self, trade_id: str, now: float,
             pnl_pct: float, mfe_pct: float, mae_pct: float) -> str:
        """
        Evaluate exit decision for one tick.

        Returns: "EXIT", "TRAIL", or "HOLD"
        """
        ts = self.trades.get(trade_id)
        if ts is None:
            return "HOLD"

        elapsed_sec = now - ts.open_time
        t_min = elapsed_sec / 60.0

        # Query surface
        pw = self._query_pw(t_min, mfe_pct * 100)  # surface uses pct (0-20)

        # Info rate: dP(win)/dt
        dt = now - ts.last_pw_time if ts.last_pw_time > 0 else 1.0
        info_rate = abs(pw - ts.last_pw) / max(dt, 1.0) if dt > 0 else 0.0

        # Update state
        ts.last_pw = pw
        ts.last_pw_time = now

        # ── Hard stop loss (unconditional) ──
        if pnl_pct <= -self.hard_sl:
            return "EXIT"

        # ── Shannon decision ──
        action = "HOLD"

        if pw < self.exit_pw:
            # Low probability of winning → exit
            if elapsed_sec >= self.min_observe_sec:
                action = "EXIT"

        elif pw >= self.trail_pw:
            # High probability → activate trail to capture
            if not ts.trailing_active:
                ts.trailing_active = True
                ts.trailing_peak = pnl_pct
                log.info(
                    f"Shannon TRAIL activated | {ts.symbol} #{trade_id} "
                    f"| P(win)={pw:.3f} MFE={mfe_pct:.2%}"
                )
            action = "TRAIL"

        else:
            # Between exit_pw and trail_pw → check info rate stagnation
            if (elapsed_sec >= self.min_observe_sec
                    and info_rate < self.info_rate_eps
                    and elapsed_sec > 300):  # at least 5min before stagnation exit
                action = "EXIT"

        # ── Trailing stop logic ──
        if ts.trailing_active:
            if pnl_pct > ts.trailing_peak:
                ts.trailing_peak = pnl_pct
            trail_floor = ts.trailing_peak * (1.0 - self.trailing_callback)
            if pnl_pct < trail_floor and ts.trailing_peak > 0:
                action = "EXIT"

        # Record action
        ts.action_history.append((now, pw, action, pnl_pct, mfe_pct))

        return action

    def close(self, trade_id: str, pnl_pct: float, reason: str):
        """
        Record trade close and remove from active trades.
        Appends to trade log.
        """
        ts = self.trades.pop(trade_id, None)
        if ts is None:
            return

        elapsed = time.time() - ts.open_time
        is_winner = pnl_pct > 0

        record = {
            "trade_id": trade_id,
            "symbol": ts.symbol,
            "entry_price": ts.entry_price,
            "open_time": ts.open_time,
            "close_time": time.time(),
            "elapsed_sec": elapsed,
            "pnl_pct": pnl_pct,
            "reason": reason,
            "is_winner": is_winner,
            "final_pw": ts.last_pw,
            "trailing_used": ts.trailing_active,
            "trailing_peak": ts.trailing_peak,
            "n_ticks": len(ts.action_history),
        }

        # Append to trades log
        log_path = os.path.join(self.output_dir, "trades.jsonl")
        try:
            with open(log_path, "a") as f:
                f.write(json.dumps(record) + "\n")
        except Exception as e:
            log.warning(f"Shannon: failed to write trade log: {e}")

        # Append detailed path for surface recalibration
        detail_path = os.path.join(self.output_dir, "trade_details.jsonl")
        try:
            # Save sampled path (every 10th point to keep file reasonable)
            sampled = ts.action_history[::10]
            if ts.action_history and ts.action_history[-1] not in sampled:
                sampled.append(ts.action_history[-1])
            detail = {
                "trade_id": trade_id,
                "symbol": ts.symbol,
                "is_winner": is_winner,
                "pnl_pct": pnl_pct,
                "path": [
                    {"t": p[0] - ts.open_time, "pw": p[1],
                     "action": p[2], "pnl": p[3], "mfe": p[4]}
                    for p in sampled
                ],
            }
            with open(detail_path, "a") as f:
                f.write(json.dumps(detail) + "\n")
        except Exception as e:
            log.warning(f"Shannon: failed to write detail log: {e}")

        emoji = "🟢" if is_winner else "🔻"
        log.warning(
            f"{emoji} Shannon CLOSE | {ts.symbol} #{trade_id} "
            f"| reason={reason} PnL={pnl_pct:+.2%} "
            f"| π_W={ts.last_pw:.3f} trail={ts.trailing_active} "
            f"| hold={elapsed/60:.1f}min | ticks={len(ts.action_history)}"
        )

    # ──────────────────────────────────────────────────────────
    #  Status
    # ──────────────────────────────────────────────────────────

    def status(self) -> dict:
        """Return current status for API/dashboard."""
        active = {}
        for tid, ts in self.trades.items():
            active[tid] = {
                "symbol": ts.symbol,
                "elapsed_min": (time.time() - ts.open_time) / 60,
                "last_pw": ts.last_pw,
                "trailing": ts.trailing_active,
                "trailing_peak": ts.trailing_peak,
            }

        # Read trade history stats
        stats = self._compute_stats()

        return {
            "type": "shannon",
            "active_trades": active,
            "surface_loaded": len(self.surface) > 0,
            "surface_cells": len(self.surface),
            "config": {
                "prior_win": self.prior_win,
                "exit_pw": self.exit_pw,
                "trail_pw": self.trail_pw,
                "info_rate_eps": self.info_rate_eps,
                "hard_sl": self.hard_sl,
                "trailing_callback": self.trailing_callback,
            },
            "stats": stats,
        }

    def _compute_stats(self) -> dict:
        """Compute stats from trades.jsonl."""
        log_path = os.path.join(self.output_dir, "trades.jsonl")
        if not os.path.exists(log_path):
            return {"total": 0}

        trades = []
        try:
            with open(log_path) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        trades.append(json.loads(line))
        except Exception:
            return {"total": 0}

        if not trades:
            return {"total": 0}

        winners = [t for t in trades if t.get("is_winner")]
        losers = [t for t in trades if not t.get("is_winner")]
        cum_pnl = sum(t.get("pnl_pct", 0) for t in trades)

        # Breakdown by reason
        by_reason = {}
        for t in trades:
            r = t.get("reason", "unknown")
            if r not in by_reason:
                by_reason[r] = {"count": 0, "wins": 0, "pnl": 0.0}
            by_reason[r]["count"] += 1
            if t.get("is_winner"):
                by_reason[r]["wins"] += 1
            by_reason[r]["pnl"] += t.get("pnl_pct", 0)

        return {
            "total": len(trades),
            "winners": len(winners),
            "losers": len(losers),
            "win_rate": len(winners) / len(trades) if trades else 0,
            "cum_pnl_pct": cum_pnl,
            "avg_pnl_w": (sum(t["pnl_pct"] for t in winners) / len(winners))
                         if winners else 0,
            "avg_pnl_l": (sum(t["pnl_pct"] for t in losers) / len(losers))
                         if losers else 0,
            "by_reason": by_reason,
            "last_trade": trades[-1] if trades else None,
        }
