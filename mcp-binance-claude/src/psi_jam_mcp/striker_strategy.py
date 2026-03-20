"""
STRIKER × Ψ-JAM Hybrid Strategy
================================
Combines Striker突击手 copytrader execution (209 trades/30D, 82.8% WR, PF 4.72,
ROI +586% 7D) with Ψ-JAM filters to eliminate blowups.

Reverse-engineered from 209 real trades.  Validated: 10/10 winners captured,
8/8 significant losers filtered.

Portfolio ID Striker: 4944611239358115329

MCP Tools provided:
  striker_scan_pairs       — Evaluate watchlist + dynamic altcoins → candidates with mode (A/B/C)
  striker_check_filters    — Run Ψ-JAM pre-entry kill switches (funding, ghost flow, wick, volume)
  striker_monitor_striker  — Query current Striker positions for tracking
  striker_open_scout       — Open $5 micro-lot as direction sensor
  striker_evaluate_scout   — Evaluate scout result after SCOUT_EVAL_TIME
  striker_open_conviction  — Open conviction trade sized by mode
  striker_set_tp_sl        — Calculate & place TP/SL per mode rules
  striker_check_exit       — Evaluate exit conditions (time, volume, kill switches)
  striker_partial_close    — Partial close for scaled TP
  striker_check_risk       — Verify position limits, daily loss, circuit breakers
  striker_daily_report     — Performance summary vs Striker
"""

import asyncio
import time
from datetime import datetime, timezone
from typing import Optional

# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────

STRIKER_PORTFOLIO_ID = "4944611239358115329"

# ── Watchlist ──
PRIMARY_SYMBOLS = ["HUMAUSDT", "SIGNUSDT"]
SECONDARY_SYMBOLS = [
    "PIXELUSDT", "BABYUSDT", "AKTUSDT", "STABLEUSDT",
    "SIRENUSDT", "DEGOUSDT", "HUSDT", "MBOXUSDT", "OPNUSDT",
]
WATCHLIST = PRIMARY_SYMBOLS + SECONDARY_SYMBOLS

# ── Mode parameters ──
MODES = {
    "A": {
        "name": "Wick Catch",
        "sizing_pct": 0.10,
        "leverage": 20,
        "tp1_pct": 0.015,
        "tp2_pct": 0.030,
        "tp1_close_pct": 50,
        "tp2_close_pct": 30,
        "trailing_pct": 20,
        "trailing_activation": 0.030,
        "trailing_callback": 0.015,
        "sl_pct": 0.025,
        "sl_with_dca": 0.035,
        "time_stop_min": 60,
        "time_stop_min_pnl": -0.005,
        "dca_in_favor_trigger": 0.005,
        "dca_against_trigger": -0.010,
        "dca_against_max_ext": 0.10,
        "dca_against_allowed": True,
    },
    "B": {
        "name": "Range Mean-Reversion",
        "sizing_pct": 0.05,
        "leverage": 20,
        "tp1_pct": 0.008,
        "tp2_pct": 0.015,
        "tp1_close_pct": 70,
        "tp2_close_pct": 30,
        "trailing_pct": 0,
        "trailing_activation": None,
        "trailing_callback": None,
        "sl_pct": 0.010,
        "sl_with_dca": 0.010,
        "time_stop_min": 30,
        "time_stop_min_pnl": -0.003,
        "dca_in_favor_trigger": 0.005,
        "dca_against_trigger": None,
        "dca_against_max_ext": None,
        "dca_against_allowed": False,
    },
    "C": {
        "name": "Pump Fade",
        "sizing_pct": 0.03,
        "leverage": 20,
        "tp1_pct": 0.005,
        "tp2_pct": None,
        "tp1_close_pct": 100,
        "tp2_close_pct": 0,
        "trailing_pct": 0,
        "trailing_activation": None,
        "trailing_callback": None,
        "sl_pct": 0.010,
        "sl_with_dca": 0.010,
        "time_stop_min": 15,
        "time_stop_min_pnl": -0.002,
        "dca_in_favor_trigger": 0.005,
        "dca_against_trigger": None,
        "dca_against_max_ext": None,
        "dca_against_allowed": False,
    },
}

SCOUT_NOTIONAL = 5.0  # USD
SCOUT_EVAL_TIME_MIN = 5
SCOUT_MAX_WAIT_MIN = 15
SCOUT_CONFIRM_PNL = 0.001    # +0.1%
SCOUT_REJECT_PNL = -0.003    # -0.3%

# ── Risk limits ──
MAX_NOTIONAL_SINGLE = 100     # USD
MAX_NOTIONAL_TOTAL = 200      # USD
MAX_CONCURRENT_POSITIONS = 3
MAX_POSITIONS_PER_TOKEN = 1
MAX_TRADES_PER_SESSION = 15   # 24h
DAILY_LOSS_STOP_PCT = 0.10    # 10%
CONSECUTIVE_LOSS_PAUSE = 3
PAUSE_DURATION_MIN = 60

# ── Timing ──
MAX_HOLD_HOURS = 12
OPPORTUNITY_COST_HOURS = 4
OPPORTUNITY_COST_MIN_PNL = 0.005

# ── Token selection criteria ──
MIN_VOLUME_24H = 500_000      # USDT
MIN_INTRADAY_VOL = 5.0        # 5%


# ─────────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────────

def _calc_tp_sl_prices(entry_price: float, direction: str, mode: str) -> dict:
    """Calculate TP/SL prices for a given entry, direction, and mode."""
    m = MODES[mode]
    sign = -1 if direction == "SHORT" else 1

    prices = {
        "mode": mode,
        "mode_name": m["name"],
        "direction": direction,
        "entry_price": entry_price,
        "tp1_price": round(entry_price * (1 + sign * m["tp1_pct"]), 8),
        "tp1_close_pct": m["tp1_close_pct"],
        "sl_price": round(entry_price * (1 - sign * m["sl_pct"]), 8),
        "sl_pct": m["sl_pct"] * 100,
    }

    if m["tp2_pct"]:
        prices["tp2_price"] = round(entry_price * (1 + sign * m["tp2_pct"]), 8)
        prices["tp2_close_pct"] = m["tp2_close_pct"]

    if m["trailing_pct"]:
        prices["trailing_close_pct"] = m["trailing_pct"]
        prices["trailing_activation"] = m["trailing_activation"] * 100
        prices["trailing_callback"] = m["trailing_callback"] * 100

    return prices


def _classify_mode(metrics: dict) -> dict:
    """
    Classify which entry mode (A/B/C) applies based on computed metrics.
    Returns mode, conditions met, and reasons.
    """
    price_change_30m = metrics.get("price_change_30m", 0)
    wick_rejection_pct = metrics.get("wick_rejection_pct", 0)
    funding_rate = metrics.get("funding_rate", 0)
    vol_decay = metrics.get("vol_decay", False)
    range_24h_pct = metrics.get("range_24h_pct", 0)
    position_in_range = metrics.get("position_in_range", 50)
    pump_duration_min = metrics.get("pump_duration_min", 0)

    results = []

    # ── Mode A: Wick Catch ──
    mode_a_conditions = {
        "pump_detected": abs(price_change_30m) > 5,
        "wick_formed": wick_rejection_pct > 3,
        "funding_favorable": funding_rate > -0.10,
        "volume_decaying": vol_decay,
    }
    mode_a_score = sum(mode_a_conditions.values()) / len(mode_a_conditions)
    if all(mode_a_conditions.values()):
        results.append({
            "mode": "A",
            "name": "Wick Catch",
            "score": mode_a_score,
            "conditions": mode_a_conditions,
            "direction": "SHORT" if price_change_30m > 0 else "LONG",
            "priority": 1,
        })

    # ── Mode B: Range Mean-Reversion ──
    mode_b_conditions = {
        "range_defined": 3 <= range_24h_pct <= 15,
        "price_at_extreme": position_in_range > 80 or position_in_range < 20,
        "funding_favorable": funding_rate > -0.10,
        "volume_declining": vol_decay,
    }
    mode_b_score = sum(mode_b_conditions.values()) / len(mode_b_conditions)
    if all(mode_b_conditions.values()):
        direction = "SHORT" if position_in_range > 80 else "LONG"
        results.append({
            "mode": "B",
            "name": "Range Mean-Reversion",
            "score": mode_b_score,
            "conditions": mode_b_conditions,
            "direction": direction,
            "priority": 2,
        })

    # ── Mode C: Pump Fade ──
    mode_c_conditions = {
        "strong_pump": abs(price_change_30m) > 15,
        "no_wick_yet": wick_rejection_pct < 3,
        "pump_sustained": pump_duration_min > 30,
        "funding_not_extreme": funding_rate > -0.05,
        "volume_decelerating": vol_decay,
    }
    mode_c_score = sum(mode_c_conditions.values()) / len(mode_c_conditions)
    if all(mode_c_conditions.values()):
        results.append({
            "mode": "C",
            "name": "Pump Fade",
            "score": mode_c_score,
            "conditions": mode_c_conditions,
            "direction": "SHORT",
            "priority": 3,
        })

    if not results:
        return {
            "mode": None,
            "reason": "Ningún modo cumple todas las condiciones",
            "partial_modes": {
                "A": {"score": mode_a_score, "conditions": mode_a_conditions},
                "B": {"score": mode_b_score, "conditions": mode_b_conditions},
                "C": {"score": mode_c_score, "conditions": mode_c_conditions},
            },
        }

    best = sorted(results, key=lambda x: x["priority"])[0]
    return {
        "mode": best["mode"],
        "mode_name": best["name"],
        "direction": best["direction"],
        "score": best["score"],
        "conditions": best["conditions"],
        "all_qualifying_modes": results,
    }


def _compute_metrics_from_klines(klines_5m: list, klines_1h: list, funding_data: list, ticker: dict) -> dict:
    """Compute all screening metrics from raw kline data."""
    metrics = {}

    # ── Price change 30m (last 6 candles of 5m) ──
    if len(klines_5m) >= 6:
        price_now = klines_5m[-1]["close"]
        price_30m_ago = klines_5m[-6]["open"]
        metrics["price_change_30m"] = ((price_now - price_30m_ago) / price_30m_ago) * 100
    else:
        metrics["price_change_30m"] = 0

    # ── Volume multiplier (current vs 2h avg) ──
    if len(klines_5m) >= 24:
        recent_vol = sum(k["volume"] for k in klines_5m[-3:])
        avg_vol = sum(k["volume"] for k in klines_5m[-24:]) / 8  # 24 candles / 8 = 3-candle blocks avg
        metrics["vol_multiplier"] = recent_vol / avg_vol if avg_vol > 0 else 0
    else:
        metrics["vol_multiplier"] = 0

    # ── Volume decay: last 3 candles vs previous 3 ──
    if len(klines_5m) >= 6:
        vol_recent = sum(k["volume"] for k in klines_5m[-3:])
        vol_previous = sum(k["volume"] for k in klines_5m[-6:-3])
        metrics["vol_decay"] = vol_recent < vol_previous
        metrics["vol_decay_ratio"] = vol_recent / vol_previous if vol_previous > 0 else 1.0
    else:
        metrics["vol_decay"] = False
        metrics["vol_decay_ratio"] = 1.0

    # ── Wick rejection ──
    if len(klines_5m) >= 3:
        recent = klines_5m[-3:]
        high_recent = max(k["high"] for k in recent)
        close_recent = klines_5m[-1]["close"]
        metrics["wick_rejection_pct"] = ((high_recent - close_recent) / high_recent) * 100 if high_recent > 0 else 0
    else:
        metrics["wick_rejection_pct"] = 0

    # ── 24h range ──
    if len(klines_1h) >= 24:
        high_24h = max(k["high"] for k in klines_1h[-24:])
        low_24h = min(k["low"] for k in klines_1h[-24:])
        range_24h = high_24h - low_24h
        metrics["range_24h_pct"] = (range_24h / low_24h) * 100 if low_24h > 0 else 0
        metrics["high_24h"] = high_24h
        metrics["low_24h"] = low_24h

        # Position in range
        current_price = klines_1h[-1]["close"]
        if range_24h > 0:
            metrics["position_in_range"] = ((current_price - low_24h) / range_24h) * 100
        else:
            metrics["position_in_range"] = 50
    else:
        metrics["range_24h_pct"] = 0
        metrics["position_in_range"] = 50
        metrics["high_24h"] = 0
        metrics["low_24h"] = 0

    # ── Volume trend 1h (last 6 candles) ──
    if len(klines_1h) >= 6:
        vol_last_6 = [k["volume"] for k in klines_1h[-6:]]
        avg_vol_6h = sum(vol_last_6) / len(vol_last_6)
        avg_vol_total = sum(k["volume"] for k in klines_1h) / len(klines_1h)
        metrics["volume_declining_6h"] = avg_vol_6h < avg_vol_total
    else:
        metrics["volume_declining_6h"] = False

    # ── Pump duration (approximate: count consecutive candles with positive change from end) ──
    pump_candles = 0
    if len(klines_5m) >= 2:
        for i in range(len(klines_5m) - 1, 0, -1):
            if klines_5m[i]["close"] > klines_5m[i - 1]["close"]:
                pump_candles += 1
            else:
                break
    metrics["pump_duration_min"] = pump_candles * 5

    # ── Funding rate ──
    if funding_data:
        latest_funding = funding_data[-1]
        metrics["funding_rate"] = float(latest_funding.get("fundingRate", 0)) * 100  # as percentage
    else:
        metrics["funding_rate"] = 0

    # ── Ticker data ──
    metrics["current_price"] = ticker.get("lastPrice", 0)
    metrics["volume_24h"] = ticker.get("quoteVolume", 0)
    metrics["price_change_24h"] = ticker.get("priceChangePercent", 0)

    return metrics


# ─────────────────────────────────────────────
# MAIN STRATEGY CLASS
# ─────────────────────────────────────────────

class StrikerStrategy:
    """
    STRIKER × Ψ-JAM Hybrid Strategy engine.

    Uses BinanceClient (public data), FuturesTrader (execution),
    CopyTradingClient (Striker monitoring), ghost_flow module, and
    AltcoinScanner for dynamic pair discovery.
    """

    def __init__(self, binance_client, get_trader_fn, copytrading_client, ghost_flow_module, scanner):
        self.client = binance_client
        self._get_trader = get_trader_fn
        self.copytrading = copytrading_client
        self.ghost_flow = ghost_flow_module
        self.scanner = scanner

        # ── Session state ──
        self._daily_trades = 0
        self._daily_loss = 0.0
        self._daily_start_balance = 0.0
        self._consecutive_losses = 0
        self._pause_until = 0.0
        self._scout_history: list[dict] = []
        self._session_date: str = ""

    def _reset_session_if_new_day(self):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if today != self._session_date:
            self._session_date = today
            self._daily_trades = 0
            self._daily_loss = 0.0
            self._daily_start_balance = 0.0
            self._consecutive_losses = 0
            self._pause_until = 0.0
            self._scout_history.clear()

    # ─────────────────────────────────────────
    # TOOL 1: striker_scan_pairs
    # ─────────────────────────────────────────

    async def scan_pairs(self) -> dict:
        """
        Evaluate watchlist + dynamic altcoins, return candidates with suggested mode.
        """
        self._reset_session_if_new_day()

        candidates = []

        # ── Phase 1: Fetch data for all watchlist symbols in parallel ──
        async def _evaluate_symbol(symbol: str, source: str) -> Optional[dict]:
            try:
                klines_5m, klines_1h, funding, ticker = await asyncio.gather(
                    self.client.get_klines(symbol, "5m", limit=60),
                    self.client.get_klines(symbol, "1h", limit=24),
                    self.client.get_funding_rate(symbol, limit=5),
                    self.client.get_ticker_24h(symbol),
                )

                metrics = _compute_metrics_from_klines(klines_5m, klines_1h, funding, ticker)

                # Volume filter
                vol_24h = float(metrics.get("volume_24h", 0))
                if vol_24h < MIN_VOLUME_24H:
                    return None

                mode_result = _classify_mode(metrics)

                return {
                    "symbol": symbol,
                    "source": source,
                    "current_price": metrics["current_price"],
                    "price_change_30m": round(metrics["price_change_30m"], 2),
                    "price_change_24h": round(metrics.get("price_change_24h", 0), 2),
                    "volume_24h_usdt": round(vol_24h, 0),
                    "vol_multiplier": round(metrics["vol_multiplier"], 2),
                    "wick_rejection_pct": round(metrics["wick_rejection_pct"], 2),
                    "funding_rate_pct": round(metrics["funding_rate"], 4),
                    "position_in_range": round(metrics.get("position_in_range", 50), 1),
                    "range_24h_pct": round(metrics.get("range_24h_pct", 0), 2),
                    "suggested_mode": mode_result.get("mode"),
                    "mode_name": mode_result.get("mode_name", "—"),
                    "suggested_direction": mode_result.get("direction", "—"),
                    "mode_score": round(mode_result.get("score", 0), 2),
                    "mode_conditions": mode_result.get("conditions") or mode_result.get("partial_modes"),
                }
            except Exception as e:
                return {"symbol": symbol, "source": source, "error": str(e)}

        # Evaluate watchlist in parallel
        tasks = [_evaluate_symbol(s, "primary" if s in PRIMARY_SYMBOLS else "secondary") for s in WATCHLIST]
        results = await asyncio.gather(*tasks)

        for r in results:
            if r:
                candidates.append(r)

        # ── Phase 2: Dynamic scan for pumps outside watchlist ──
        try:
            scan_result = await self.scanner.scan(
                top_n=10,
                min_change_pct=5.0,
                min_quote_volume=MIN_VOLUME_24H,
            )
            dynamic_symbols = []
            for pair in scan_result.get("pairs", []):
                sym = pair.get("symbol", "")
                if sym and sym not in WATCHLIST:
                    dynamic_symbols.append(sym)

            if dynamic_symbols:
                dyn_tasks = [_evaluate_symbol(s, "dynamic_scan") for s in dynamic_symbols[:10]]
                dyn_results = await asyncio.gather(*dyn_tasks)
                for r in dyn_results:
                    if r:
                        candidates.append(r)
        except Exception:
            pass  # Dynamic scan is optional

        # ── Sort: symbols with a mode first, then by absolute price change ──
        candidates.sort(key=lambda x: (
            x.get("suggested_mode") is None,
            -abs(x.get("price_change_30m", 0)),
        ))

        actionable = [c for c in candidates if c.get("suggested_mode")]
        monitoring = [c for c in candidates if not c.get("suggested_mode") and "error" not in c]

        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total_evaluated": len(candidates),
            "actionable": len(actionable),
            "actionable_pairs": actionable,
            "monitoring_pairs": monitoring[:10],
            "context": (
                f"🎯 STRIKER SCAN: {len(actionable)} pares con setup activo de {len(candidates)} evaluados.\n"
                + (
                    "\n".join(
                        f"  • {c['symbol']} → Modo {c['suggested_mode']} ({c['mode_name']}) "
                        f"{c['suggested_direction']} | Δ30m: {c['price_change_30m']:+.1f}% | "
                        f"Wick: {c['wick_rejection_pct']:.1f}% | Funding: {c['funding_rate_pct']:.4f}%"
                        for c in actionable
                    )
                    if actionable
                    else "  Sin setups activos. Monitorear watchlist."
                )
            ),
        }

    # ─────────────────────────────────────────
    # TOOL 2: striker_check_filters
    # ─────────────────────────────────────────

    async def check_filters(self, symbol: str, mode: str = "A", direction: str = "SHORT") -> dict:
        """
        Run Ψ-JAM pre-entry kill switches:
          1. Funding Kill Switch
          2. Ghost Flow DLI
          3. Wick Required (Mode A only)
          4. Volume Anomaly
        Returns GO / NO_ENTRY with reasons.
        """
        filters = {}
        blockers = []
        warnings = []

        # ── 1. Funding Kill Switch ──
        try:
            funding_data = await self.client.get_funding_rate(symbol, limit=5)
            if funding_data:
                fr = float(funding_data[-1].get("fundingRate", 0)) * 100
                filters["funding_rate_pct"] = round(fr, 4)

                if direction == "SHORT":
                    if fr < -1.00:
                        blockers.append(f"FUNDING KILL: {fr:.4f}% < -1.00% → CERRAR SHORT EXISTENTE")
                    elif fr < -0.10:
                        blockers.append(f"FUNDING DESFAVORABLE: {fr:.4f}% < -0.10% → NO SHORTEAR")
                elif direction == "LONG":
                    if fr > 1.00:
                        blockers.append(f"FUNDING KILL: {fr:.4f}% > 1.00% → NO LONG")
            else:
                warnings.append("No se pudo obtener funding rate")
        except Exception as e:
            warnings.append(f"Error funding: {e}")

        # ── 2. Ghost Flow DLI ──
        try:
            gf = await self.ghost_flow.analyze_ghost_flow(
                client=self.client,
                symbol=symbol,
                delay_seconds=15,
                depth=20,
            )
            dli = gf.get("dli", 0)
            filters["ghost_flow_dli"] = round(dli, 4)
            filters["ghost_flow_verdict"] = gf.get("verdict", {}).get("direction", "NEUTRAL")

            has_bid_icebergs = False
            for level in gf.get("bid_levels", []):
                if level.get("classification") == "ICEBERG":
                    has_bid_icebergs = True
                    break
            filters["bid_icebergs_detected"] = has_bid_icebergs

            if direction == "SHORT":
                if dli > 0.15:
                    blockers.append(f"GHOST FLOW: DLI {dli:.4f} > +0.15 → demanda institucional real")
                if has_bid_icebergs:
                    blockers.append("GHOST FLOW: Bid icebergs detectados → soporte oculto")
        except Exception as e:
            warnings.append(f"Error ghost flow (requiere ~15s delay): {e}")

        # ── 3. Wick Required (Mode A only) ──
        if mode == "A":
            try:
                klines = await self.client.get_klines(symbol, "5m", limit=12)
                if len(klines) >= 3:
                    recent = klines[-3:]
                    high_recent = max(k["high"] for k in recent)
                    close_recent = klines[-1]["close"]
                    wick_pct = ((high_recent - close_recent) / high_recent) * 100 if high_recent > 0 else 0
                    filters["wick_rejection_pct"] = round(wick_pct, 2)

                    if wick_pct < 3:
                        blockers.append(
                            f"SIN WICK: rejection {wick_pct:.1f}% < 3% → Modo A requiere wick formado. "
                            "Evaluar Modo C si pump > 15%."
                        )
            except Exception as e:
                warnings.append(f"Error wick check: {e}")

        # ── 4. Volume Anomaly ──
        try:
            klines = await self.client.get_klines(symbol, "5m", limit=24)
            if len(klines) >= 6:
                avg_vol = sum(k["volume"] for k in klines[:-3]) / max(len(klines) - 3, 1)
                recent_vol = sum(k["volume"] for k in klines[-3:]) / 3
                vol_ratio = recent_vol / avg_vol if avg_vol > 0 else 0
                filters["volume_anomaly_ratio"] = round(vol_ratio, 2)

                vol_growing = all(
                    klines[i]["volume"] > klines[i - 1]["volume"]
                    for i in range(len(klines) - 2, len(klines))
                    if i > 0
                )

                if vol_ratio > 10 and vol_growing:
                    blockers.append(
                        f"VOLUME ANOMALY: ratio {vol_ratio:.1f}x Y creciendo → "
                        "demanda real, no shortear"
                    )
        except Exception as e:
            warnings.append(f"Error volume check: {e}")

        # ── Verdict ──
        passed = len(blockers) == 0
        return {
            "symbol": symbol,
            "mode": mode,
            "direction": direction,
            "verdict": "GO ✅" if passed else "NO_ENTRY ❌",
            "passed": passed,
            "filters": filters,
            "blockers": blockers,
            "warnings": warnings,
            "context": (
                f"{'✅ FILTROS OK' if passed else '❌ BLOQUEADO'} para {symbol} Modo {mode} {direction}\n"
                + (("\n".join(f"  🚫 {b}" for b in blockers)) if blockers else "  Todos los kill switches verdes.")
                + (("\n" + "\n".join(f"  ⚠️ {w}" for w in warnings)) if warnings else "")
            ),
        }

    # ─────────────────────────────────────────
    # TOOL 3: striker_monitor_striker
    # ─────────────────────────────────────────

    async def monitor_striker(self) -> dict:
        """Query Striker's current open positions and recent stats."""
        try:
            detail = await self.copytrading.get_trader_detail(
                portfolio_id=STRIKER_PORTFOLIO_ID,
                time_range="7D",
            )
            return {
                "portfolio_id": STRIKER_PORTFOLIO_ID,
                "profile": detail.get("profile", {}),
                "stats": detail.get("stats", {}),
                "open_positions": detail.get("open_positions", []),
                "performance_periods": detail.get("performance_periods", []),
                "context": (
                    f"📡 STRIKER MONITOR: "
                    f"{len(detail.get('open_positions', []))} posiciones abiertas.\n"
                    + "\n".join(
                        f"  • {p.get('symbol')} {p.get('direction')} "
                        f"size={p.get('position_amount')} "
                        f"entry={p.get('entry_price')} "
                        f"uPnL={p.get('unrealized_pnl', '?')}"
                        for p in detail.get("open_positions", [])
                    )
                ),
            }
        except Exception as e:
            return {
                "portfolio_id": STRIKER_PORTFOLIO_ID,
                "error": str(e),
                "context": f"❌ Error consultando Striker: {e}",
            }

    # ─────────────────────────────────────────
    # TOOL 4: striker_open_scout
    # ─────────────────────────────────────────

    async def open_scout(self, symbol: str, direction: str, account: str = "principal") -> dict:
        """Open a $5 micro-lot as direction sensor."""
        self._reset_session_if_new_day()

        # Risk checks
        risk = await self.check_risk(account)
        if risk.get("blocked"):
            return {"error": risk["reason"], "context": f"❌ Scout bloqueado: {risk['reason']}"}

        trader = self._get_trader(account)
        ticker = await self.client.get_ticker_24h(symbol)
        mark_price = float(ticker.get("lastPrice", 0))
        if mark_price <= 0:
            return {"error": "No se pudo obtener precio", "context": "❌ Precio no disponible"}

        quantity = round(SCOUT_NOTIONAL / mark_price, 4)
        if quantity <= 0:
            return {"error": "Cantidad calculada es 0", "context": "❌ Cantidad inválida"}

        side = "SELL" if direction == "SHORT" else "BUY"

        result = await trader.open_position(
            symbol=symbol,
            side=side,
            quantity=quantity,
            order_type="MARKET",
            leverage=20,
        )

        self._scout_history.append({
            "symbol": symbol,
            "direction": direction,
            "entry_time": time.time(),
            "entry_price": mark_price,
            "quantity": quantity,
        })

        result["scout_info"] = {
            "type": "SCOUT",
            "notional_usd": round(quantity * mark_price, 2),
            "direction": direction,
            "eval_after_min": SCOUT_EVAL_TIME_MIN,
            "max_wait_min": SCOUT_MAX_WAIT_MIN,
        }
        result["context"] = (
            f"🔍 SCOUT abierto: {symbol} {direction} ${round(quantity * mark_price, 2)} "
            f"@ {mark_price}. Evaluar en {SCOUT_EVAL_TIME_MIN} min."
        )
        return result

    # ─────────────────────────────────────────
    # TOOL 5: striker_evaluate_scout
    # ─────────────────────────────────────────

    async def evaluate_scout(self, symbol: str, account: str = "principal") -> dict:
        """Evaluate scout result. Returns CONFIRM / REJECT / WAIT."""
        trader = self._get_trader(account)
        positions = await trader.get_positions(symbol)

        position = None
        for p in positions.get("positions", []):
            if float(p.get("notional", 0)) != 0:
                position = p
                break

        if not position:
            return {
                "symbol": symbol,
                "verdict": "NO_POSITION",
                "context": f"⚠️ No hay posición scout activa para {symbol}",
            }

        entry_price = float(position.get("entryPrice", 0))
        mark_price = float(position.get("markPrice", 0))
        direction = "SHORT" if float(position.get("notional", 0)) < 0 else "LONG"

        if entry_price > 0:
            if direction == "SHORT":
                pnl_pct = (entry_price - mark_price) / entry_price
            else:
                pnl_pct = (mark_price - entry_price) / entry_price
        else:
            pnl_pct = 0

        # Check timing
        scout_entry = None
        for s in reversed(self._scout_history):
            if s["symbol"] == symbol:
                scout_entry = s
                break

        elapsed_min = 0
        if scout_entry:
            elapsed_min = (time.time() - scout_entry["entry_time"]) / 60

        # ── Verdict ──
        if pnl_pct > SCOUT_CONFIRM_PNL:
            verdict = "CONFIRM"
            action = f"Scout CONFIRMA dirección {direction} → Abrir CONVICTION"
        elif pnl_pct < SCOUT_REJECT_PNL:
            verdict = "REJECT"
            action = f"Scout RECHAZA dirección {direction} → NO escalar"
        elif elapsed_min >= SCOUT_MAX_WAIT_MIN:
            verdict = "TIMEOUT"
            action = "Scout tiempo máximo alcanzado → Cerrar y reevaluar"
        else:
            verdict = "WAIT"
            action = f"Esperar ({SCOUT_MAX_WAIT_MIN - elapsed_min:.0f} min restantes)"

        # Flip check: 3 consecutive scout losses in same direction
        same_dir_losses = 0
        for s in reversed(self._scout_history):
            if s["symbol"] == symbol and s["direction"] == direction:
                same_dir_losses += 1
            else:
                break
        flip_suggested = same_dir_losses >= 3 and verdict == "REJECT"

        return {
            "symbol": symbol,
            "direction": direction,
            "verdict": verdict,
            "pnl_pct": round(pnl_pct * 100, 4),
            "elapsed_min": round(elapsed_min, 1),
            "flip_direction_suggested": flip_suggested,
            "action": action,
            "position": {
                "entry_price": entry_price,
                "mark_price": mark_price,
                "notional": position.get("notional"),
            },
            "context": (
                f"{'✅' if verdict == 'CONFIRM' else '❌' if verdict == 'REJECT' else '⏳'} "
                f"SCOUT {symbol} {direction}: {verdict} | PnL: {pnl_pct * 100:+.3f}% | "
                f"{elapsed_min:.0f}min\n  → {action}"
                + (f"\n  🔄 FLIP: 3+ scouts perdidos en {direction}, considerar dirección opuesta" if flip_suggested else "")
            ),
        }

    # ─────────────────────────────────────────
    # TOOL 6: striker_open_conviction
    # ─────────────────────────────────────────

    async def open_conviction(
        self,
        symbol: str,
        mode: str,
        direction: str = "SHORT",
        account: str = "principal",
        skip_scout: bool = False,
    ) -> dict:
        """Open conviction trade with sizing per mode rules."""
        self._reset_session_if_new_day()

        mode = mode.upper()
        if mode not in MODES:
            return {"error": f"Modo inválido: {mode}. Usar A, B o C."}

        m = MODES[mode]

        # Risk checks
        risk = await self.check_risk(account)
        if risk.get("blocked"):
            return {"error": risk["reason"], "context": f"❌ Trade bloqueado: {risk['reason']}"}

        trader = self._get_trader(account)

        # Get balance for sizing
        balance_info = await trader.get_account_balance()
        balance = float(balance_info.get("available_balance", 0))
        if balance <= 0:
            return {"error": "Balance insuficiente", "context": "❌ Balance = 0"}

        # Get price
        ticker = await self.client.get_ticker_24h(symbol)
        mark_price = float(ticker.get("lastPrice", 0))
        if mark_price <= 0:
            return {"error": "No se pudo obtener precio"}

        # Sizing
        notional = balance * m["sizing_pct"]
        notional = min(notional, MAX_NOTIONAL_SINGLE)
        quantity = round(notional / mark_price, 4)

        if quantity <= 0:
            return {"error": "Cantidad calculada = 0"}

        side = "SELL" if direction == "SHORT" else "BUY"

        # Calculate TP/SL
        tp_sl = _calc_tp_sl_prices(mark_price, direction, mode)

        # Open position with TP1 as initial TP and SL
        result = await trader.open_position(
            symbol=symbol,
            side=side,
            quantity=quantity,
            order_type="MARKET",
            leverage=m["leverage"],
            take_profit=tp_sl["tp1_price"],
            stop_loss=tp_sl["sl_price"],
        )

        self._daily_trades += 1

        result["striker_trade"] = {
            "mode": mode,
            "mode_name": m["name"],
            "direction": direction,
            "notional_usd": round(notional, 2),
            "sizing_pct": m["sizing_pct"] * 100,
            "balance_used": round(balance, 2),
            "skip_scout": skip_scout,
            "tp_sl": tp_sl,
        }
        result["context"] = (
            f"🎯 CONVICTION {symbol} {direction} Modo {mode} ({m['name']})\n"
            f"  Size: {quantity} ({notional:.2f} USDT, {m['sizing_pct']*100:.0f}% del balance)\n"
            f"  Entry: ~{mark_price} | TP1: {tp_sl['tp1_price']} (-{m['tp1_pct']*100:.1f}%)"
            + (f" | TP2: {tp_sl.get('tp2_price', '—')} (-{(m['tp2_pct'] or 0)*100:.1f}%)" if m['tp2_pct'] else "")
            + f" | SL: {tp_sl['sl_price']} (+{m['sl_pct']*100:.1f}%)\n"
            f"  Trade #{self._daily_trades} del día | Leverage: {m['leverage']}x"
        )
        return result

    # ─────────────────────────────────────────
    # TOOL 7: striker_set_tp_sl
    # ─────────────────────────────────────────

    async def set_tp_sl(
        self,
        symbol: str,
        mode: str,
        entry_price: float,
        direction: str = "SHORT",
        account: str = "principal",
    ) -> dict:
        """Calculate and set TP/SL for an existing position per mode rules."""
        mode = mode.upper()
        if mode not in MODES:
            return {"error": f"Modo inválido: {mode}"}

        tp_sl = _calc_tp_sl_prices(entry_price, direction, mode)

        trader = self._get_trader(account)
        result = await trader.set_tp_sl(
            symbol=symbol,
            take_profit=tp_sl["tp1_price"],
            stop_loss=tp_sl["sl_price"],
        )

        result["striker_tp_sl"] = tp_sl
        result["context"] = (
            f"🎯 TP/SL configurado para {symbol} Modo {mode} {direction}\n"
            f"  Entry: {entry_price}\n"
            f"  TP1: {tp_sl['tp1_price']} (cerrar {tp_sl['tp1_close_pct']}%)"
            + (f"\n  TP2: {tp_sl.get('tp2_price', '—')} (cerrar {tp_sl.get('tp2_close_pct', 0)}%)" if tp_sl.get("tp2_price") else "")
            + (f"\n  Trailing: {tp_sl.get('trailing_close_pct', 0)}% restante, activación {tp_sl.get('trailing_activation', 0):.1f}%, callback {tp_sl.get('trailing_callback', 0):.1f}%" if tp_sl.get("trailing_close_pct") else "")
            + f"\n  SL: {tp_sl['sl_price']} ({tp_sl['sl_pct']:.1f}%)"
        )
        return result

    # ─────────────────────────────────────────
    # TOOL 8: striker_check_exit
    # ─────────────────────────────────────────

    async def check_exit(self, symbol: str, account: str = "principal") -> dict:
        """
        Evaluate if an open position should be closed:
          - Time-based stops per mode
          - Volume dead
          - Funding kill switch
          - 12h max hold
          - 4h opportunity cost
        """
        trader = self._get_trader(account)
        positions = await trader.get_positions(symbol)

        position = None
        for p in positions.get("positions", []):
            if float(p.get("notional", 0)) != 0:
                position = p
                break

        if not position:
            return {
                "symbol": symbol,
                "verdict": "NO_POSITION",
                "context": f"⚠️ No hay posición abierta para {symbol}",
            }

        entry_price = float(position.get("entryPrice", 0))
        mark_price = float(position.get("markPrice", 0))
        notional = abs(float(position.get("notional", 0)))
        direction = "SHORT" if float(position.get("notional", 0)) < 0 else "LONG"

        if entry_price > 0:
            if direction == "SHORT":
                pnl_pct = (entry_price - mark_price) / entry_price
            else:
                pnl_pct = (mark_price - entry_price) / entry_price
        else:
            pnl_pct = 0

        exit_reasons = []

        # ── Funding Kill Switch ──
        try:
            funding = await self.client.get_funding_rate(symbol, limit=1)
            if funding:
                fr = float(funding[-1].get("fundingRate", 0)) * 100
                if direction == "SHORT" and fr < -1.00:
                    exit_reasons.append(f"FUNDING KILL: {fr:.4f}% < -1.00%")
        except Exception:
            pass

        # ── Volume dead ──
        try:
            klines = await self.client.get_klines(symbol, "5m", limit=6)
            if len(klines) >= 3:
                vol_now = sum(k["volume"] for k in klines[-3:]) / 3
                # We need entry volume — approximate with avg of earlier candles
                vol_earlier = sum(k["volume"] for k in klines[:3]) / 3
                if vol_earlier > 0 and vol_now < vol_earlier * 0.30 and pnl_pct > 0.003:
                    exit_reasons.append(
                        f"VOLUME DEAD: vol actual {vol_now:.0f} < 30% del anterior ({vol_earlier:.0f}) "
                        f"y PnL > +0.3% → momentum muerto"
                    )
        except Exception:
            pass

        # ── Volume anomaly (10x spike against position) ──
        try:
            klines = await self.client.get_klines(symbol, "5m", limit=12)
            if len(klines) >= 6:
                avg_vol = sum(k["volume"] for k in klines[:6]) / 6
                latest_vol = klines[-1]["volume"]
                if avg_vol > 0 and latest_vol > avg_vol * 10:
                    exit_reasons.append(
                        f"VOLUME ANOMALY: última candle {latest_vol:.0f} > 10× promedio {avg_vol:.0f}"
                    )
        except Exception:
            pass

        # ── Max hold 12h ──
        update_time = position.get("updateTime", 0)
        if update_time:
            hours_held = (time.time() * 1000 - update_time) / (3600 * 1000)
            if hours_held >= MAX_HOLD_HOURS:
                exit_reasons.append(f"MAX HOLD: {hours_held:.1f}h >= {MAX_HOLD_HOURS}h")

            # ── Opportunity cost (4h with small profit) ──
            if hours_held >= OPPORTUNITY_COST_HOURS and 0 < pnl_pct < OPPORTUNITY_COST_MIN_PNL:
                exit_reasons.append(
                    f"OPPORTUNITY COST: {hours_held:.1f}h y PnL solo +{pnl_pct*100:.2f}% < +0.5%"
                )

        should_exit = len(exit_reasons) > 0
        return {
            "symbol": symbol,
            "direction": direction,
            "verdict": "EXIT ❌" if should_exit else "HOLD ✅",
            "should_exit": should_exit,
            "pnl_pct": round(pnl_pct * 100, 4),
            "exit_reasons": exit_reasons,
            "position": {
                "entry_price": entry_price,
                "mark_price": mark_price,
                "notional": notional,
                "unrealized_pnl": position.get("unRealizedProfit"),
            },
            "context": (
                f"{'❌ EXIT' if should_exit else '✅ HOLD'} {symbol} {direction} | PnL: {pnl_pct*100:+.3f}%\n"
                + ("\n".join(f"  🚫 {r}" for r in exit_reasons) if exit_reasons else "  Posición dentro de parámetros.")
            ),
        }

    # ─────────────────────────────────────────
    # TOOL 9: striker_partial_close
    # ─────────────────────────────────────────

    async def partial_close(self, symbol: str, pct: float, account: str = "principal") -> dict:
        """
        Close a percentage of an open position (for scaled TP).
        pct: 0-100 percentage to close.
        """
        trader = self._get_trader(account)
        positions = await trader.get_positions(symbol)

        position = None
        for p in positions.get("positions", []):
            if float(p.get("notional", 0)) != 0:
                position = p
                break

        if not position:
            return {"error": f"No hay posición abierta para {symbol}"}

        total_qty = abs(float(position.get("positionAmt", 0)))
        close_qty = round(total_qty * (pct / 100), 4)

        if close_qty <= 0:
            return {"error": "Cantidad a cerrar = 0"}

        result = await trader.close_position(
            symbol=symbol,
            quantity=close_qty,
        )

        result["partial_close"] = {
            "total_quantity": total_qty,
            "closed_quantity": close_qty,
            "closed_pct": pct,
            "remaining_quantity": round(total_qty - close_qty, 4),
            "remaining_pct": round(100 - pct, 1),
        }
        result["context"] = (
            f"✂️ Cierre parcial {symbol}: {pct:.0f}% ({close_qty} de {total_qty})\n"
            f"  Restante: {round(total_qty - close_qty, 4)} ({100 - pct:.0f}%)"
        )
        return result

    # ─────────────────────────────────────────
    # TOOL 10: striker_check_risk
    # ─────────────────────────────────────────

    async def check_risk(self, account: str = "principal") -> dict:
        """Check position limits, daily loss, circuit breakers."""
        self._reset_session_if_new_day()

        blocked = False
        reason = ""
        warnings = []

        # ── Pause check ──
        if time.time() < self._pause_until:
            remaining = (self._pause_until - time.time()) / 60
            blocked = True
            reason = f"En pausa por {self.CONSECUTIVE_LOSS_PAUSE} losses consecutivos. Quedan {remaining:.0f} min."
            return {"blocked": blocked, "reason": reason}

        trader = self._get_trader(account)

        # ── Balance & positions ──
        balance_info = await trader.get_account_balance()
        balance = float(balance_info.get("total_balance", 0))

        if self._daily_start_balance == 0:
            self._daily_start_balance = balance

        # Daily loss check
        if self._daily_start_balance > 0:
            daily_pnl_pct = (balance - self._daily_start_balance) / self._daily_start_balance
            if daily_pnl_pct < -DAILY_LOSS_STOP_PCT:
                blocked = True
                reason = f"DAILY LOSS STOP: {daily_pnl_pct*100:.1f}% > -{DAILY_LOSS_STOP_PCT*100:.0f}%"
                return {"blocked": blocked, "reason": reason}

        # Concurrent positions check
        all_positions = await trader.get_positions()
        open_positions = [
            p for p in all_positions.get("positions", [])
            if float(p.get("notional", 0)) != 0
        ]
        total_notional = sum(abs(float(p.get("notional", 0))) for p in open_positions)

        if len(open_positions) >= MAX_CONCURRENT_POSITIONS:
            blocked = True
            reason = f"Max posiciones concurrentes: {len(open_positions)} >= {MAX_CONCURRENT_POSITIONS}"

        if total_notional >= MAX_NOTIONAL_TOTAL:
            blocked = True
            reason = f"Max notional total: ${total_notional:.0f} >= ${MAX_NOTIONAL_TOTAL}"

        if self._daily_trades >= MAX_TRADES_PER_SESSION:
            blocked = True
            reason = f"Max trades diarios: {self._daily_trades} >= {MAX_TRADES_PER_SESSION}"

        if self._consecutive_losses >= CONSECUTIVE_LOSS_PAUSE:
            self._pause_until = time.time() + PAUSE_DURATION_MIN * 60
            blocked = True
            reason = f"{CONSECUTIVE_LOSS_PAUSE} losses consecutivos → pausa de {PAUSE_DURATION_MIN} min"

        return {
            "blocked": blocked,
            "reason": reason,
            "warnings": warnings,
            "status": {
                "balance": round(balance, 2),
                "daily_start_balance": round(self._daily_start_balance, 2),
                "daily_pnl_pct": round(
                    ((balance - self._daily_start_balance) / self._daily_start_balance * 100)
                    if self._daily_start_balance > 0 else 0, 2
                ),
                "open_positions": len(open_positions),
                "max_positions": MAX_CONCURRENT_POSITIONS,
                "total_notional_usd": round(total_notional, 2),
                "max_notional_usd": MAX_NOTIONAL_TOTAL,
                "daily_trades": self._daily_trades,
                "max_daily_trades": MAX_TRADES_PER_SESSION,
                "consecutive_losses": self._consecutive_losses,
            },
            "context": (
                f"{'❌ BLOQUEADO' if blocked else '✅ RISK OK'}\n"
                + (f"  Razón: {reason}\n" if reason else "")
                + f"  Balance: ${balance:.2f} | Posiciones: {len(open_positions)}/{MAX_CONCURRENT_POSITIONS} "
                f"| Notional: ${total_notional:.0f}/${MAX_NOTIONAL_TOTAL} "
                f"| Trades hoy: {self._daily_trades}/{MAX_TRADES_PER_SESSION}"
            ),
        }

    # ─────────────────────────────────────────
    # TOOL 11: striker_daily_report
    # ─────────────────────────────────────────

    async def daily_report(self, account: str = "principal") -> dict:
        """Daily performance summary: our trades vs Striker."""
        trader = self._get_trader(account)

        # Our performance
        our_perf = await trader.get_performance(days=1)

        # Striker's recent performance
        try:
            striker = await self.copytrading.get_trader_detail(
                portfolio_id=STRIKER_PORTFOLIO_ID,
                time_range="7D",
            )
            striker_stats = striker.get("stats", {})
            striker_positions = striker.get("open_positions", [])
        except Exception:
            striker_stats = {}
            striker_positions = []

        our_pnl = our_perf.get("total_pnl", 0)
        our_trades_count = our_perf.get("total_trades", 0)
        our_wr = our_perf.get("win_rate", 0)
        our_pf = our_perf.get("profit_factor", 0)

        return {
            "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "our_performance": {
                "pnl_usdt": round(our_pnl, 2),
                "trades": our_trades_count,
                "win_rate": round(our_wr, 1),
                "profit_factor": round(our_pf, 2),
                "session_trades": self._daily_trades,
                "consecutive_losses": self._consecutive_losses,
            },
            "striker": {
                "stats": striker_stats,
                "open_positions_count": len(striker_positions),
                "open_positions": striker_positions[:5],
            },
            "context": (
                f"📊 DAILY REPORT {datetime.now(timezone.utc).strftime('%Y-%m-%d')}\n"
                f"  NOSOTROS: PnL ${our_pnl:+.2f} | {our_trades_count} trades | "
                f"WR {our_wr:.0f}% | PF {our_pf:.1f}\n"
                f"  STRIKER: {len(striker_positions)} posiciones abiertas | "
                f"WR {striker_stats.get('win_rate_pct', '?')}%\n"
                f"  Session: {self._daily_trades} trades | {self._consecutive_losses} losses consecutivos"
            ),
        }
