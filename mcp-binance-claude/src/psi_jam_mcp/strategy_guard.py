"""
Strategy Guard — Módulo de evaluación de entrada + watchdog automático.

3 tools para el MCP:
  evaluate_short_entry  → Evalúa condiciones R0-R4. NUNCA abre posición.
  activate_guard        → Activa monitoreo automático para posición abierta.
  guard_status          → Estado del watchdog + posiciones monitoreadas.

1 proceso background:
  _guard_loop()         → Cada CHECK_INTERVAL_S chequea R5-R8 y cierra
                          automáticamente si auto_close=True.
"""

import asyncio
import logging
import time
from datetime import datetime, timezone

import numpy as np

logger = logging.getLogger("strategy_guard")

# ─────────────────────────────────────────────────────────────
# PARAMS
# ─────────────────────────────────────────────────────────────
PARAMS = {
    # R1: JAM
    "vol_threshold": 2.0,
    "delta_threshold": 0.10,
    "retention_threshold": 0.70,

    # R2: OI capitulation
    "oi_drawdown_threshold": 0.50,
    "oi_pre_pump_lookback": 10,
    "oi_history_periods": 50,

    # R3/R6: Funding
    "funding_entry_max": 0.0003,      # 0.03%/h para entrar
    "funding_alert": 0.0005,          # 0.05%/h alerta
    "funding_close": 0.0010,          # 0.10%/h cierre forzado

    # R4: Sizing
    "max_risk_pct": 0.01,

    # R7: Time stop
    "max_hold_hours": 12,
    "max_hold_hours_profit": 24,

    # Background loop
    "check_interval_s": 5 * 60,  # 5 minutos
}


class StrategyGuard:
    """Evaluación de entrada SHORT + watchdog automático de posiciones."""

    def __init__(self, binance_client, get_trader_fn, analysis_module):
        self.client = binance_client
        self._get_trader = get_trader_fn
        self.analysis = analysis_module

        # State
        self.guarded_positions: dict[str, dict] = {}
        self.guard_log: list[dict] = []
        self._loop_running = False
        self._loop_task: asyncio.Task | None = None

    # ─────────────────────────────────────────────────────────
    # TOOL 1: evaluate_short_entry
    # ─────────────────────────────────────────────────────────

    async def evaluate_short_entry(
        self, symbol: str, account: str, sl_percent: float = 5.0
    ) -> dict:
        """Evalúa condiciones R0-R4 para entrada SHORT. NUNCA abre posición."""
        checks = []
        dominated = False  # hard NO
        waiting = False    # soft WAIT

        # ── R0: Mercado global ──────────────────────────────
        try:
            global_data = await self.client.get_global_market_analysis()
            gv = (
                global_data.get("verdict")
                or global_data.get("market_verdict", "NEUTRAL")
            )
        except Exception:
            gv = "UNKNOWN"

        if "DESFAVORABLE" in gv.upper():
            checks.append({
                "rule": "R0", "ok": False, "wait": False,
                "label": "Mercado global", "value": gv,
                "detail": "Desfavorable para shorts. No operar.",
            })
            dominated = True
        else:
            checks.append({
                "rule": "R0", "ok": True, "wait": False,
                "label": "Mercado global", "value": gv,
            })

        # ── R1: JAM Régimen ─────────────────────────────────
        try:
            klines = await self.client.get_klines(
                symbol=symbol, interval="1h", limit=200
            )
            closes = np.array([k["close"] for k in klines])
            volumes = np.array([k["volume"] for k in klines])
            taker_buy_vols = np.array([k["taker_buy_volume"] for k in klines])
            highs = np.array([k["high"] for k in klines])
            lows = np.array([k["low"] for k in klines])

            jam = self.analysis.jam_regime_analysis(
                closes, volumes, taker_buy_vols, highs, lows, window=20
            )
            regime = jam.get("regime", "UNKNOWN")
            kappa = jam.get("kappa")
            gamma = jam.get("gamma")
        except Exception as e:
            regime = "ERROR"
            kappa = gamma = None
            jam = {}
            checks.append({
                "rule": "R1", "ok": False, "label": "JAM Régimen",
                "value": f"Error: {e}",
            })

        if regime != "ERROR":
            if regime == "B":
                kappa_str = f"{kappa:.3f}" if kappa is not None else "?"
                gamma_str = f"{gamma:.3f}" if gamma is not None else "?"
                checks.append({
                    "rule": "R1", "ok": True,
                    "label": "JAM Régimen",
                    "value": f"B (κ={kappa_str}, γ={gamma_str})",
                })
            else:
                detail = (
                    "Impulso sostenido — no shortear"
                    if regime == "A"
                    else "Sin impulso claro"
                )
                checks.append({
                    "rule": "R1", "ok": False,
                    "label": "JAM Régimen", "value": regime,
                    "detail": detail,
                })
                dominated = True

        # ── R2: OI Capitulación ─────────────────────────────
        try:
            oi_hist = await self.client.get_open_interest_hist(
                symbol, period="1h", limit=PARAMS["oi_history_periods"]
            )
            oi_vals = [h["sum_open_interest"] for h in oi_hist]
        except Exception:
            oi_vals = []

        d_oi = None
        oi_spike = 0.0
        oi_pre = 0.0
        oi_peak = 0.0
        oi_now = 0.0

        if len(oi_vals) >= 10:
            oi_now = oi_vals[-1]
            oi_peak = max(oi_vals)
            lookback = PARAMS["oi_pre_pump_lookback"]
            oi_pre = sum(oi_vals[:lookback]) / lookback
            oi_spike = ((oi_peak / oi_pre) - 1) * 100 if oi_peak > oi_pre else 0
            if oi_peak > oi_pre and (oi_peak - oi_pre) > 0:
                d_oi = max(0.0, min(1.0, (oi_peak - oi_now) / (oi_peak - oi_pre)))

        if oi_spike < 10 or d_oi is None:
            checks.append({
                "rule": "R2", "ok": True, "wait": False,
                "label": "OI Capitulación",
                "value": f"Sin spike significativo ({oi_spike:.0f}%)",
                "detail": "Condición no aplica",
            })
        elif d_oi >= PARAMS["oi_drawdown_threshold"]:
            checks.append({
                "rule": "R2", "ok": True,
                "label": "OI Capitulación",
                "value": f"D_OI={d_oi*100:.0f}% ✓",
                "detail": (
                    f"{oi_pre/1e6:.2f}M → {oi_peak/1e6:.2f}M → {oi_now/1e6:.2f}M"
                ),
            })
        else:
            checks.append({
                "rule": "R2", "ok": False, "wait": True,
                "label": "OI Capitulación",
                "value": f"D_OI={d_oi*100:.0f}% < 50%",
                "detail": f"Solo cerró {d_oi*100:.0f}% del OI del pump. ESPERAR.",
            })
            waiting = True

        # ── R3: Funding ─────────────────────────────────────
        try:
            fund_data = await self.client.get_funding_rate(symbol, limit=5)
            last_rate = fund_data[-1] if fund_data else {}
            r = last_rate.get("funding_rate", 0)
        except Exception:
            r = 0

        abs_r = abs(r)
        shorts_pay = r < 0

        if shorts_pay and abs_r > PARAMS["funding_close"]:
            checks.append({
                "rule": "R3", "ok": False,
                "label": "Funding",
                "value": f"{r*100:.4f}%/h — shorts pagan",
                "detail": "MUY ALTO. No entrar.",
            })
            dominated = True
        elif shorts_pay and abs_r > PARAMS["funding_entry_max"]:
            checks.append({
                "rule": "R3", "ok": False, "wait": True,
                "label": "Funding",
                "value": f"{r*100:.4f}%/h — shorts pagan",
                "detail": "Elevado. Esperar normalización.",
            })
            waiting = True
        else:
            note = (
                "Shorts pagan pero costo aceptable"
                if shorts_pay
                else "Shorts cobran — favorable"
            )
            checks.append({
                "rule": "R3", "ok": True,
                "label": "Funding", "value": f"{r*100:.4f}%/h",
                "detail": note,
            })

        # ── R4: Sizing ──────────────────────────────────────
        try:
            bal = await self._get_trader(account).get_account_balance()
            capital = float(
                bal.get("usdt_balance", 0)
                or bal.get("total_balance", 0)
                or bal.get("balance", 0)
            )
        except Exception:
            capital = 0

        max_loss = capital * PARAMS["max_risk_pct"]
        max_notional = max_loss / (sl_percent / 100) if sl_percent > 0 else 0

        try:
            ticker = await self.client.get_ticker_24h(symbol)
            price = float(ticker.get("last_price") or ticker.get("mark_price") or 0)
        except Exception:
            price = 0

        max_contracts = max_notional / price if price > 0 else 0

        checks.append({
            "rule": "R4", "ok": True,
            "label": "Sizing",
            "value": (
                f"Max loss ${max_loss:.2f} → "
                f"Nocional ${max_notional:.2f} → "
                f"{max_contracts:.1f} contratos"
            ),
            "detail": f"Capital ${capital:.2f} | SL {sl_percent}%",
        })

        # ── VERDICT ─────────────────────────────────────────
        if dominated:
            verdict = "NO_TRADE"
            emoji = "🔴"
        elif waiting:
            verdict = "WAIT"
            emoji = "🟡"
        else:
            verdict = "GO"
            emoji = "🟢"

        sl_price = price * (1 + sl_percent / 100) if price > 0 else 0
        tp_price = price * (1 - sl_percent / 100) if price > 0 else 0

        entry_params = None
        if verdict == "GO":
            time_stop = datetime.now(timezone.utc).timestamp() + PARAMS["max_hold_hours"] * 3600
            entry_params = {
                "side": "SHORT",
                "price": round(price, 6),
                "sl_price": round(sl_price, 6),
                "tp_price": round(tp_price, 6),
                "max_notional": round(max_notional, 2),
                "max_contracts": round(max_contracts, 2),
                "sl_percent": sl_percent,
                "time_stop_utc": datetime.fromtimestamp(
                    time_stop, tz=timezone.utc
                ).isoformat(),
            }

        # next_steps guía al LLM sobre qué hacer después
        if verdict == "GO":
            next_steps = (
                "Presentar esta evaluación al usuario. "
                "ANTES de abrir la posición, PREGUNTAR: "
                "'¿Querés que el trade se autogestione? "
                "(cierre automático si el funding se dispara o se cumple el time stop)'. "
                "Usar futures_open_position para abrir con los parámetros sugeridos "
                "(incluir stop_loss y take_profit). "
                "Después de abrir, llamar activate_guard con auto_close=true si "
                "el usuario aceptó autogestión, o auto_close=false para solo monitoreo."
            )
        elif verdict == "WAIT":
            next_steps = (
                "Condiciones parciales — no abrir todavía. "
                "Monitorear y re-evaluar cuando las condiciones marcadas como WAIT mejoren."
            )
        else:
            next_steps = (
                "Condiciones NO aptas para short. No abrir esta posición. "
                "Buscar otros pares o esperar cambio de condiciones."
            )

        return {
            "symbol": symbol,
            "account": account,
            "verdict": f"{emoji} {verdict}",
            "checklist": checks,
            "entry_params": entry_params,
            "next_steps": next_steps,
            "context": {
                "price": price,
                "funding_rate": r,
                "d_oi": round(d_oi * 100, 1) if d_oi is not None else None,
                "oi_spike": round(oi_spike),
                "regime": regime,
                "capital": round(capital, 2),
            },
        }

    # ─────────────────────────────────────────────────────────
    # TOOL 2: activate_guard
    # ─────────────────────────────────────────────────────────

    async def activate_guard(
        self,
        symbol: str,
        account: str,
        auto_close: bool = False,
        entry_price: float | None = None,
        sl_price: float | None = None,
        max_hold_hours: float | None = None,
    ) -> dict:
        """Activa watchdog para una posición SHORT abierta."""
        key = f"{account}:{symbol}"

        # Obtener datos de la posición si no se pasan
        if not entry_price:
            try:
                pos_data = await self._get_trader(account).get_positions(symbol=symbol)
                positions = pos_data.get("positions", [])
                pos = next(
                    (
                        p
                        for p in positions
                        if p.get("symbol") == symbol and p.get("side") == "SHORT"
                    ),
                    None,
                )
                if not pos:
                    return {
                        "error": (
                            f"No se encontró posición SHORT abierta en {symbol} "
                            f"({account}). Abrir primero."
                        )
                    }
                entry_price = float(pos.get("entry_price", 0))
            except Exception as e:
                return {"error": f"Error obteniendo posición: {e}"}

        hold_hours = max_hold_hours or PARAMS["max_hold_hours"]
        hold_ms = hold_hours * 3_600_000

        guard = {
            "account": account,
            "symbol": symbol,
            "entry_time": time.time() * 1000,  # ms epoch
            "entry_price": entry_price,
            "sl_price": sl_price,
            "auto_close": auto_close,
            "max_hold_ms": hold_ms,
            "max_hold_hours": hold_hours,
            "last_check": None,
            "alerts": [],
            "status": "ACTIVE",
            "closes_executed": 0,
        }

        self.guarded_positions[key] = guard

        time_stop_utc = datetime.fromtimestamp(
            (guard["entry_time"] + hold_ms) / 1000, tz=timezone.utc
        ).isoformat()

        self._log(
            f"GUARD ACTIVATED: {key} | auto_close={auto_close} | "
            f"max_hold={hold_hours}h | SL={sl_price or 'from exchange'}"
        )

        return {
            "message": f"✅ Guard activado para {symbol} ({account})",
            "key": key,
            "auto_close": auto_close,
            "max_hold_hours": hold_hours,
            "time_stop_utc": time_stop_utc,
            "monitoring": {
                "check_interval": f"{PARAMS['check_interval_s'] / 60} minutos",
                "rules_monitored": [
                    "R5: SL colocado",
                    "R6: Funding adverso (cierre a > 0.10%/h)",
                    f"R7: Time stop ({hold_hours}h)",
                    "R8: PnL negativo → no agregar",
                ],
                "auto_actions": (
                    "R6 y R7 cierran posición automáticamente a mercado"
                    if auto_close
                    else "Solo alertas — cierre manual requerido"
                ),
            },
            "note": (
                "⚠️ AUTO-CLOSE ACTIVO. El MCP cerrará esta posición si el "
                "funding supera 0.10%/h o se excede el time stop."
                if auto_close
                else "Monitoreo pasivo. Alertas se acumulan en guard_status "
                "pero no se ejecutan cierres."
            ),
        }

    # ─────────────────────────────────────────────────────────
    # TOOL 3: guard_status
    # ─────────────────────────────────────────────────────────

    async def guard_status(self, account: str | None = None) -> dict:
        """Estado actual del watchdog y posiciones monitoreadas."""
        entries = []
        now_ms = time.time() * 1000

        for key, g in self.guarded_positions.items():
            if account and g["account"] != account:
                continue

            hold_hours = (now_ms - g["entry_time"]) / 3_600_000
            time_remaining = max(0, g["max_hold_hours"] - hold_hours)
            time_stop_utc = datetime.fromtimestamp(
                (g["entry_time"] + g["max_hold_ms"]) / 1000, tz=timezone.utc
            ).isoformat()

            entries.append({
                "key": key,
                "symbol": g["symbol"],
                "account": g["account"],
                "auto_close": g["auto_close"],
                "status": g["status"],
                "hold_hours": round(hold_hours, 1),
                "time_remaining_hours": round(time_remaining, 1),
                "time_stop_utc": time_stop_utc,
                "last_check": (
                    datetime.fromtimestamp(
                        g["last_check"] / 1000, tz=timezone.utc
                    ).isoformat()
                    if g["last_check"]
                    else "Pendiente"
                ),
                "alerts_count": len(g["alerts"]),
                "alerts_last_3": g["alerts"][-3:],
                "closes_executed": g["closes_executed"],
            })

        return {
            "guard_active": self._loop_running,
            "check_interval": f"{PARAMS['check_interval_s'] / 60} min",
            "positions_monitored": len(entries),
            "positions": entries,
            "recent_log": self.guard_log[-10:],
            "params": {
                "funding_close_threshold": f"{PARAMS['funding_close'] * 100}%/h",
                "max_hold_default": f"{PARAMS['max_hold_hours']}h",
                "max_hold_profit": f"{PARAMS['max_hold_hours_profit']}h",
            },
        }

    # ─────────────────────────────────────────────────────────
    # BACKGROUND LOOP
    # ─────────────────────────────────────────────────────────

    async def start_guard_loop(self):
        """Inicia el loop de monitoreo en background."""
        if self._loop_running:
            return
        self._loop_task = asyncio.create_task(
            self._guard_loop(), name="strategy_guard_loop"
        )
        self._loop_running = True
        self._log(
            f"Guard loop started. Interval: {PARAMS['check_interval_s'] / 60} min"
        )

    async def stop_guard_loop(self):
        """Detiene el loop de monitoreo."""
        if self._loop_task:
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass
        self._loop_running = False
        self._log("Guard loop stopped.")

    async def _guard_loop(self):
        """Loop interno — corre hasta ser cancelado."""
        while True:
            try:
                await self._check_all_guards()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._log(f"Loop error: {e}")

            await asyncio.sleep(PARAMS["check_interval_s"])

    async def _check_all_guards(self):
        """Chequea todas las posiciones bajo guardia."""
        if not self.guarded_positions:
            return

        # Iterar sobre copia de keys por si se modifica durante el loop
        for key in list(self.guarded_positions.keys()):
            g = self.guarded_positions.get(key)
            if not g or g["status"] != "ACTIVE":
                continue

            try:
                await self._check_single_guard(key, g)
            except Exception as e:
                self._log(f"ERROR checking {key}: {e}")

    async def _check_single_guard(self, key: str, g: dict):
        """Chequea una posición individual (R5-R8)."""
        now_ms = time.time() * 1000
        g["last_check"] = now_ms
        hold_hours = (now_ms - g["entry_time"]) / 3_600_000

        # ── Verificar si la posición sigue abierta ──────────
        try:
            pos_data = await self._get_trader(g["account"]).get_positions(
                symbol=g["symbol"]
            )
            positions = pos_data.get("positions", [])
            pos = next(
                (
                    p
                    for p in positions
                    if p.get("symbol") == g["symbol"]
                    and p.get("side") == "SHORT"
                ),
                None,
            )
        except Exception:
            pos = None

        if not pos:
            self._deactivate_guard(
                key, "Posición cerrada externamente (SL hit o cierre manual)"
            )
            return

        unrealized_pnl = float(pos.get("unrealized_pnl", 0))
        notional = abs(float(pos.get("notional_USDT", 0) or pos.get("notional", 0)))

        # ── R5: Verificar SL ────────────────────────────────
        try:
            orders_data = await self._get_trader(g["account"]).get_open_orders(
                symbol=g["symbol"]
            )
            orders = orders_data.get("orders", []) if isinstance(orders_data, dict) else []
            has_sl = any(
                "STOP" in (o.get("type", "") or o.get("orig_type", ""))
                for o in orders
            )
        except Exception:
            has_sl = True  # Asumir que existe para no spamear alertas si falla API

        if not has_sl:
            alert = {
                "time": now_ms,
                "rule": "R5",
                "message": f"⚠️ SIN STOP LOSS en {g['symbol']}. Colocar manualmente.",
            }
            g["alerts"].append(alert)
            self._log(f"ALERT R5: {key} — Sin SL")

        # ── R6: Funding ─────────────────────────────────────
        try:
            fund_data = await self.client.get_funding_rate(g["symbol"], limit=3)
            last_rate = fund_data[-1] if fund_data else {}
            r = last_rate.get("funding_rate", 0)
        except Exception:
            r = 0

        abs_r = abs(r)
        shorts_pay = r < 0

        if shorts_pay and abs_r > PARAMS["funding_close"]:
            cost_per_hour = abs_r * notional
            action = (
                "CERRANDO AUTOMÁTICAMENTE." if g["auto_close"]
                else "CERRAR MANUALMENTE."
            )
            alert = {
                "time": now_ms,
                "rule": "R6",
                "message": (
                    f"🔴 FUNDING EXTREMO: {r*100:.3f}%/h. "
                    f"Costo ${cost_per_hour:.2f}/h. {action}"
                ),
            }
            g["alerts"].append(alert)
            self._log(
                f"ALERT R6: {key} — Funding {r*100:.3f}%/h | "
                f"auto_close={g['auto_close']}"
            )

            if g["auto_close"]:
                await self._execute_close(g, "R6: Funding extremo")
                return
        elif shorts_pay and abs_r > PARAMS["funding_alert"]:
            g["alerts"].append({
                "time": now_ms,
                "rule": "R6",
                "message": f"🟡 Funding elevado: {r*100:.3f}%/h. Monitorear.",
            })

        # ── R7: Time stop ───────────────────────────────────
        max_h = (
            PARAMS["max_hold_hours_profit"]
            if unrealized_pnl > 0
            else g["max_hold_hours"]
        )

        if hold_hours > max_h:
            action = (
                "CERRANDO AUTOMÁTICAMENTE." if g["auto_close"]
                else "CERRAR MANUALMENTE."
            )
            alert = {
                "time": now_ms,
                "rule": "R7",
                "message": (
                    f"🔴 TIME STOP: {hold_hours:.1f}h > {max_h}h. "
                    f"PnL: ${unrealized_pnl:.2f}. {action}"
                ),
            }
            g["alerts"].append(alert)
            self._log(
                f"ALERT R7: {key} — {hold_hours:.1f}h > {max_h}h | "
                f"auto_close={g['auto_close']}"
            )

            if g["auto_close"]:
                await self._execute_close(g, "R7: Time stop excedido")
                return
        elif hold_hours > max_h * 0.80:
            remaining = max_h - hold_hours
            g["alerts"].append({
                "time": now_ms,
                "rule": "R7",
                "message": f"🟡 Time stop en {remaining:.1f}h.",
            })

        # ── R8: Reminder no DCA ─────────────────────────────
        if unrealized_pnl < 0:
            last_r8 = [a for a in g["alerts"] if a.get("rule") == "R8"]
            last_r8_time = last_r8[-1]["time"] if last_r8 else 0
            if now_ms - last_r8_time > 3_600_000:  # 1h entre alertas
                g["alerts"].append({
                    "time": now_ms,
                    "rule": "R8",
                    "message": (
                        f"PnL ${unrealized_pnl:.2f} — PROHIBIDO agregar size."
                    ),
                })

        # ── Trim alerts (max 50) ────────────────────────────
        if len(g["alerts"]) > 50:
            g["alerts"] = g["alerts"][-50:]

    # ─────────────────────────────────────────────────────────
    # INTERNAL HELPERS
    # ─────────────────────────────────────────────────────────

    async def _execute_close(self, guard: dict, reason: str):
        """Cierra posición automáticamente a mercado."""
        key = f"{guard['account']}:{guard['symbol']}"
        try:
            self._log(f"AUTO-CLOSE: {key} — {reason}")

            await self._get_trader(guard["account"]).close_position(
                symbol=guard["symbol"],
                order_type="MARKET",
            )

            guard["closes_executed"] += 1
            guard["alerts"].append({
                "time": time.time() * 1000,
                "rule": "CLOSE",
                "message": (
                    f"✅ Posición cerrada automáticamente. Razón: {reason}"
                ),
            })

            self.guard_log.append({
                "time": datetime.now(timezone.utc).isoformat(),
                "action": "AUTO_CLOSE",
                "key": key,
                "reason": reason,
            })

            self._deactivate_guard(key, f"Cerrada automáticamente: {reason}")

        except Exception as e:
            self._log(f"CLOSE FAILED: {key} — {e}")
            guard["alerts"].append({
                "time": time.time() * 1000,
                "rule": "CLOSE_FAILED",
                "message": (
                    f"❌ Cierre automático falló: {e}. CERRAR MANUALMENTE."
                ),
            })

    def _deactivate_guard(self, key: str, reason: str):
        """Desactiva y remueve un guard."""
        g = self.guarded_positions.get(key)
        if g:
            g["status"] = "DEACTIVATED"
            g["alerts"].append({
                "time": time.time() * 1000,
                "message": f"Guard desactivado: {reason}",
            })
            del self.guarded_positions[key]
            self._log(f"GUARD DEACTIVATED: {key} — {reason}")

    def _log(self, msg: str):
        """Log con timestamp."""
        entry = {
            "time": datetime.now(timezone.utc).isoformat(),
            "message": msg,
        }
        self.guard_log.append(entry)
        if len(self.guard_log) > 200:
            self.guard_log[:] = self.guard_log[-200:]
        logger.info("[GUARD] %s", msg)
