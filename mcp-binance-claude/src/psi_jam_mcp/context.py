"""
Contextualización de respuestas MCP para short trading.
========================================================
Cada función recibe el dict de datos crudos de una tool y devuelve
un dict 'context' con interpretación dinámica en español, siempre
orientada a operaciones short.

Los datos raw NO se modifican — el context se agrega como campo extra.
"""

from typing import Optional


# ═══════════════════════════════════════════════════
# PERFIL DE TRADES ÉLITE — Extraído del análisis retroactivo
# ═══════════════════════════════════════════════════
#
# Patrones ganadores históricos (cuenta principal, 30d, Feb 2026):
#
# Dos modos de operación exitosos:
#   1. SINGLE-SHOT: Pump parabólico >30% en <48h, volumen >10x promedio,
#      shortear DESPUÉS del pico visible (mechas superiores, velocidad
#      desacelerando). Un solo trade captura la reversión. (AGLD, ZRO, POWER)
#   2. MULTI-SCALP Régimen B: Después de un pump, cuando κ > F_ext (JAM),
#      la energía se disipó. Scalps cortos sobre cada rebote, 100% WR
#      consistente. (NOM 5/5, FIGHT 4/4, KITE 4/4, AZTEC 3/3)
#
# El multi-scalp Régimen B es el patrón MÁS CONSISTENTE (100% WR).
# El single-shot tiene mayor PnL/trade pero más varianza.
#
# Funding: cerrar antes del 2do ciclo (16h max). FIGHT pagó 1.3% de PnL,
# POWER pagó 18% — misma dirección correcta pero 14x peor eficiencia.
#
# Pump multi-asset: cuando varias alts pumpean juntas (ej: 11 Feb AGLD+ZRO),
# diversificar shorts en 2-3 tokens reduce riesgo individual.

ELITE_TRADE_CHECKLIST = [
    {"check": "volume_spike",    "label": "Volumen actual >10x promedio 7d",                   "weight": "crítico"},
    {"check": "pump_magnitude",  "label": "Pump >30% en <48h (altcoin low-cap)",               "weight": "alto"},
    {"check": "wicks",           "label": "Mechas superiores largas en últimas 2-3 velas 1h",  "weight": "alto"},
    {"check": "taker_fading",    "label": "Taker Buy Ratio cayendo (de >55% a <50%)",          "weight": "medio"},
    {"check": "regime_b",        "label": "Régimen B confirmado por JAM (κ > F_ext)",          "weight": "alto"},
    {"check": "funding_plan",    "label": "Plan: TP antes del 2do ciclo de funding (16h max)", "weight": "medio"},
    {"check": "multi_asset",     "label": "Si pump multi-asset → diversificar en 2-3 shorts",  "weight": "medio"},
    {"check": "no_anticipate",   "label": "NO anticipar el pico — esperar desaceleración",     "weight": "crítico"},
]

# Thresholds para scoring contra el perfil élite
ELITE_VOLUME_SPIKE_THRESHOLD = 10.0    # vol_ratio >= 10x = match perfil élite
ELITE_PUMP_PCT_THRESHOLD     = 30.0    # change >= 30% en 48h
ELITE_TAKER_WEAK_THRESHOLD   = -0.03   # taker_delta < -3% = buyers fading


# ═══════════════════════════════════════════════════
# REGLAS DE EXIT — Derivadas de Trades Élite
# ═══════════════════════════════════════════════════
#
# Anatomía del exit óptimo:
#   - El 80% de la ganancia se captura en el primer 20% del holding.
#   - Después de 6-8h, el retorno marginal cae pero el funding es constante.
#   - Trades con funding <$0.50 tuvieron eficiencia >95%.
#   - FIGHT (4/4, $0.33 funding) vs POWER ($4.96 funding): 14x más eficiente.
#
# Dos modos de exit:
#   SINGLE-SHOT: Capturar 40-60% del rango del pump y cerrar.
#   MULTI-SCALP: Pulsos de 2-4h, cerrar en cada soporte, re-entry en rechazo.
#
# El holding nocturno es el mayor destructor de PnL por funding silencioso.

EXIT_RULES = [
    {
        "id": "R1_WINDOW_8H",
        "label": "Ventana de 8 Horas",
        "description": (
            "Holding óptimo ≤8h. El 83% de trades élite (eficiencia >95%) cerraron en este período. "
            "Después de 8h, cada hora extra cuesta ~$0.20-0.50 en funding sin ganancia proporcional."
        ),
        "weight": "alto",
    },
    {
        "id": "R2_GREEN_CANDLE",
        "label": "Primera Vela Verde 1h con Volumen = Exit",
        "description": (
            "Cuando aparece vela verde en 1h con volumen >50% de las velas rojas previas, "
            "el dump terminó. Exit signal más consistente. No esperar segundo bounce."
        ),
        "weight": "crítico",
    },
    {
        "id": "R3_TARGET_40_60",
        "label": "Target: 40-60% del Rango del Pump",
        "description": (
            "Si pump fue $0.20→$0.40 (rango $0.20), TP en capturar $0.08-$0.12 de caída (40-60%). "
            "No buscar el fondo. Trades élite capturaron esta proporción."
        ),
        "weight": "alto",
    },
    {
        "id": "R4_MULTISCALP",
        "label": "Multi-Scalp > Single Hold en Régimen B",
        "description": (
            "En Régimen B, 3-5 scalps de 2-4h > un hold de 24-48h. "
            "PnL comparable, funding 10-30x menor. Re-entry en rechazo de resistencia."
        ),
        "weight": "medio",
    },
    {
        "id": "R5_FUNDING_PRESLEEP",
        "label": "Funding Check Pre-Sleep",
        "description": (
            "Si vas a dormir: calcular cobros de funding overnight (cada 8h). "
            "Si funding estimado >10% del unrealized profit → cerrar antes de dormir."
        ),
        "weight": "alto",
    },
    {
        "id": "R6_EFFICIENCY_STOP",
        "label": "Stop de Eficiencia: Funding >15% del PnL Bruto",
        "description": (
            "Si funding acumulado >15% del PnL bruto → exit forzado. "
            "La posición ya no es eficiente. Circuit breaker contra erosión gradual."
        ),
        "weight": "crítico",
    },
]

# Exit signal thresholds
EXIT_MAX_HOLDING_HOURS       = 8       # Regla 1: window óptimo
EXIT_PUMP_CAPTURE_MIN_PCT    = 40      # Regla 3: mínimo % del rango a capturar
EXIT_PUMP_CAPTURE_MAX_PCT    = 60      # Regla 3: máximo % (target ideal)
EXIT_FUNDING_EFFICIENCY_STOP = 0.15    # Regla 6: funding/pnl_bruto > 15% = exit
EXIT_FUNDING_PRESLEEP_LIMIT  = 0.10    # Regla 5: funding_estimado/profit > 10%
EXIT_GREEN_CANDLE_VOL_RATIO  = 0.50    # Regla 2: vela verde vol >= 50% de rojas previas
EXIT_TAKER_BUY_FLIP          = 0.55    # Taker buy ratio > 55% = buyers retomando
EXIT_VOLUME_COLLAPSE_PCT     = 70      # Volumen cayó >70% desde pico = agotamiento


def evaluate_exit(position: dict, market_data: dict) -> dict:
    """
    Evaluar si una posición SHORT abierta debe cerrarse.

    Args:
        position: dict con datos de la posición (symbol, side, entry_price,
                  mark_price, unrealized_pnl, holding_hours, etc.)
        market_data: dict con:
            - klines_1h: últimas ~24 velas 1h
            - taker_ratio: últimos datos de taker buy/sell
            - funding_paid: total funding pagado en esta posición
            - funding_rate_current: tasa de funding actual
            - ticker_24h: datos del ticker 24h (high, low, etc.)

    Returns:
        dict con evaluación completa: señales, árbol de decisión, recomendación.
    """
    symbol = position.get("symbol", "?")
    side = position.get("side", "")
    entry = position.get("entry_price", 0)
    mark = position.get("mark_price", 0)
    pnl = position.get("unrealized_pnl", 0)
    holding_h = position.get("holding_hours", 0)
    notional = position.get("notional_usdt", 0)

    klines = market_data.get("klines_1h", [])
    taker = market_data.get("taker_ratio", [])
    funding_paid = market_data.get("funding_paid", 0)
    funding_rate = market_data.get("funding_rate_current", 0)
    ticker = market_data.get("ticker_24h", {})

    signals = []
    decision_tree = []
    urgency = 0  # 0-100, higher = more urgent to exit

    # ── Señal A: Primera vela verde 1h con volumen ──
    green_candle_signal = False
    if klines and len(klines) >= 3:
        # Look at last 6 candles
        recent = klines[-6:]
        red_candles = [k for k in recent[:-1] if k["close"] < k["open"]]
        last = recent[-1]
        is_green = last["close"] > last["open"]

        if is_green and red_candles:
            avg_red_vol = sum(k["quote_volume"] for k in red_candles) / len(red_candles) if red_candles else 0
            if avg_red_vol > 0 and last["quote_volume"] >= avg_red_vol * EXIT_GREEN_CANDLE_VOL_RATIO:
                green_candle_signal = True
                signals.append({
                    "signal": "GREEN_CANDLE_WITH_VOLUME",
                    "severity": "CRITICAL",
                    "detail": (
                        f"Primera vela verde 1h con volumen significativo "
                        f"(${last['quote_volume']:,.0f} vs avg roja ${avg_red_vol:,.0f}). "
                        "Compradores entrando — el dump perdió momentum."
                    ),
                })
                urgency += 35

    # ── Señal B: Taker Buy Ratio flipping ──
    taker_flip = False
    if taker and len(taker) >= 2:
        latest_ratio = taker[-1].get("buy_sell_ratio", 1.0)
        prior_ratio = taker[-2].get("buy_sell_ratio", 1.0)
        if latest_ratio > EXIT_TAKER_BUY_FLIP:
            taker_flip = True
            signals.append({
                "signal": "TAKER_BUY_FLIP",
                "severity": "HIGH",
                "detail": (
                    f"Taker buy ratio en {latest_ratio:.3f} (>{EXIT_TAKER_BUY_FLIP}). "
                    f"Previo: {prior_ratio:.3f}. "
                    "Compradores agresivos superan vendedores — dump perdió fuerza."
                ),
            })
            urgency += 20

    # ── Señal C: Bounce en soporte pre-pump ──
    # Use 24h low as proxy for pre-pump level
    high_24h = ticker.get("high", 0)
    low_24h = ticker.get("low", 0)
    pump_range = high_24h - low_24h if high_24h > low_24h else 0

    pump_capture_pct = 0
    if pump_range > 0 and entry > 0 and mark > 0 and side == "SHORT":
        captured = entry - mark
        pump_capture_pct = (captured / pump_range * 100) if pump_range > 0 else 0

    if pump_capture_pct >= EXIT_PUMP_CAPTURE_MIN_PCT:
        signals.append({
            "signal": "TARGET_REACHED",
            "severity": "HIGH",
            "detail": (
                f"Capturado {pump_capture_pct:.1f}% del rango del pump "
                f"(rango: ${pump_range:.6g}, capturado: ${entry - mark:.6g}). "
                f"Target élite: 40-60%. {'En zona óptima de cierre.' if pump_capture_pct <= 70 else 'Excedido — considerar cierre inmediato.'}"
            ),
        })
        urgency += 15 if pump_capture_pct < EXIT_PUMP_CAPTURE_MAX_PCT else 25

    # ── Señal D: Volumen colapsando ──
    vol_collapse = False
    if klines and len(klines) >= 6:
        volumes = [k["quote_volume"] for k in klines[-12:]] if len(klines) >= 12 else [k["quote_volume"] for k in klines]
        peak_vol = max(volumes) if volumes else 0
        recent_vol = klines[-1]["quote_volume"]
        if peak_vol > 0:
            vol_drop_pct = (1 - recent_vol / peak_vol) * 100
            if vol_drop_pct >= EXIT_VOLUME_COLLAPSE_PCT:
                vol_collapse = True
                signals.append({
                    "signal": "VOLUME_COLLAPSE",
                    "severity": "MEDIUM",
                    "detail": (
                        f"Volumen cayó {vol_drop_pct:.0f}% desde el pico "
                        f"(pico: ${peak_vol:,.0f}, actual: ${recent_vol:,.0f}). "
                        "Movimiento en 'goteo' — no vale el costo de oportunidad."
                    ),
                })
                urgency += 10

    # ── Regla 1: Ventana de 8h ──
    if holding_h > EXIT_MAX_HOLDING_HOURS:
        signals.append({
            "signal": "HOLDING_EXCEEDED_8H",
            "severity": "HIGH",
            "detail": (
                f"Posición abierta hace {holding_h:.1f}h (límite óptimo: {EXIT_MAX_HOLDING_HOURS}h). "
                f"Cada hora extra tiene costo esperado de ~$0.20-0.50 en funding sin garantía de ganancia adicional."
            ),
        })
        urgency += min(20, (holding_h - EXIT_MAX_HOLDING_HOURS) * 3)
    elif holding_h > 6:
        signals.append({
            "signal": "HOLDING_WARNING",
            "severity": "MEDIUM",
            "detail": f"Posición abierta hace {holding_h:.1f}h — acercándose al límite de 8h.",
        })
        urgency += 5

    # ── Regla 6: Eficiencia de funding ──
    funding_efficiency = None
    pnl_bruto = abs(pnl) + abs(funding_paid)  # PnL bruto = what you'd have without funding
    if pnl_bruto > 0 and abs(funding_paid) > 0:
        funding_efficiency = abs(funding_paid) / pnl_bruto
        if funding_efficiency > EXIT_FUNDING_EFFICIENCY_STOP:
            signals.append({
                "signal": "FUNDING_EFFICIENCY_STOP",
                "severity": "CRITICAL",
                "detail": (
                    f"Funding acumulado ${abs(funding_paid):.2f} = "
                    f"{funding_efficiency * 100:.1f}% del PnL bruto ${pnl_bruto:.2f}. "
                    f"SUPERA LÍMITE DE {EXIT_FUNDING_EFFICIENCY_STOP * 100:.0f}%. EXIT FORZADO."
                ),
            })
            urgency += 40
        elif funding_efficiency > 0.08:
            signals.append({
                "signal": "FUNDING_ERODING",
                "severity": "HIGH",
                "detail": (
                    f"Funding acumulado ${abs(funding_paid):.2f} = "
                    f"{funding_efficiency * 100:.1f}% del PnL bruto. "
                    f"Acercándose al stop de eficiencia ({EXIT_FUNDING_EFFICIENCY_STOP * 100:.0f}%)."
                ),
            })
            urgency += 15

    # ── Regla 5: Funding pre-sleep (estimación próximas 8h = 1 cobro) ──
    next_funding_cost = 0
    if funding_rate != 0 and notional > 0:
        # funding_rate is per 8h cycle, cost = rate * notional
        next_funding_cost = abs(funding_rate * notional)
        if pnl > 0 and next_funding_cost > 0:
            funding_vs_profit = next_funding_cost / pnl
            if funding_vs_profit > EXIT_FUNDING_PRESLEEP_LIMIT:
                signals.append({
                    "signal": "FUNDING_PRESLEEP_WARNING",
                    "severity": "HIGH",
                    "detail": (
                        f"Próximo cobro de funding estimado: ${next_funding_cost:.2f} "
                        f"({funding_vs_profit * 100:.1f}% del profit actual ${pnl:.2f}). "
                        f"Supera límite pre-sleep de {EXIT_FUNDING_PRESLEEP_LIMIT * 100:.0f}%. "
                        "Considerar cerrar si no hay catalizador activo."
                    ),
                })
                urgency += 15

    # ── Consolidación lateral (>2h sin nuevo mínimo) ──
    if klines and len(klines) >= 3:
        last_3 = klines[-3:]
        lows = [k["low"] for k in last_3]
        if all(lows[i] >= lows[0] * 0.998 for i in range(1, len(lows))):
            # Lows are flat (+/- 0.2%) for 3 candles
            signals.append({
                "signal": "CONSOLIDATION",
                "severity": "MEDIUM",
                "detail": (
                    f"Precio consolidando hace ~{len(last_3)}h — sin nuevos mínimos. "
                    "Cada hora de consolidación es funding pagado sin progreso."
                ),
            })
            urgency += 10

    # ── Árbol de decisión ──
    decision_tree = []

    # Nodo 1: ¿Capturó >40% del rango?
    if pump_capture_pct >= EXIT_PUMP_CAPTURE_MIN_PCT:
        decision_tree.append({
            "question": "¿El dump acumuló >40% del rango del pump?",
            "answer": f"SÍ ({pump_capture_pct:.1f}%)",
            "action": "Considerar cerrar al menos 50% de la posición",
        })
    else:
        decision_tree.append({
            "question": "¿El dump acumuló >40% del rango del pump?",
            "answer": f"NO ({pump_capture_pct:.1f}%)",
            "action": "Puede mantener, pero seguir monitoreando",
        })

    # Nodo 2: ¿Vela verde con volumen?
    if green_candle_signal:
        decision_tree.append({
            "question": "¿Apareció vela verde 1h con volumen?",
            "answer": "SÍ",
            "action": "EXIT inmediato (al menos 75% de la posición)",
        })

    # Nodo 3: ¿Holding >6h?
    if holding_h > 6:
        consolidating = any(s["signal"] == "CONSOLIDATION" for s in signals)
        decision_tree.append({
            "question": f"¿Llevás >{EXIT_MAX_HOLDING_HOURS - 2}h de holding?",
            "answer": f"SÍ ({holding_h:.1f}h)",
            "action": "EXIT — consolidó >2h" if consolidating else "Movimiento sigue activo, pero acercándose al límite",
        })

    # Nodo 4: ¿Funding próximo no justificado?
    if next_funding_cost > 0 and pnl > 0 and next_funding_cost / pnl > EXIT_FUNDING_PRESLEEP_LIMIT:
        decision_tree.append({
            "question": "¿Se acerca cobro de funding y el profit no justifica?",
            "answer": f"SÍ (funding ~${next_funding_cost:.2f} vs profit ${pnl:.2f})",
            "action": "EXIT antes del cobro. Re-entrar después si setup sigue vivo.",
        })

    # Nodo 5: ¿Funding >15% del PnL bruto?
    if funding_efficiency and funding_efficiency > EXIT_FUNDING_EFFICIENCY_STOP:
        decision_tree.append({
            "question": "¿Funding acumulado >15% del PnL bruto?",
            "answer": f"SÍ ({funding_efficiency * 100:.1f}%)",
            "action": "EXIT FORZADO. La posición ya no es eficiente.",
        })

    # ── Veredicto final ──
    urgency = min(urgency, 100)
    critical_signals = [s for s in signals if s["severity"] == "CRITICAL"]
    high_signals = [s for s in signals if s["severity"] == "HIGH"]

    if urgency >= 60 or critical_signals:
        verdict = "EXIT_NOW"
        verdict_text = (
            f"EXIT RECOMENDADO AHORA para {symbol}. "
            f"{len(critical_signals)} señales críticas, {len(high_signals)} altas. "
            "La posición ha superado el punto de eficiencia óptima."
        )
    elif urgency >= 35 or len(high_signals) >= 2:
        verdict = "EXIT_SOON"
        verdict_text = (
            f"Preparar EXIT para {symbol}. "
            f"Múltiples señales de agotamiento. "
            "Cerrar al menos 50-75% de la posición en la próxima hora."
        )
    elif urgency >= 15:
        verdict = "MONITOR"
        verdict_text = (
            f"{symbol}: señales mixtas. "
            "Posición aún viable pero ajustar TP o reducir tamaño. "
            "Revisar en 1-2h."
        )
    else:
        verdict = "HOLD"
        verdict_text = (
            f"{symbol}: sin señales de exit. "
            "Posición sana — mantener con SL activo."
        )

    return {
        "symbol": symbol,
        "side": side,
        "entry_price": entry,
        "mark_price": mark,
        "unrealized_pnl": round(pnl, 4),
        "holding_hours": round(holding_h, 1),
        "funding_paid": round(abs(funding_paid), 4),
        "funding_efficiency_pct": round(funding_efficiency * 100, 1) if funding_efficiency else None,
        "pump_capture_pct": round(pump_capture_pct, 1),
        "next_funding_estimate": round(next_funding_cost, 4),
        "signals": signals,
        "decision_tree": decision_tree,
        "urgency": urgency,
        "verdict": verdict,
        "verdict_text": verdict_text,
        "exit_rules_reference": EXIT_RULES,
    }


# ═══════════════════════════════════════════════════
# MARKET DATA
# ═══════════════════════════════════════════════════

def contextualize_orderbook(data: dict) -> dict:
    """Interpretar order book L2 para short trading."""
    m = data.get("metrics", {})
    ctx = {}

    imb = m.get("top10_imbalance", 0)
    if imb < -0.15:
        ctx["imbalance"] = f"Imbalance {imb:.2f} — presión vendedora dominante. Asks superan bids, favorable para short."
    elif imb < -0.05:
        ctx["imbalance"] = f"Imbalance {imb:.2f} — leve presión vendedora."
    elif imb > 0.15:
        ctx["imbalance"] = f"Imbalance {imb:.2f} — presión compradora fuerte. Bids dominan, podría dificultar short a corto plazo."
    elif imb > 0.05:
        ctx["imbalance"] = f"Imbalance {imb:.2f} — leve presión compradora."
    else:
        ctx["imbalance"] = f"Imbalance {imb:.2f} — book equilibrado, sin presión direccional clara."

    ratio = m.get("depth_ratio", 1)
    if ratio < 0.8:
        ctx["depth"] = f"Depth ratio {ratio:.2f} — más profundidad en asks vs bids. Selling pressure estructural."
    elif ratio > 1.2:
        ctx["depth"] = f"Depth ratio {ratio:.2f} — más profundidad en bids vs asks. Soporte fuerte, short más difícil."
    else:
        ctx["depth"] = f"Depth ratio {ratio:.2f} — profundidad equilibrada."

    spread = m.get("spread_bps", 0)
    if spread > 5:
        ctx["spread"] = f"Spread {spread:.1f} bps — amplio, baja liquidez. Cuidado con slippage al entrar/salir short."
    elif spread > 2:
        ctx["spread"] = f"Spread {spread:.1f} bps — moderado."
    else:
        ctx["spread"] = f"Spread {spread:.1f} bps — tight, buena liquidez para operar."

    # Walls
    bid_walls = m.get("bid_walls", [])
    ask_walls = m.get("ask_walls", [])
    wall_notes = []
    if bid_walls:
        w = bid_walls[0]
        wall_notes.append(f"Soporte (bid wall) en ${w['price']:.4g} ({w['distance_pct']:.1f}% del mid, {w['qty']:.1f} contratos). Si rompe, aceleración bajista.")
    if ask_walls:
        w = ask_walls[0]
        wall_notes.append(f"Resistencia (ask wall) en ${w['price']:.4g} ({w['distance_pct']:.1f}% del mid). Techo para el precio, favorable para short.")
    if wall_notes:
        ctx["walls"] = " | ".join(wall_notes)

    return ctx


def contextualize_premium_index(data: dict) -> dict:
    """Interpretar premium index: mark/index spread, funding, y countdown al próximo funding."""
    import time as _time

    ctx = {}

    # Single symbol
    if "mark_price" in data:
        items = [data]
    else:
        items = data.get("symbols", [])
        if not items:
            return {"premium_index": "Sin datos de premium index."}

    # If it's a bulk query, summarize top negative funding
    if len(items) > 1:
        sorted_by_funding = sorted(items, key=lambda x: x.get("last_funding_rate", 0))
        top_neg = [s for s in sorted_by_funding if s.get("last_funding_rate", 0) < 0][:10]
        top_pos = [s for s in sorted_by_funding if s.get("last_funding_rate", 0) > 0][-5:][::-1]

        if top_neg:
            neg_lines = [f"  {s['symbol']}: {s['last_funding_rate']*100:.4f}%" for s in top_neg[:5]]
            ctx["top_negative_funding"] = "Tokens con funding más negativo (shorts pagan a longs):\n" + "\n".join(neg_lines)

        if top_pos:
            pos_lines = [f"  {s['symbol']}: {s['last_funding_rate']*100:.4f}%" for s in top_pos[:5]]
            ctx["top_positive_funding"] = "Tokens con funding más positivo (longs pagan a shorts):\n" + "\n".join(pos_lines)

        # Next funding time from first item
        nft = items[0].get("next_funding_time", 0)
        if nft > 0:
            now_ms = int(_time.time() * 1000)
            remaining_ms = nft - now_ms
            if remaining_ms > 0:
                mins = remaining_ms // 60_000
                hrs = mins // 60
                mins_rem = mins % 60
                ctx["next_funding"] = f"Próximo funding en {hrs}h {mins_rem}m."
            else:
                ctx["next_funding"] = "Funding acaba de ejecutarse."
        return ctx

    # Single symbol detail
    s = items[0]
    mark = s.get("mark_price", 0)
    index = s.get("index_price", 0)
    settle = s.get("estimated_settle_price", 0)
    rate = s.get("last_funding_rate", 0)
    nft = s.get("next_funding_time", 0)

    # Mark vs Index spread (premium/discount)
    if index > 0 and mark > 0:
        spread_pct = (mark - index) / index * 100
        if spread_pct > 0.1:
            ctx["premium"] = f"Mark ${mark:.4g} vs Index ${index:.4g} — premium de {spread_pct:+.3f}%. Futuros cotizan por encima del spot."
        elif spread_pct < -0.1:
            ctx["premium"] = f"Mark ${mark:.4g} vs Index ${index:.4g} — descuento de {spread_pct:+.3f}%. Futuros cotizan por debajo del spot."
        else:
            ctx["premium"] = f"Mark ${mark:.4g} ≈ Index ${index:.4g} — sin premium/descuento significativo ({spread_pct:+.3f}%)."

    # Funding
    rate_pct = rate * 100
    if rate > 0.0005:
        ctx["funding"] = f"Funding {rate_pct:.4f}% — MUY positivo. Longs pagan a shorts."
    elif rate > 0.0001:
        ctx["funding"] = f"Funding {rate_pct:.4f}% — positivo. Longs pagan a shorts."
    elif rate < -0.0005:
        ctx["funding"] = f"Funding {rate_pct:.4f}% — MUY negativo. Shorts pagan a longs. Oportunidad carry."
    elif rate < -0.0001:
        ctx["funding"] = f"Funding {rate_pct:.4f}% — negativo. Shorts pagan a longs."
    else:
        ctx["funding"] = f"Funding {rate_pct:.4f}% — neutral."

    # Next funding countdown
    if nft > 0:
        now_ms = int(_time.time() * 1000)
        remaining_ms = nft - now_ms
        if remaining_ms > 0:
            total_secs = remaining_ms // 1000
            hrs = total_secs // 3600
            mins_rem = (total_secs % 3600) // 60
            secs_rem = total_secs % 60
            ctx["next_funding"] = f"Próximo funding en {hrs}h {mins_rem}m {secs_rem}s."
            if remaining_ms < 1800_000:  # < 30 min
                ctx["urgency"] = "⚠ Funding inminente (< 30 min). Posición debe estar abierta ANTES del snapshot para cobrar/pagar."
            elif remaining_ms < 3600_000:  # < 1 hour
                ctx["urgency"] = "Funding en menos de 1 hora. Tiempo suficiente para abrir posición si se desea capturar este intervalo."
        else:
            ctx["next_funding"] = "Funding acaba de ejecutarse. Próximo en ~8 horas."

    return ctx


def contextualize_funding(data: dict) -> dict:
    """Interpretar funding rate para short trading."""
    rates = data.get("funding_rates", [])
    if not rates:
        return {"funding": "Sin datos de funding disponibles."}

    last = rates[-1]
    rate = last.get("rate") or last.get("funding_rate", 0)
    ctx = {}

    if rate > 0.0005:
        ctx["current"] = f"Funding {rate*100:.4f}% — MUY positivo. Longs pagan a shorts. Holding short es muy rentable. Longs sobreapalancados."
    elif rate > 0.0001:
        ctx["current"] = f"Funding {rate*100:.4f}% — positivo. Longs pagan a shorts. Mantener short genera ingreso por funding."
    elif rate < -0.0005:
        ctx["current"] = f"Funding {rate*100:.4f}% — MUY negativo. Shorts pagan a longs. Alto costo de mantener short."
    elif rate < -0.0001:
        ctx["current"] = f"Funding {rate*100:.4f}% — negativo. Shorts pagan a longs. Costo de carry para short."
    else:
        ctx["current"] = f"Funding {rate*100:.4f}% — neutral, sin sesgo significativo."

    # Tendencia
    if len(rates) >= 3:
        recent = [r.get("rate") or r.get("funding_rate", 0) for r in rates[-5:]]
        trend = recent[-1] - recent[0]
        if trend > 0.0001:
            ctx["trend"] = "Funding subiendo — cada vez más cuentas long. Crowd posicionándose alcista, potencial squeeze si revierte."
        elif trend < -0.0001:
            ctx["trend"] = "Funding bajando — presión long disminuye o shorts crecen."
        else:
            ctx["trend"] = "Funding estable, sin cambio de tendencia."

    return ctx


def contextualize_long_short_ratio(data: dict) -> dict:
    """Interpretar ratio long/short para short trading."""
    ratios = data.get("ratios", [])
    if not ratios:
        return {"ratio": "Sin datos de L/S ratio."}

    last = ratios[-1]
    ls = last.get("long_short_ratio") or last.get("long_account", 0)
    long_pct = last.get("long_account", 0)
    short_pct = last.get("short_account", 0)
    ctx = {}

    if long_pct and short_pct:
        long_val = float(long_pct) * 100
        short_val = float(short_pct) * 100
    elif ls:
        ls = float(ls)
        long_val = ls / (1 + ls) * 100
        short_val = 100 - long_val
    else:
        return {"ratio": "Datos insuficientes para interpretar."}

    if long_val > 60:
        ctx["positioning"] = f"Longs {long_val:.0f}% vs Shorts {short_val:.0f}% — crowd muy posicionada long. Operar contra la mayoría (short) históricamente favorable."
    elif long_val > 52:
        ctx["positioning"] = f"Longs {long_val:.0f}% vs Shorts {short_val:.0f}% — sesgo long moderado."
    elif short_val > 60:
        ctx["positioning"] = f"Shorts {short_val:.0f}% vs Longs {long_val:.0f}% — short crowded. CUIDADO: riesgo de short squeeze."
    elif short_val > 52:
        ctx["positioning"] = f"Shorts {short_val:.0f}% vs Longs {long_val:.0f}% — sesgo short moderado. Posición no tan contrarian."
    else:
        ctx["positioning"] = f"Longs {long_val:.0f}% vs Shorts {short_val:.0f}% — mercado dividido, sin sesgo claro."

    # Tendencia
    if len(ratios) >= 3:
        recent_ls = []
        for r in ratios[-5:]:
            v = r.get("long_account") or r.get("long_short_ratio", 0)
            recent_ls.append(float(v))
        if recent_ls[-1] > recent_ls[0] * 1.03:
            ctx["trend"] = "Más cuentas abriendo longs — si el precio cae, liquidación en cadena favorece short."
        elif recent_ls[-1] < recent_ls[0] * 0.97:
            ctx["trend"] = "Longs cerrando posiciones — presión vendedora creciente."

    return ctx


def contextualize_taker_volume(data: dict) -> dict:
    """Interpretar taker buy/sell ratio para short trading."""
    ratios = data.get("ratios", [])
    if not ratios:
        return {"taker": "Sin datos de taker volume."}

    last = ratios[-1]
    bsr = float(last.get("buy_sell_ratio", 1))
    ctx = {}

    if bsr < 0.85:
        ctx["aggression"] = f"Ratio taker {bsr:.2f} — ventas agresivas dominan fuertemente. Presión bajista activa, favorable para short."
    elif bsr < 0.95:
        ctx["aggression"] = f"Ratio taker {bsr:.2f} — ventas agresivas superan compras. Presión bajista presente."
    elif bsr > 1.15:
        ctx["aggression"] = f"Ratio taker {bsr:.2f} — compras agresivas dominan. Momentum alcista activo, esperar agotamiento antes de short."
    elif bsr > 1.05:
        ctx["aggression"] = f"Ratio taker {bsr:.2f} — compras ligeramente dominan."
    else:
        ctx["aggression"] = f"Ratio taker {bsr:.2f} — equilibrado, sin dirección agresiva clara."

    # Tendencia
    if len(ratios) >= 3:
        recent = [float(r.get("buy_sell_ratio", 1)) for r in ratios[-5:]]
        if recent[-1] < recent[0] * 0.95:
            ctx["trend"] = "Ratio compra/venta cayendo — compras debilitándose, momentum alcista se agota. Favorable para short."
        elif recent[-1] > recent[0] * 1.05:
            ctx["trend"] = "Ratio compra/venta subiendo — compras intensificándose. No ideal para short ahora."

    return ctx


def contextualize_open_interest(data: dict, price_data: dict = None) -> dict:
    """Interpretar open interest para short trading."""
    ctx = {}

    # Caso REST single
    if "open_interest" in data and not data.get("history"):
        oi = data.get("open_interest", 0)
        ctx["current"] = f"Open Interest: {float(oi):,.0f} contratos."
        return ctx

    # Caso histórico
    history = data.get("history", [])
    if not history or len(history) < 2:
        return {"oi": "Datos insuficientes de OI histórico."}

    last_oi = float(history[-1].get("sum_open_interest", 0))
    first_oi = float(history[0].get("sum_open_interest", 0))
    oi_change = (last_oi - first_oi) / first_oi * 100 if first_oi > 0 else 0

    if oi_change > 5:
        ctx["oi_trend"] = f"OI subiendo {oi_change:.1f}% — nuevas posiciones abriéndose. Si el precio sube simultáneamente, nuevos longs acumulándose (potencial squeeze bajista si revierte)."
    elif oi_change < -5:
        ctx["oi_trend"] = f"OI bajando {oi_change:.1f}% — posiciones cerrándose. Potencial agotamiento del movimiento actual."
    else:
        ctx["oi_trend"] = f"OI estable ({oi_change:+.1f}%). Sin acumulación significativa de nuevas posiciones."

    return ctx


def contextualize_ticker(data: dict) -> dict:
    """Interpretar ticker 24h para short trading."""
    ctx = {}

    pct = float(data.get("price_change_percent", data.get("priceChangePercent", 0)))
    vol = float(data.get("quote_volume", data.get("quoteVolume", 0)))

    if pct > 10:
        ctx["price"] = f"Subida {pct:+.1f}% en 24h — MUY sobreextendido al alza. Candidato fuerte para reversión/short."
    elif pct > 5:
        ctx["price"] = f"Subida {pct:+.1f}% en 24h — sobreextendido, posible reversión. Buscar señales de agotamiento para short."
    elif pct > 2:
        ctx["price"] = f"Subida {pct:+.1f}% en 24h — movimiento moderado. Evaluar si hay fundamento o si es pump."
    elif pct < -10:
        ctx["price"] = f"Caída {pct:+.1f}% en 24h — movimiento bajista fuerte. Short podría estar tardío, considerar esperar retrace para entrar."
    elif pct < -5:
        ctx["price"] = f"Caída {pct:+.1f}% en 24h — tendencia bajista activa. Short con momentum a favor."
    elif pct < -2:
        ctx["price"] = f"Caída {pct:+.1f}% en 24h — presión bajista moderada."
    else:
        ctx["price"] = f"Cambio {pct:+.1f}% en 24h — sin movimiento direccional significativo."

    if vol > 0:
        vol_m = vol / 1_000_000
        ctx["volume"] = f"Volumen 24h: ${vol_m:.1f}M USDT."

    return ctx


def contextualize_trades(data: dict) -> dict:
    """Interpretar trades recientes para short trading."""
    trades = data.get("trades", [])
    if not trades:
        return {"trades": "Sin trades recientes."}

    total = len(trades)
    buyer_maker_count = sum(1 for t in trades if t.get("is_buyer_maker", False))
    seller_aggressor_pct = buyer_maker_count / total * 100 if total > 0 else 50

    # is_buyer_maker=True → el taker fue VENDEDOR (venta agresiva)
    ctx = {}
    if seller_aggressor_pct > 60:
        ctx["aggression"] = f"Últimos {total} trades: {seller_aggressor_pct:.0f}% fueron ventas agresivas. Sellers dominan, favorable para short."
    elif seller_aggressor_pct > 52:
        ctx["aggression"] = f"Últimos {total} trades: {seller_aggressor_pct:.0f}% ventas agresivas. Leve presión vendedora."
    elif seller_aggressor_pct < 40:
        ctx["aggression"] = f"Últimos {total} trades: {100-seller_aggressor_pct:.0f}% fueron compras agresivas. Buyers dominan, cautela con short."
    else:
        ctx["aggression"] = f"Últimos {total} trades: agresividad equilibrada (ventas {seller_aggressor_pct:.0f}%)."

    return ctx


def contextualize_klines(data: dict) -> dict:
    """Interpretar klines/velas para short trading."""
    klines = data.get("klines") or data.get("data")
    if not klines:
        return {}

    # Si es multi-tf, contextualizar el dict
    if isinstance(klines, dict):
        return _contextualize_multi_tf_klines(klines)

    if len(klines) < 5:
        return {}

    recent = klines[-5:]
    closes = [k["close"] for k in recent]
    volumes = [k["volume"] for k in recent]

    ctx = {}

    # Tendencia reciente
    if closes[-1] < closes[0]:
        pct = (closes[-1] - closes[0]) / closes[0] * 100
        ctx["trend"] = f"Últimas 5 velas: bajando {pct:.2f}%. Momentum bajista activo."
    else:
        pct = (closes[-1] - closes[0]) / closes[0] * 100
        ctx["trend"] = f"Últimas 5 velas: subiendo {pct:+.2f}%."

    # Volumen
    avg_vol = sum(volumes) / len(volumes) if volumes else 0
    if volumes[-1] > avg_vol * 1.5 and closes[-1] < closes[-2]:
        ctx["vol_signal"] = "Última vela bajista con volumen alto (>1.5× promedio). Venta con convicción."
    elif volumes[-1] > avg_vol * 1.5 and closes[-1] > closes[-2]:
        ctx["vol_signal"] = "Última vela alcista con volumen alto. Compras activas, no ideal para short inmediato."

    return ctx


def _contextualize_multi_tf_klines(data: dict) -> dict:
    """Interpretar klines multi-timeframe."""
    directions = {}
    for tf, klines in data.items():
        if not klines or len(klines) < 2:
            continue
        closes = [k["close"] for k in klines[-5:]]
        if closes[-1] < closes[0]:
            directions[tf] = "bajista"
        elif closes[-1] > closes[0]:
            directions[tf] = "alcista"
        else:
            directions[tf] = "neutral"

    bearish_tfs = [tf for tf, d in directions.items() if d == "bajista"]
    bullish_tfs = [tf for tf, d in directions.items() if d == "alcista"]

    ctx = {}
    if len(bearish_tfs) > len(bullish_tfs):
        ctx["alignment"] = f"Mayoría de TFs bajistas ({', '.join(bearish_tfs)}). Confluencia favorable para short."
    elif len(bullish_tfs) > len(bearish_tfs):
        ctx["alignment"] = f"Mayoría de TFs alcistas ({', '.join(bullish_tfs)}). Short contra tendencia multi-TF, mayor riesgo."
    else:
        ctx["alignment"] = "TFs divididos, sin confluencia direccional clara."

    return ctx


# ═══════════════════════════════════════════════════
# ANALYSIS
# ═══════════════════════════════════════════════════

def contextualize_hurst(data: dict) -> dict:
    """Interpretar Hurst exponent para short trading."""
    h = data.get("hurst")
    regime = data.get("regime", "")
    if h is None:
        return {}

    ctx = {}
    if h < 0.4:
        ctx["hurst"] = f"H = {h:.3f} — anti-persistente. Los movimientos tienden a revertir. Pumps se agotan rápido → favorable para short en sobreextensiones."
    elif h < 0.48:
        ctx["hurst"] = f"H = {h:.3f} — ligeramente anti-persistente. Tendencia a mean-reversion moderada."
    elif h > 0.65:
        ctx["hurst"] = f"H = {h:.3f} — fuertemente persistente. Las tendencias se auto-refuerzan. Si está bajando, short tiene momentum. Si sube, NO shortear contra esta tendencia."
    elif h > 0.52:
        ctx["hurst"] = f"H = {h:.3f} — ligeramente persistente. Tendencia moderada a continuar."
    else:
        ctx["hurst"] = f"H = {h:.3f} — random walk. Sin edge direccional claro desde la estructura del proceso."

    return ctx


def contextualize_vpin(data: dict) -> dict:
    """Interpretar VPIN para short trading."""
    level = data.get("toxicity_level", "")
    z = data.get("vpin_z_score", 0)
    vpin = data.get("vpin_current", 0)
    ctx = {}

    if level == "HIGH" or (z and z > 2):
        ctx["toxicity"] = f"VPIN alto (z-score {z:.1f}) — flow tóxico. Traders informados activos. Alta probabilidad de movimiento brusco inminente. Si hay señales bajistas, short con convicción."
    elif level == "ELEVATED" or (z and z > 1):
        ctx["toxicity"] = f"VPIN elevado (z-score {z:.1f}) — actividad informada creciente. Estar alerta a movimiento direccional próximo."
    elif level == "NORMAL":
        ctx["toxicity"] = f"VPIN normal (z-score {z:.1f}) — sin evidencia de información asimétrica fuerte. Mercado en condiciones normales."
    else:
        ctx["toxicity"] = f"VPIN: z-score {z:.1f}. Nivel: {level}."

    return ctx


def contextualize_kramers_moyal(data: dict) -> dict:
    """Interpretar Kramers-Moyal para short trading."""
    d1 = data.get("D1_mean_drift")
    d2 = data.get("D2_mean_diffusion")
    pawula = data.get("pawula_ratio")
    ctx = {}

    if d1 is not None:
        if d1 < -0.001:
            ctx["drift"] = f"D1 (drift) = {d1:.6f} — drift negativo promedio. La dinámica empuja el precio hacia abajo. Favorable para short."
        elif d1 > 0.001:
            ctx["drift"] = f"D1 (drift) = {d1:.6f} — drift positivo. Tendencia ascendente en la dinámica. Short contra el drift requiere timing preciso."
        else:
            ctx["drift"] = f"D1 (drift) ≈ 0 — sin sesgo direccional en la dinámica. El precio difunde sin atractor claro."

    if d2 is not None:
        if d2 > 0.001:
            ctx["volatility"] = f"D2 (difusión) = {d2:.6f} — alta volatilidad. Movimientos amplios probables. Usar stops más amplios para short."
        elif d2 < 0.0001:
            ctx["volatility"] = f"D2 (difusión) = {d2:.6f} — baja volatilidad. Movimientos comprimidos, posible expansión inminente."

    if pawula is not None and data.get("is_langevin_valid") is False:
        ctx["model"] = "Proceso NO es Langevin válido (Pawula ratio alto). Dinámica con saltos, no solo difusión. Riesgo de movimientos discontinuos."

    # Potential wells
    wells = data.get("potential_wells", [])
    if wells:
        well_strs = [f"{w['position']:.6f}" for w in wells[:3]]
        ctx["attractors"] = f"Pozos de potencial en log-returns: [{', '.join(well_strs)}]. El precio tiende a gravitar hacia estos niveles de retorno."

    return ctx


def contextualize_lyapunov(data: dict) -> dict:
    """Interpretar Lyapunov exponent para short trading."""
    lam = data.get("lyapunov")
    regime = data.get("regime", "")
    if lam is None:
        return {}

    ctx = {}
    if lam > 0.05:
        ctx["chaos"] = f"λ = {lam:.4f} — sistema caótico. Altamente sensible a condiciones iniciales. Movimientos impredecibles posibles. Reducir tamaño de posición, stops amplios."
    elif lam > 0:
        ctx["chaos"] = f"λ = {lam:.4f} — inestabilidad incipiente. Dinámica en zona de transición."
    elif lam < -0.05:
        ctx["chaos"] = f"λ = {lam:.4f} — sistema estable. Precio en atractor, tiende a revertir a equilibrio. Short en desviaciones fuertes del equilibrio."
    else:
        ctx["chaos"] = f"λ ≈ 0 — marginalamente estable. Sistema en el borde del orden/caos."

    return ctx


def contextualize_rqa(data: dict) -> dict:
    """Interpretar RQA para short trading."""
    lam = data.get("laminarity", 0)
    det = data.get("determinism", 0)
    tt = data.get("trapping_time", 0)
    ctx = {}

    if lam > 0.7:
        ctx["laminarity"] = f"LAM = {lam:.3f} — alta laminaridad. Sistema acercándose a estado congelado. Señal pre-crash: el precio puede romperse violentamente. Short preparado para ruptura."
    elif lam > 0.4:
        ctx["laminarity"] = f"LAM = {lam:.3f} — laminaridad moderada. Dinámica parcialmente atrapada."
    else:
        ctx["laminarity"] = f"LAM = {lam:.3f} — baja laminaridad. Dinámica fluida, sin señal de congelamiento."

    if det > 0.7:
        ctx["determinism"] = f"DET = {det:.3f} — alta determinismo. Estructura no-aleatoria fuerte. Patrones predecibles presentes."
    elif det < 0.3:
        ctx["determinism"] = f"DET = {det:.3f} — bajo determinismo. Comportamiento más aleatorio, menos predecible."

    if tt > 5:
        ctx["trapping"] = f"TT = {tt:.1f} — tiempos de atrapamiento largos. El sistema se estanca en estados antes de moverse. Potencial de movimiento explosivo al liberarse."

    return ctx


def contextualize_kyles_lambda(data: dict) -> dict:
    """Interpretar Kyle's lambda para short trading."""
    lam = data.get("lambda")
    trend = data.get("lambda_trend", "")
    ctx = {}

    if lam is not None:
        if lam > 0:
            ctx["impact"] = f"λ = {lam:.6f} — cada unidad de volumen neto mueve el precio. Impacto positivo."
        if trend == "INCREASING":
            ctx["liquidity"] = "λ creciente — liquidez deteriorándose. Slippage aumentando. Considerar reducir tamaño de entrada short o usar limit orders."
        elif trend == "STABLE":
            ctx["liquidity"] = "λ estable — liquidez consistente. Condiciones normales de ejecución."
        elif trend == "DECREASING":
            ctx["liquidity"] = "λ decreciente — liquidez mejorando. Buen momento para ejecutar órdenes."

    return ctx


def contextualize_jam_regime(data: dict) -> dict:
    """Interpretar JAM regime para short trading (capa adicional al regime_description existente)."""
    regime = data.get("regime", "")
    criteria = data.get("criteria", {})
    langevin = data.get("langevin_params", {})
    current = data.get("current_state", {})
    ctx = {}

    if regime == "B":
        ctx["short_signal"] = "RÉGIMEN B — PUMP FALLIDO. Condición ideal para short. El impulso no se auto-sostiene: la energía inyectada se disipa rápidamente."
        # Detalle de por qué falla
        notes = []
        if not criteria.get("delta_strong"):
            notes.append("Delta débil: la fuerza no tiene dirección clara")
        if not criteria.get("retention_high"):
            notes.append("Retención baja (γ alto): la energía del pump se consume rápido")
        if not criteria.get("absorption_low"):
            notes.append("Absorción alta (κ restaura): mean-reversion activa contra el pump")
        if notes:
            ctx["failure_detail"] = ". ".join(notes) + "."
    elif regime == "A":
        ctx["short_signal"] = "RÉGIMEN A — IMPULSO SOSTENIDO. NO shortear contra este impulso. El sistema tiene F_ext dominante, baja disipación, y no hay mean-reversion. Esperar transición a Régimen B o agotamiento."
        delta = current.get("delta_mean_last5", 0.5)
        if delta < 0.4:
            ctx["direction"] = "Delta < 0.5 — impulso sostenido BAJISTA. Short tiene el momentum del Régimen A a favor."
    else:
        ctx["short_signal"] = "NEUTRAL — sin impulso significativo. F_ext por debajo del umbral. Buscar señales técnicas para timing de short."

    gamma = langevin.get("gamma_eff_damping", 0)
    if gamma > 5:
        ctx["damping"] = f"γ = {gamma:.1f} — disipación MUY rápida. Cualquier pump se agota en pocas velas. Favorable para fading/short."
    elif gamma > 2:
        ctx["damping"] = f"γ = {gamma:.1f} — disipación moderada-alta. Pumps pierden fuerza relativamente rápido."

    return ctx


def contextualize_full_pipeline(data: dict) -> dict:
    """Sintetizar todo el pipeline Ψ-jam en un veredicto short."""
    ctx = {}
    signals_pro_short = []
    signals_contra_short = []

    # Hurst
    h_data = data.get("hurst", {})
    h = h_data.get("hurst")
    if h is not None:
        if h < 0.45:
            signals_pro_short.append(f"Hurst anti-persistente ({h:.2f}): pumps revierten")
        elif h > 0.6:
            regime = h_data.get("regime", "")
            signals_contra_short.append(f"Hurst persistente ({h:.2f}): trending market")

    # Lyapunov
    ly_data = data.get("lyapunov", {})
    ly = ly_data.get("lyapunov")
    if ly is not None and ly > 0.05:
        signals_contra_short.append(f"λ caótico ({ly:.3f}): impredecible, riesgo alto")

    # VPIN
    vpin_data = data.get("vpin", {})
    vlevel = vpin_data.get("toxicity_level", "")
    vz = vpin_data.get("vpin_z_score", 0)
    if vlevel == "HIGH" or (vz and vz > 2):
        signals_pro_short.append(f"VPIN tóxico (z={vz:.1f}): informed traders activos")
    elif vlevel == "ELEVATED":
        signals_pro_short.append(f"VPIN elevado (z={vz:.1f}): actividad informada creciente")

    # JAM Regime
    jam = data.get("jam_regime", {})
    regime = jam.get("regime", "")
    if regime == "B":
        signals_pro_short.append("Régimen B: pump fallido, impulso no sostenido")
    elif regime == "A":
        delta = jam.get("current_state", {}).get("delta_mean_last5", 0.5)
        if delta > 0.6:
            signals_contra_short.append("Régimen A alcista: impulso sostenido al alza")
        elif delta < 0.4:
            signals_pro_short.append("Régimen A bajista: impulso sostenido a la baja")

    # RQA
    rqa = data.get("rqa", {})
    lam_rqa = rqa.get("laminarity", 0)
    if lam_rqa > 0.7:
        signals_pro_short.append(f"Laminaridad alta ({lam_rqa:.2f}): pre-crash, ruptura inminente")

    # Kramers-Moyal
    km = data.get("kramers_moyal", {})
    d1 = km.get("D1_mean_drift")
    if d1 is not None and d1 < -0.001:
        signals_pro_short.append(f"Drift negativo ({d1:.5f}): dinámica empuja precio abajo")
    elif d1 is not None and d1 > 0.001:
        signals_contra_short.append(f"Drift positivo ({d1:.5f}): dinámica empuja precio arriba")

    # Composite risk
    comp = data.get("composite_risk", {})
    risk_level = comp.get("level", "")

    # Build thesis
    total_signals = len(signals_pro_short) + len(signals_contra_short)
    pro = len(signals_pro_short)
    contra = len(signals_contra_short)

    if pro >= 3 and contra == 0:
        confidence = "ALTA"
        thesis = f"Confluencia fuerte para short ({pro} señales favorables, 0 en contra). "
    elif pro >= 2 and contra <= 1:
        confidence = "MODERADA"
        thesis = f"Condiciones favorables para short ({pro} señales a favor, {contra} en contra). "
    elif contra >= 2 and pro <= 1:
        confidence = "BAJA"
        thesis = f"Condiciones NO favorables para short ({contra} señales en contra, {pro} a favor). "
    elif pro == 0 and contra == 0:
        confidence = "NEUTRAL"
        thesis = "Sin señales fuertes en ninguna dirección. Mercado en condiciones neutrales. "
    else:
        confidence = "MIXTA"
        thesis = f"Señales mixtas ({pro} a favor, {contra} en contra de short). Selectividad requerida. "

    if signals_pro_short:
        thesis += "A favor: " + "; ".join(signals_pro_short) + ". "
    if signals_contra_short:
        thesis += "En contra: " + "; ".join(signals_contra_short) + "."

    ctx["short_thesis"] = thesis.strip()
    ctx["confidence"] = confidence
    ctx["signals_pro_short"] = signals_pro_short
    ctx["signals_contra_short"] = signals_contra_short
    if risk_level:
        ctx["risk_level"] = risk_level

    return ctx


# ═══════════════════════════════════════════════════
# TECHNICAL ANALYSIS
# ═══════════════════════════════════════════════════

def contextualize_technical_analysis(data: dict) -> dict:
    """Interpretar análisis técnico multi-TF para short trading."""
    ctx = {}

    alignment = data.get("multi_tf_alignment", "")
    biases = data.get("tf_biases", {})
    analysis = data.get("analysis", {})

    # Alignment
    if alignment == "ALL_BEARISH":
        ctx["alignment"] = "TODOS los TFs bajistas — confluencia máxima para short. Condición ideal."
    elif alignment == "MOSTLY_BEARISH":
        ctx["alignment"] = "Mayoría de TFs bajistas — buena confluencia para short."
    elif alignment == "ALL_BULLISH":
        ctx["alignment"] = "TODOS los TFs alcistas — NO shortear contra esta confluencia. Esperar cambio de estructura."
    elif alignment == "MOSTLY_BULLISH":
        ctx["alignment"] = "Mayoría de TFs alcistas — short contra tendencia, alto riesgo."
    else:
        ctx["alignment"] = "TFs mixtos — sin confluencia direccional clara. Buscar TF dominante."

    # Per-TF highlights
    tf_notes = []
    for tf, tf_data in analysis.items():
        if isinstance(tf_data, dict) and "error" not in tf_data:
            notes = _extract_tf_short_signals(tf, tf_data)
            if notes:
                tf_notes.extend(notes)

    if tf_notes:
        ctx["signals"] = tf_notes

    return ctx


def _extract_tf_short_signals(tf: str, ta: dict) -> list[str]:
    """Extraer señales relevantes para short de un TF."""
    signals = []

    rsi = ta.get("rsi", {})
    rsi_val = rsi.get("rsi")
    if rsi_val is not None:
        if rsi_val > 75:
            signals.append(f"{tf}: RSI {rsi_val:.0f} — MUY sobrecomprado. Señal fuerte para short.")
        elif rsi_val > 70:
            signals.append(f"{tf}: RSI {rsi_val:.0f} — sobrecomprado. Condición favorable para short.")
        elif rsi_val < 30:
            signals.append(f"{tf}: RSI {rsi_val:.0f} — sobreventa. NO shortear aquí, esperar rebote.")

    macd = ta.get("macd", {})
    cross = macd.get("crossover", "")
    if cross == "BEARISH_CROSS":
        signals.append(f"{tf}: MACD cruce bajista — señal de venta, favorable para short.")
    elif cross == "BULLISH_CROSS":
        signals.append(f"{tf}: MACD cruce alcista — cautela con short.")

    bb = ta.get("bollinger", {})
    pos = bb.get("position", "")
    if pos == "ABOVE_UPPER":
        signals.append(f"{tf}: Precio sobre Bollinger superior — sobreextendido, reversión probable. Short favorable.")
    elif pos == "BELOW_LOWER":
        signals.append(f"{tf}: Precio bajo Bollinger inferior — no shortear en sobreventa extrema.")

    st = ta.get("supertrend", {})
    if st.get("direction") == "DOWN":
        signals.append(f"{tf}: Supertrend bajista — tendencia bajista confirmada.")

    ichi = ta.get("ichimoku", {})
    if ichi.get("price_vs_cloud") == "BELOW_CLOUD":
        signals.append(f"{tf}: Precio bajo la nube Ichimoku — zona bajista.")

    adx = ta.get("adx", {})
    if adx.get("trend_direction") == "BEARISH" and (adx.get("adx") or 0) > 25:
        signals.append(f"{tf}: ADX {adx.get('adx', 0):.0f} con dirección bajista — tendencia bajista fuerte.")

    stoch = ta.get("stochastic", {})
    if stoch.get("zone") == "OVERBOUGHT":
        signals.append(f"{tf}: Stochastic sobrecomprado — señal de reversión.")

    return signals


# ═══════════════════════════════════════════════════
# TRADING
# ═══════════════════════════════════════════════════

def contextualize_positions(data: dict) -> dict:
    """Interpretar posiciones abiertas para short trading."""
    positions = data.get("positions", [])
    if not positions:
        return {"status": "Sin posiciones abiertas."}

    ctx = {"positions": []}
    for pos in positions:
        p_ctx = {}
        symbol = pos.get("symbol", "?")
        side = pos.get("side", "?")
        roe = pos.get("roe_pct", 0)
        pnl = pos.get("unrealized_pnl", 0)
        liq_dist = pos.get("liq_distance_pct", 100)
        leverage = pos.get("leverage", 1)
        entry = pos.get("entry_price", 0)
        mark = pos.get("mark_price", 0)

        # Risk level based on liquidation distance
        if liq_dist < 5:
            risk = "CRÍTICO"
            p_ctx["risk_note"] = f"⚠ {symbol} {side}: liquidación a {liq_dist:.1f}% — PELIGRO INMINENTE. Considerar cerrar o agregar margen."
        elif liq_dist < 10:
            risk = "ALTO"
            p_ctx["risk_note"] = f"{symbol} {side}: liquidación a {liq_dist:.1f}% — riesgo alto con leverage {leverage}x."
        elif liq_dist < 20:
            risk = "MODERADO"
            p_ctx["risk_note"] = f"{symbol} {side}: liquidación a {liq_dist:.1f}% — riesgo moderado."
        else:
            risk = "BAJO"
            p_ctx["risk_note"] = f"{symbol} {side}: liquidación a {liq_dist:.1f}% — margen cómodo."

        p_ctx["risk_level"] = risk

        # PnL context
        if roe < -20:
            p_ctx["pnl_note"] = f"ROE {roe:+.1f}% (PnL ${pnl:+.2f}) — pérdida significativa. Evaluar seriamente cortar pérdida."
        elif roe < -10:
            p_ctx["pnl_note"] = f"ROE {roe:+.1f}% (PnL ${pnl:+.2f}) — pérdida moderada. Revisar tesis original."
        elif roe < 0:
            p_ctx["pnl_note"] = f"ROE {roe:+.1f}% (PnL ${pnl:+.2f}) — ligeramente en pérdida."
        elif roe > 20:
            p_ctx["pnl_note"] = f"ROE {roe:+.1f}% (PnL ${pnl:+.2f}) — ganancia amplia. Considerar tomar profit parcial o trailing SL."
        elif roe > 5:
            p_ctx["pnl_note"] = f"ROE {roe:+.1f}% (PnL ${pnl:+.2f}) — en ganancia. Considerar mover SL a breakeven."
        else:
            p_ctx["pnl_note"] = f"ROE {roe:+.1f}% (PnL ${pnl:+.2f})."

        # Direction context
        if side == "SHORT":
            p_ctx["direction"] = f"Posición SHORT — alineada con estrategia preferida."
        elif side == "LONG":
            p_ctx["direction"] = f"Posición LONG — contraria a la estrategia habitual."

        ctx["positions"].append(p_ctx)

    return ctx


def contextualize_balance(data: dict) -> dict:
    """Interpretar balance para trading."""
    balance = data.get("usdt_balance", 0)
    available = data.get("usdt_available", 0)
    pnl = data.get("usdt_unrealized_pnl", 0)
    ctx = {}

    if balance > 0:
        used = balance - available
        used_pct = (used / balance * 100) if balance > 0 else 0
        ctx["margin"] = f"Balance: ${balance:.2f} USDT. Disponible: ${available:.2f} ({100-used_pct:.0f}%). Margen en uso: ${used:.2f} ({used_pct:.0f}%)."

        if used_pct > 80:
            ctx["warning"] = "Más del 80% del margen en uso. Alto riesgo de liquidación en cadena si el mercado se mueve en contra."
        elif used_pct > 50:
            ctx["caution"] = "Más del 50% del margen en uso. Moderar nuevas posiciones."

    if pnl != 0:
        ctx["pnl"] = f"PnL no realizado: ${pnl:+.2f}."
        if pnl < -balance * 0.1 and balance > 0:
            ctx["pnl_warning"] = f"Pérdida no realizada > 10% del balance. Evaluar reducir exposición."

    return ctx


def contextualize_tp_sl(data: dict) -> dict:
    """Interpretar TP/SL con risk:reward. Soporta TP único y múltiples TPs parciales."""
    pos = data.get("position", {})
    tp_order = data.get("tp_order", {})
    sl_order = data.get("sl_order", {})
    ctx = {}

    entry = pos.get("entry_price", 0)
    mark = pos.get("mark_price", 0)
    side = pos.get("side", "")

    if not entry or not mark:
        return ctx

    sl_price = sl_order.get("stop_price") or sl_order.get("stopPrice")

    # ── Detect multiple TP orders (tp_order_1, tp_order_2, ...) ──
    multi_tps = []
    for key in sorted(data.keys()):
        if key.startswith("tp_order_") and isinstance(data[key], dict):
            multi_tps.append((key, data[key]))

    if multi_tps:
        # Multiple TP levels
        tp_lines = []
        for key, tp_ord in multi_tps:
            tp_p = tp_ord.get("stop_price") or tp_ord.get("stopPrice")
            tp_qty = tp_ord.get("quantity") or tp_ord.get("origQty", 0)
            has_error = "error" in tp_ord
            if has_error:
                tp_lines.append(f"  {key}: FALLÓ — {tp_ord.get('error', '?')}")
            elif tp_p:
                tp_p = float(tp_p)
                dist = abs(tp_p - entry) / entry * 100
                tp_lines.append(f"  {key}: {tp_p} ({dist:.1f}% del entry, qty {tp_qty})")
            else:
                tp_lines.append(f"  {key}: colocado OK (qty {tp_qty})")

        ctx["tp_sl"] = "TPs escalonados:\n" + "\n".join(tp_lines)

        if sl_price:
            sl_price = float(sl_price)
            sl_dist = abs(sl_price - entry) / entry * 100
            ctx["tp_sl"] += f"\nSL: {sl_price} ({sl_dist:.1f}% del entry)"

    elif tp_order:
        # Single TP
        tp_price = tp_order.get("stop_price") or tp_order.get("stopPrice")

        if tp_price and sl_price:
            tp_price = float(tp_price)
            sl_price = float(sl_price)

            if side == "SHORT":
                tp_dist = (entry - tp_price) / entry * 100
                sl_dist = (sl_price - entry) / entry * 100
            elif side == "LONG":
                tp_dist = (tp_price - entry) / entry * 100
                sl_dist = (entry - sl_price) / entry * 100
            else:
                tp_dist = abs(tp_price - entry) / entry * 100
                sl_dist = abs(sl_price - entry) / entry * 100

            rr = tp_dist / sl_dist if sl_dist > 0 else 0

            ctx["tp_sl"] = f"TP a {tp_dist:.1f}% del entry, SL a {sl_dist:.1f}% del entry."

            if rr >= 3:
                ctx["risk_reward"] = f"R:R = {rr:.1f}:1 — excelente ratio riesgo/recompensa."
            elif rr >= 2:
                ctx["risk_reward"] = f"R:R = {rr:.1f}:1 — buen ratio."
            elif rr >= 1:
                ctx["risk_reward"] = f"R:R = {rr:.1f}:1 — aceptable, mínimo recomendado."
            else:
                ctx["risk_reward"] = f"R:R = {rr:.1f}:1 — desfavorable. El riesgo supera la recompensa. Considerar ajustar niveles."
        elif tp_price:
            tp_price = float(tp_price)
            dist = abs(tp_price - entry) / entry * 100
            ctx["tp_sl"] = f"TP a {dist:.1f}% del entry. Sin SL configurado — alto riesgo sin protección."
        elif sl_price:
            sl_price = float(sl_price)
            dist = abs(sl_price - entry) / entry * 100
            ctx["tp_sl"] = f"SL a {dist:.1f}% del entry. Sin TP configurado."

    elif sl_price:
        sl_price = float(sl_price)
        dist = abs(sl_price - entry) / entry * 100
        ctx["tp_sl"] = f"SL a {dist:.1f}% del entry. Sin TP configurado."

    # Check for errors
    if tp_order and "error" in tp_order:
        ctx["tp_error"] = "TP no se pudo colocar. Verificar precio."
    if "error" in sl_order:
        ctx["sl_error"] = "SL no se pudo colocar. Verificar precio."

    # Trailing stop info
    trailing_order = data.get("trailing_order", {})
    if trailing_order and "error" not in trailing_order:
        callback = trailing_order.get("callback_rate") or trailing_order.get("callbackRate", "?")
        activation = trailing_order.get("activation_price") or trailing_order.get("activationPrice", "")
        parts = [f"Trailing Stop activo: callback {callback}%"]
        if activation:
            parts.append(f"activación en {activation}")
        ctx["trailing"] = ". ".join(parts) + "."
    elif trailing_order and "error" in trailing_order:
        ctx["trailing_error"] = "Trailing Stop no se pudo colocar. Verificar parámetros."

    return ctx


def contextualize_open_orders(data: dict) -> dict:
    """Interpretar órdenes abiertas."""
    orders = data.get("orders", [])
    if not orders:
        return {"status": "Sin órdenes pendientes."}

    by_symbol = {}
    for o in orders:
        sym = o.get("symbol", "?")
        otype = o.get("type", "?")
        if sym not in by_symbol:
            by_symbol[sym] = {"tp": 0, "sl": 0, "trailing": 0, "limit": 0, "other": 0}
        if "TAKE_PROFIT" in otype:
            by_symbol[sym]["tp"] += 1
        elif "TRAILING_STOP" in otype:
            by_symbol[sym]["trailing"] += 1
        elif "STOP" in otype:
            by_symbol[sym]["sl"] += 1
        elif "LIMIT" in otype:
            by_symbol[sym]["limit"] += 1
        else:
            by_symbol[sym]["other"] += 1

    summaries = []
    for sym, counts in by_symbol.items():
        parts = []
        if counts["tp"]:
            parts.append(f"{counts['tp']} TP")
        if counts["sl"]:
            parts.append(f"{counts['sl']} SL")
        if counts["trailing"]:
            parts.append(f"{counts['trailing']} Trailing")
        if counts["limit"]:
            parts.append(f"{counts['limit']} LIMIT")
        if counts["other"]:
            parts.append(f"{counts['other']} otras")
        summaries.append(f"{sym}: {', '.join(parts)}")

    ctx = {"summary": f"{len(orders)} órdenes pendientes. " + " | ".join(summaries)}

    # Warn if position has no SL
    for sym, counts in by_symbol.items():
        if counts["tp"] > 0 and counts["sl"] == 0:
            ctx.setdefault("warnings", []).append(f"{sym}: tiene TP pero NO SL. Posición sin protección a la baja.")

    return ctx


# ═══════════════════════════════════════════════════
# SCANNER
# ═══════════════════════════════════════════════════

def contextualize_scan(data: dict) -> dict:
    """
    Scorecard de cada candidato contra el perfil élite de trades ganadores.

    Para CADA candidato devuelve:
      - conditions: lista de 8 condiciones con status (✅/⚠️/❌), valor actual,
        objetivo y distancia al ideal
      - elite_score: "N/8" con peso ponderado
      - trade_mode: SINGLE-SHOT | MULTI-SCALP | null
      - action_summary: resumen ejecutivo en español
    """
    altcoins = data.get("top_altcoins", [])
    if not altcoins:
        return {"scan": "Sin oportunidades detectadas en este scan."}

    all_candidates = []

    # Detect multi-asset pump (for condition #7)
    pumps_15 = [a for a in altcoins if a.get("change_pct_24h", 0) > 15]
    is_multi_asset_pump = len(pumps_15) >= 2
    multi_pump_symbols = [a["symbol"] for a in pumps_15[:5]] if is_multi_asset_pump else []

    for alt in altcoins:
        short_analysis = alt.get("short_analysis") or {}
        conviction = short_analysis.get("short_conviction", {})
        signal = conviction.get("signal", "")
        conv_score = conviction.get("score", 0)
        jam_regime = alt.get("jam_regime", "")
        symbol = alt.get("symbol", "?")
        change = alt.get("change_pct_24h", 0)
        momentum = short_analysis.get("momentum_decay", {})
        taker_delta = momentum.get("taker_delta", 0)
        buyers_weak = momentum.get("buyers_weakening", False)
        wicks_data = short_analysis.get("upper_wicks", {})
        dist_from_high = short_analysis.get("price_action", {}).get("distance_from_high_pct", 0)
        langevin = alt.get("langevin") or {}
        kappa = langevin.get("kappa_restoring", 0) or 0
        f_ext = langevin.get("F_ext", 0) or 0
        vol_ratio = (alt.get("jam_state") or {}).get("vol_ratio", 1.0) or 1.0

        conditions = []
        weighted_score = 0.0
        total_weight = 0.0

        # ── C1: Volume Spike (peso 3 — crítico) ──
        w = 3.0
        total_weight += w
        target = ELITE_VOLUME_SPIKE_THRESHOLD
        if vol_ratio >= target:
            conditions.append({
                "check": "volume_spike", "status": "✅",
                "actual": f"{vol_ratio:.1f}x", "target": f"≥{target:.0f}x",
                "distance": "superado",
            })
            weighted_score += w
        elif vol_ratio >= 5.0:
            pct = (vol_ratio / target) * 100
            conditions.append({
                "check": "volume_spike", "status": "⚠️",
                "actual": f"{vol_ratio:.1f}x", "target": f"≥{target:.0f}x",
                "distance": f"al {pct:.0f}% del ideal (falta {target - vol_ratio:.1f}x)",
            })
            weighted_score += w * 0.5
        elif vol_ratio >= 2.0:
            pct = (vol_ratio / target) * 100
            conditions.append({
                "check": "volume_spike", "status": "⚠️",
                "actual": f"{vol_ratio:.1f}x", "target": f"≥{target:.0f}x",
                "distance": f"al {pct:.0f}% del ideal",
            })
            weighted_score += w * 0.25
        else:
            conditions.append({
                "check": "volume_spike", "status": "❌",
                "actual": f"{vol_ratio:.1f}x", "target": f"≥{target:.0f}x",
                "distance": f"volumen bajo — sin anomalía",
            })

        # ── C2: Pump Magnitude (peso 2.5 — alto) ──
        w = 2.5
        total_weight += w
        target_pump = ELITE_PUMP_PCT_THRESHOLD
        abs_change = abs(change)
        if abs_change >= target_pump:
            conditions.append({
                "check": "pump_magnitude", "status": "✅",
                "actual": f"{change:+.1f}%", "target": f"≥{target_pump:.0f}%",
                "distance": "superado",
            })
            weighted_score += w
        elif abs_change >= 15:
            pct = (abs_change / target_pump) * 100
            conditions.append({
                "check": "pump_magnitude", "status": "⚠️",
                "actual": f"{change:+.1f}%", "target": f"≥{target_pump:.0f}%",
                "distance": f"al {pct:.0f}% — falta {target_pump - abs_change:.1f}pp",
            })
            weighted_score += w * 0.5
        elif abs_change >= 5:
            pct = (abs_change / target_pump) * 100
            conditions.append({
                "check": "pump_magnitude", "status": "⚠️",
                "actual": f"{change:+.1f}%", "target": f"≥{target_pump:.0f}%",
                "distance": f"al {pct:.0f}% — movimiento moderado",
            })
            weighted_score += w * 0.2
        else:
            conditions.append({
                "check": "pump_magnitude", "status": "❌",
                "actual": f"{change:+.1f}%", "target": f"≥{target_pump:.0f}%",
                "distance": "movimiento insuficiente",
            })

        # ── C3: Upper Wicks / Rechazo (peso 2.5 — alto) ──
        w = 2.5
        total_weight += w
        avg_wick = wicks_data.get("avg_wick_pct", 0)
        rejection_count = wicks_data.get("rejection_candles", 0)
        strong_wicks = wicks_data.get("strong_wicks", False)
        if strong_wicks:
            conditions.append({
                "check": "upper_wicks", "status": "✅",
                "actual": f"{avg_wick:.0f}% wick ({rejection_count}/3 rechazos)",
                "target": "≥2 velas con mechas >30%",
                "distance": "rechazo claro en máximos",
            })
            weighted_score += w
        elif rejection_count >= 1:
            conditions.append({
                "check": "upper_wicks", "status": "⚠️",
                "actual": f"{avg_wick:.0f}% wick ({rejection_count}/3 rechazos)",
                "target": "≥2 velas con mechas >30%",
                "distance": f"rechazo parcial — falta{' 1 vela más' if rejection_count == 1 else ''}",
            })
            weighted_score += w * 0.4
        elif avg_wick > 15:
            conditions.append({
                "check": "upper_wicks", "status": "⚠️",
                "actual": f"{avg_wick:.0f}% wick promedio",
                "target": "≥2 velas con mechas >30%",
                "distance": "mechas presentes pero débiles",
            })
            weighted_score += w * 0.2
        else:
            conditions.append({
                "check": "upper_wicks", "status": "❌",
                "actual": f"{avg_wick:.0f}% wick promedio",
                "target": "≥2 velas con mechas >30%",
                "distance": "sin señal de rechazo en máximos",
            })

        # ── C4: Taker Fading (peso 2 — medio) ──
        w = 2.0
        total_weight += w
        if buyers_weak or taker_delta < ELITE_TAKER_WEAK_THRESHOLD:
            conditions.append({
                "check": "taker_fading", "status": "✅",
                "actual": f"Δ{taker_delta:+.3f} (ratio reciente {momentum.get('taker_buy_ratio_recent', 0):.2%})",
                "target": f"Δ < {ELITE_TAKER_WEAK_THRESHOLD:+.2f}",
                "distance": "compradores agotándose",
            })
            weighted_score += w
        elif taker_delta < 0:
            pct = min(abs(taker_delta) / abs(ELITE_TAKER_WEAK_THRESHOLD), 1.0) * 100
            conditions.append({
                "check": "taker_fading", "status": "⚠️",
                "actual": f"Δ{taker_delta:+.3f}",
                "target": f"Δ < {ELITE_TAKER_WEAK_THRESHOLD:+.2f}",
                "distance": f"al {pct:.0f}% — compradores debilitándose lento",
            })
            weighted_score += w * 0.4
        else:
            conditions.append({
                "check": "taker_fading", "status": "❌",
                "actual": f"Δ{taker_delta:+.3f}",
                "target": f"Δ < {ELITE_TAKER_WEAK_THRESHOLD:+.2f}",
                "distance": "compradores aún dominan" if taker_delta > 0.02 else "neutral",
            })

        # ── C5: Régimen B confirmado (peso 2.5 — alto) ──
        w = 2.5
        total_weight += w
        if jam_regime == "B":
            conditions.append({
                "check": "regime_b", "status": "✅",
                "actual": f"Régimen B (κ={kappa:.2f}, F_ext={f_ext:.2f})",
                "target": "Régimen B",
                "distance": "reversión confirmada por JAM",
            })
            weighted_score += w
        elif jam_regime == "A":
            conditions.append({
                "check": "regime_b", "status": "❌",
                "actual": f"Régimen A (F_ext={f_ext:.2f} > κ={kappa:.2f})",
                "target": "Régimen B",
                "distance": "pump todavía activo — esperar agotamiento" if change > 0 else "inercia bajista fuerte",
            })
        else:
            conditions.append({
                "check": "regime_b", "status": "⚠️",
                "actual": f"NEUTRAL (κ={kappa:.2f}, F_ext={f_ext:.2f})",
                "target": "Régimen B",
                "distance": "sin régimen definido — señal ambigua",
            })
            weighted_score += w * 0.15

        # ── C6: Short Conviction (peso 2 — medio) ──
        w = 2.0
        total_weight += w
        if signal == "STRONG":
            conditions.append({
                "check": "short_conviction", "status": "✅",
                "actual": f"{signal} ({conv_score:.2f})",
                "target": "MODERATE+ (≥0.45)",
                "distance": "confluencia técnica alta",
            })
            weighted_score += w
        elif signal == "MODERATE":
            conditions.append({
                "check": "short_conviction", "status": "⚠️",
                "actual": f"{signal} ({conv_score:.2f})",
                "target": "MODERATE+ (≥0.45)",
                "distance": "señales técnicas parciales",
            })
            weighted_score += w * 0.6
        else:
            gap = 0.45 - conv_score
            conditions.append({
                "check": "short_conviction", "status": "❌",
                "actual": f"{signal} ({conv_score:.2f})",
                "target": "MODERATE+ (≥0.45)",
                "distance": f"falta {gap:.2f} — sin confluencia técnica",
            })

        # ── C7: Reversión iniciada (peso 2 — medio) ──
        w = 2.0
        total_weight += w
        if dist_from_high > 5:
            conditions.append({
                "check": "reversal_started", "status": "✅",
                "actual": f"-{dist_from_high:.1f}% del máximo",
                "target": ">3% del máximo 24h",
                "distance": "reversión clara — entry más seguro",
            })
            weighted_score += w
        elif dist_from_high > 3:
            conditions.append({
                "check": "reversal_started", "status": "⚠️",
                "actual": f"-{dist_from_high:.1f}% del máximo",
                "target": ">3% del máximo 24h",
                "distance": "inicio de reversión",
            })
            weighted_score += w * 0.6
        elif dist_from_high > 1:
            conditions.append({
                "check": "reversal_started", "status": "⚠️",
                "actual": f"-{dist_from_high:.1f}% del máximo",
                "target": ">3% del máximo 24h",
                "distance": f"falta {3 - dist_from_high:.1f}pp — cerca del pico (riesgo de anticipar)",
            })
            weighted_score += w * 0.2
        else:
            conditions.append({
                "check": "reversal_started", "status": "❌",
                "actual": f"-{dist_from_high:.1f}% del máximo",
                "target": ">3% del máximo 24h",
                "distance": "en el máximo o muy cerca — NO anticipar pico",
            })

        # ── C8: Multi-asset pump (peso 1.5 — medio) ──
        w = 1.5
        total_weight += w
        if is_multi_asset_pump and symbol in multi_pump_symbols:
            conditions.append({
                "check": "multi_asset", "status": "✅",
                "actual": f"parte de pump con {', '.join(s for s in multi_pump_symbols if s != symbol)}",
                "target": "pump multi-asset",
                "distance": "diversificar shorts en 2-3 tokens",
            })
            weighted_score += w
        elif is_multi_asset_pump:
            conditions.append({
                "check": "multi_asset", "status": "⚠️",
                "actual": f"pump multi-asset en: {', '.join(multi_pump_symbols[:3])}",
                "target": "pump multi-asset",
                "distance": "este token no es parte — operar individualmente",
            })
            weighted_score += w * 0.3
        else:
            conditions.append({
                "check": "multi_asset", "status": "ℹ️",
                "actual": "pump individual",
                "target": "pump multi-asset (reduce riesgo)",
                "distance": "no aplica — no hay evento multi-asset",
            })
            weighted_score += w * 0.5  # neutral, not penalized

        # ── Composite elite score ──
        elite_pct = round((weighted_score / total_weight) * 100) if total_weight > 0 else 0
        met_count = sum(1 for c in conditions if c["status"] == "✅")

        # ── Trade mode ──
        if abs_change >= 30 and vol_ratio >= 10:
            trade_mode = "SINGLE-SHOT"
            mode_note = "Pump parabólico — shortear post-pico, un solo trade captura reversión"
        elif jam_regime == "B" and met_count >= 3:
            trade_mode = "MULTI-SCALP"
            mode_note = "Régimen B — scalps cortos sobre rebotes, cerrar antes de 2do funding"
        elif met_count >= 4:
            trade_mode = "MULTI-SCALP"
            mode_note = "Múltiples señales — scalps con gestión activa"
        else:
            trade_mode = None
            mode_note = None

        # ── Action summary ──
        met_labels = [c["check"] for c in conditions if c["status"] == "✅"]
        partial_labels = [c["check"] for c in conditions if c["status"] == "⚠️"]
        missing_labels = [c["check"] for c in conditions if c["status"] == "❌"]

        if elite_pct >= 75:
            action = f"ALTA PRIORIDAD ({elite_pct}% match élite). Condiciones históricas de trades ganadores presentes."
        elif elite_pct >= 50:
            action = f"CANDIDATO ({elite_pct}% match). "
            if missing_labels:
                action += f"Falta: {', '.join(missing_labels)}. "
            action += "Operar con posición reducida y SL ceñido."
        elif elite_pct >= 30:
            action = f"PARCIAL ({elite_pct}% match). "
            if missing_labels:
                action += f"Condiciones ausentes: {', '.join(missing_labels)}. "
            action += "Solo operar si mejora o con size mínimo."
        else:
            action = f"DÉBIL ({elite_pct}% match). No cumple perfil élite — evitar o esperar evolución."

        candidate = {
            "symbol": symbol,
            "change": f"{change:+.1f}%",
            "elite_score": f"{met_count}/8 ({elite_pct}%)",
            "elite_pct": elite_pct,
            "conditions": conditions,
            "action_summary": action,
        }
        if trade_mode:
            candidate["trade_mode"] = trade_mode
            candidate["mode_note"] = mode_note

        all_candidates.append(candidate)

    # ── Sort ALL candidates by elite_pct ──
    all_candidates.sort(key=lambda x: x["elite_pct"], reverse=True)

    ctx = {
        "total_scanned": len(altcoins),
        "candidates_scored": len(all_candidates),
        "scorecards": all_candidates,
    }

    # Top-level summary
    high_priority = [c for c in all_candidates if c["elite_pct"] >= 75]
    medium_priority = [c for c in all_candidates if 50 <= c["elite_pct"] < 75]
    low = [c for c in all_candidates if c["elite_pct"] < 50]

    parts = []
    if high_priority:
        syms = ", ".join(c["symbol"] for c in high_priority[:3])
        parts.append(f"{len(high_priority)} ALTA PRIORIDAD ({syms})")
    if medium_priority:
        syms = ", ".join(c["symbol"] for c in medium_priority[:3])
        parts.append(f"{len(medium_priority)} candidatos ({syms})")
    if low:
        parts.append(f"{len(low)} parciales/débiles")

    if parts:
        ctx["summary"] = (
            f"De {len(altcoins)} escaneadas: {' | '.join(parts)}. "
            "Cada candidato tiene su scorecard con distancia a las 8 condiciones del perfil élite de trades ganadores."
        )
    else:
        ctx["summary"] = f"{len(altcoins)} altcoins analizadas, ninguna con señal short clara según perfil élite."

    # Multi-asset pump detection
    if is_multi_asset_pump:
        ctx["multi_asset_pump"] = (
            f"Pump multi-asset detectado: {', '.join(multi_pump_symbols)}. "
            "Perfil élite: diversificar shorts en 2-3 tokens del mismo evento reduce riesgo individual."
        )

    return ctx


def contextualize_scan_quick(data: dict) -> dict:
    """Interpretar scan rápido para short trading con referencia a perfil élite."""
    alts = data.get("top_altcoins", [])
    if not alts:
        return {"scan": "Sin altcoins detectadas."}

    pumps = [a for a in alts if float(a.get("price_change_pct", 0)) > 5]
    dumps = [a for a in alts if float(a.get("price_change_pct", 0)) < -5]
    mega_pumps = [a for a in alts if float(a.get("price_change_pct", 0)) > 30]

    ctx = {}
    if mega_pumps:
        mp_names = [f"{a.get('symbol', '?')} (+{float(a.get('price_change_pct', 0)):.0f}%)" for a in mega_pumps[:5]]
        ctx["elite_alert"] = (
            f"MEGA PUMP detectado: {', '.join(mp_names)}. "
            "Match perfil élite SINGLE-SHOT: pump >30%, esperar pico visible "
            "(mechas superiores + desaceleración) antes de shortear. "
            "Si son múltiples → diversificar."
        )
    if pumps:
        pump_names = [f"{a.get('symbol', '?')} (+{float(a.get('price_change_pct', 0)):.1f}%)" for a in pumps[:5]]
        ctx["pumps"] = f"Pumps detectados: {', '.join(pump_names)}. Candidatos para short si muestran agotamiento."
    if dumps:
        dump_names = [f"{a.get('symbol', '?')} ({float(a.get('price_change_pct', 0)):+.1f}%)" for a in dumps[:5]]
        ctx["dumps"] = f"Dumps activos: {', '.join(dump_names)}. Short en curso — evaluar si queda recorrido o es tardío."

    ctx["total"] = f"{len(alts)} altcoins en movimiento. {len(pumps)} pumps, {len(dumps)} dumps."

    if len(pumps) >= 2:
        ctx["multi_asset"] = (
            f"{len(pumps)} pumps simultáneos. Perfil élite: diversificar shorts en 2-3 tokens."
        )

    return ctx


# ═══════════════════════════════════════════════════
# GLOBAL MARKET ANALYSIS
# ═══════════════════════════════════════════════════

def contextualize_global_market(data: dict) -> dict:
    """Interpretar análisis global del mercado para short trading."""
    ctx = {}

    # ── BTC status ──
    btc = data.get("btc", {})
    btc_change = btc.get("change_pct_24h", 0)
    btc_price = btc.get("price", 0)
    if btc_change > 3:
        ctx["btc"] = f"BTC ${btc_price:,.0f} (+{btc_change:.1f}%) — en rally fuerte. Altcoins tienden a seguir. Short contra tendencia es riesgoso salvo en pumps extremos de alts específicas."
    elif btc_change > 0.5:
        ctx["btc"] = f"BTC ${btc_price:,.0f} (+{btc_change:.1f}%) — ligeramente alcista. Contexto no ideal para shorts agresivos en BTC, pero alts sobreextendidas pueden corregir."
    elif btc_change < -3:
        ctx["btc"] = f"BTC ${btc_price:,.0f} ({btc_change:+.1f}%) — caída fuerte. Alts caerán más (beta alto). Shorts alineados con la tendencia — buscar alts con mayor debilidad relativa."
    elif btc_change < -0.5:
        ctx["btc"] = f"BTC ${btc_price:,.0f} ({btc_change:+.1f}%) — debilidad moderada. Ambiente favorable para shorts selectivos."
    else:
        ctx["btc"] = f"BTC ${btc_price:,.0f} ({btc_change:+.1f}%) — lateral. Operar alts con catalizadores propios, no depender de dirección de BTC."

    # ── Market breadth ──
    breadth = data.get("market_breadth", {})
    up_pct = breadth.get("up_pct", 50)
    down_pct = breadth.get("down_pct", 50)
    avg_change = breadth.get("avg_change_pct", 0)
    total = breadth.get("total_pairs", 0)

    if down_pct > 65:
        ctx["breadth"] = f"Mercado bajista amplio: {down_pct:.0f}% de {total} pares en rojo (avg {avg_change:+.1f}%). Shorts con el viento a favor — alta probabilidad de continuación bajista."
    elif down_pct > 55:
        ctx["breadth"] = f"Sesgo bajista: {down_pct:.0f}% en rojo (avg {avg_change:+.1f}%). Mercado débil, favorable para shorts selectivos."
    elif up_pct > 65:
        ctx["breadth"] = f"Mercado alcista amplio: {up_pct:.0f}% en verde (avg {avg_change:+.1f}%). Shortear con precaución, solo pumps extremos o estructuralmente débiles."
    elif up_pct > 55:
        ctx["breadth"] = f"Sesgo alcista leve: {up_pct:.0f}% en verde (avg {avg_change:+.1f}%). Selectividad importante para shorts."
    else:
        ctx["breadth"] = f"Mercado mixto: {up_pct:.0f}% verde, {down_pct:.0f}% rojo (avg {avg_change:+.1f}%). Sin dirección clara — operar señales individuales."

    # ── Funding sentiment ──
    funding = data.get("funding", [])
    if funding:
        positive_count = sum(1 for f in funding if f["funding_rate"] > 0.0001)
        negative_count = sum(1 for f in funding if f["funding_rate"] < -0.0001)
        very_positive = [f for f in funding if f["funding_rate"] > 0.0005]
        very_negative = [f for f in funding if f["funding_rate"] < -0.0005]

        parts = []
        if very_positive:
            names = ", ".join(f"{f['symbol']} ({f['funding_pct']:+.3f}%)" for f in very_positive[:5])
            parts.append(f"Funding MUY positivo en: {names} — longs pagando caro, crowded long, holders short cobran. Ideal para mantener short.")
        if very_negative:
            names = ", ".join(f"{f['symbol']} ({f['funding_pct']:+.3f}%)" for f in very_negative[:5])
            parts.append(f"Funding MUY negativo en: {names} — shorts pagando caro, carry negativo para short.")

        if positive_count > negative_count * 2:
            parts.append(f"Sentimiento funding global: dominantemente positivo ({positive_count} pares). Mercado crowded long — favorable para shorts.")
        elif negative_count > positive_count * 2:
            parts.append(f"Sentimiento funding global: dominantemente negativo ({negative_count} pares). Shorts saturados — cuidado con squeeze.")

        ctx["funding"] = " | ".join(parts) if parts else f"{len(funding)} pares analizados, funding sin extremos."

    # ── Long/Short ratio ──
    ls = data.get("long_short", [])
    if ls:
        crowded_long = [x for x in ls if x["ratio"] > 2.0]
        crowded_short = [x for x in ls if x["ratio"] < 0.7]
        if crowded_long:
            names = ", ".join(f"{x['symbol']} ({x['long_pct']:.0f}%L)" for x in crowded_long)
            ctx["long_short"] = f"Posicionamiento crowded long: {names}. Potencial squeeze a la baja si precio rompe soporte."
        elif crowded_short:
            names = ", ".join(f"{x['symbol']} ({x['short_pct']:.0f}%S)" for x in crowded_short)
            ctx["long_short"] = f"Posicionamiento crowded short: {names}. Riesgo de short squeeze — evitar agregar shorts acá."
        else:
            avg_ratio = sum(x["ratio"] for x in ls) / len(ls)
            ctx["long_short"] = f"Posicionamiento L/S equilibrado (ratio promedio {avg_ratio:.2f}). Sin extremos de crowd."

    # ── Taker flow ──
    taker = data.get("taker_flow", [])
    if taker:
        sellers_dominant = sum(1 for t in taker if t["sellers_dominate"])
        if sellers_dominant >= len(taker) * 0.8:
            ctx["taker"] = f"Taker flow: vendedores dominan en {sellers_dominant}/{len(taker)} pares clave. Presión de venta activa — shorts alineados con flujo."
        elif sellers_dominant <= len(taker) * 0.2:
            ctx["taker"] = f"Taker flow: compradores dominan en la mayoría de pares clave. Flujo contra-short — selectividad alta."
        else:
            ctx["taker"] = f"Taker flow mixto: vendedores en {sellers_dominant}/{len(taker)} pares. Sin dominio claro."

    # ── Top pumps (short candidates) ──
    pumps = data.get("top_pumps", [])
    strong_pumps = [p for p in pumps if p["change_pct"] > 5]
    if strong_pumps:
        pump_list = ", ".join(f"{p['symbol']} (+{p['change_pct']:.1f}%)" for p in strong_pumps[:5])
        ctx["short_candidates"] = f"Pumps fuertes (candidatos short): {pump_list}. Evaluar agotamiento con TA + funding antes de entrar."
    elif pumps and pumps[0]["change_pct"] > 2:
        pump_list = ", ".join(f"{p['symbol']} (+{p['change_pct']:.1f}%)" for p in pumps[:3])
        ctx["short_candidates"] = f"Pumps moderados: {pump_list}. Podrían dar oportunidad short si muestran rechazo en resistencia."

    # ── Top dumps (continuation candidates) ──
    dumps = data.get("top_dumps", [])
    strong_dumps = [d for d in dumps if d["change_pct"] < -5]
    if strong_dumps:
        dump_list = ", ".join(f"{d['symbol']} ({d['change_pct']:+.1f}%)" for d in strong_dumps[:5])
        ctx["dump_continuation"] = f"Dumps fuertes: {dump_list}. Shorts ya en curso — evaluar si queda recorrido o es tardío (soportes cercanos, RSI oversold)."

    # ── Overall verdict ──
    bearish_signals = 0
    if down_pct > 55:
        bearish_signals += 1
    if btc_change < -0.5:
        bearish_signals += 1
    if funding and sum(1 for f in funding if f["funding_rate"] > 0.0001) > len(funding) * 0.6:
        bearish_signals += 1  # crowded long = bearish potential
    if taker and sum(1 for t in taker if t["sellers_dominate"]) > len(taker) * 0.6:
        bearish_signals += 1

    if bearish_signals >= 3:
        ctx["verdict"] = "FAVORABLE para shorts — múltiples señales bajistas alineadas. Ambiente propicio para posiciones short con convicción."
    elif bearish_signals >= 2:
        ctx["verdict"] = "MODERADAMENTE favorable para shorts — algunas señales alineadas. Ser selectivo con entries y usar stops."
    elif bearish_signals == 1:
        ctx["verdict"] = "NEUTRAL — señales mixtas. Solo shorts de alta convicción con buena relación R:R."
    else:
        ctx["verdict"] = "DESFAVORABLE para shorts — mercado alcista o sin presión vendedora. Esperar mejor setup o reducir tamaño."

    return ctx


# ═══════════════════════════════════════════════════
# RANGE ASYMMETRY
# ═══════════════════════════════════════════════════

def contextualize_asymmetry(data: dict) -> dict:
    """Interpretar resultado de analyze_range_asymmetry."""
    if "error" in data:
        return {"error": data.get("message", data["error"])}

    ctx = {}
    symbol = data.get("symbol", "?")
    side = data.get("side", "SHORT")
    entry = data.get("entry_price", 0)

    # ── Verdict ──
    verdict = data.get("section_8_verdict", {})
    score = verdict.get("composite_score", 0)
    classification = verdict.get("classification", "?")
    ctx["verdict"] = f"{symbol} @ ${entry}: {classification} (score {score}/100)"

    # ── Key factors ──
    key_factors = verdict.get("key_factors", [])
    if key_factors:
        ctx["key_factors"] = " | ".join(key_factors)

    # ── Cycles ──
    cycles_data = data.get("section_1_cycles", {})
    n_cycles = cycles_data.get("cycles_detected", 0)
    state = cycles_data.get("current_state", "?")
    pump = cycles_data.get("current_pump")
    ctx["cycles"] = f"{n_cycles} ciclos pump→dump detectados. Estado actual: {state}."
    if pump:
        ctx["current_pump"] = (
            f"Pump activo: desde ${pump.get('pump_start_price', '?')} hasta peak ${pump.get('peak_price', '?')} "
            f"(+{pump.get('pct_from_start', '?')}%). Precio actual {pump.get('pct_from_peak', '?')}% debajo del peak."
        )

    # ── Retrace ──
    retrace = data.get("section_2_retrace_stats", {})
    r_median = retrace.get("retrace_ratio_median")
    interps = retrace.get("interpretation", [])
    if r_median is not None:
        ctx["retrace"] = f"Retrace ratio mediano: {r_median:.2f}x" + (f". {' '.join(interps)}" if interps else "")

    # ── Asymmetry R:R ──
    asym = data.get("section_3_asymmetry", {})
    rr_med = asym.get("rr_median")
    rr_cons = asym.get("rr_conservative")
    signal = asym.get("asymmetry_signal", "?")
    pos_range = asym.get("position_in_range_pct")
    if rr_med is not None:
        ctx["rr"] = (
            f"R:R mediano {rr_med}x, conservador {rr_cons}x. "
            f"Señal: {signal}. Posición en rango: {pos_range}%."
        )

    # ── Win rate ──
    wr = data.get("section_4_win_rate", {})
    win_rate = wr.get("win_rate_at_entry")
    ev = wr.get("expected_value_pct")
    if win_rate is not None:
        ctx["win_rate"] = f"Win rate histórico a esta posición: {win_rate}%. EV: {ev:+.2f}%."

    # ── Funding ──
    funding = data.get("section_5_funding_carry", {})
    if "error" not in funding:
        regime = funding.get("funding_regime", "NEUTRAL")
        carry = funding.get("expected_carry_pct", 0)
        hold = funding.get("expected_hold_days", 0)
        if side == "SHORT":
            if regime == "LONGS_PAY":
                ctx["funding"] = f"Funding favorable: shorts cobran carry (+{carry:.4f}% en ~{hold:.0f} días)."
            elif regime == "SHORTS_PAY":
                ctx["funding"] = f"Funding desfavorable: shorts pagan carry ({carry:.4f}% en ~{hold:.0f} días)."
            else:
                ctx["funding"] = f"Funding neutral. Carry estimado: {carry:+.4f}% en ~{hold:.0f} días."

    # ── Timing ──
    vel = data.get("section_6_velocity_timing", {})
    maturity = vel.get("pump_maturity", {})
    vel_interp = vel.get("velocity_interpretation")
    if maturity:
        ctx["timing"] = (
            f"Pump age: {maturity.get('current_pump_age_days', '?')} días "
            f"(percentil {maturity.get('pump_age_percentile', '?')}%). "
            f"{maturity.get('interpretation', '')}"
        )
    if vel_interp:
        ctx["velocity"] = vel_interp

    # ── Volume ──
    vol = data.get("section_7_volume_structure", {})
    vol_interps = vol.get("interpretation", [])
    if vol_interps:
        ctx["volume"] = " | ".join(vol_interps)

    # ── Targets ──
    targets = verdict.get("targets", {})
    if targets:
        parts = []
        for level in ["conservative", "median", "aggressive"]:
            t = targets.get(level)
            if t and t.get("price"):
                parts.append(f"{level}: ${t['price']:.6g} ({t.get('based_on', '')})")
        if parts:
            ctx["targets"] = " | ".join(parts)

    # ── Optimal entry ──
    opt = verdict.get("optimal_entry")
    if opt:
        ctx["optimal_entry"] = (
            f"Entry óptimo: ${opt['price']} ({opt['position_in_range']}) → "
            f"R:R {opt.get('rr_at_optimal', '?')}, WR {opt.get('win_rate_at_optimal', '?')}"
        )

    # ── Stop loss ──
    sl = verdict.get("stop_loss")
    if sl:
        ctx["stop_loss"] = f"Stop loss sugerido: ${sl['price']} ({sl.get('above_peak_pct', 5)}% sobre peak)"

    # ── Action recommendation ──
    if score >= 75:
        ctx["action"] = f"Alta convicción {side}. Asimetría fuerte. Considerar posición con SL en ${sl['price'] if sl else 'peak+5%'}."
    elif score >= 60:
        ctx["action"] = f"{side} aceptable con gestión de riesgo. Usar SL estricto y/o esperar mejor entry."
    elif score >= 45:
        ctx["action"] = f"{side} marginal. Sólo con catalizador adicional (TA, funding extremo, etc)."
    else:
        ctx["action"] = f"No recomendado. Asimetría insuficiente para {side} en este nivel."

    return ctx


def contextualize_performance(data: dict) -> dict:
    """Interpretar performance de trading para el modelo."""
    ctx = {}
    summary = data.get("summary", {})
    stats = data.get("trade_stats", {})
    risk = data.get("risk_metrics", {})
    by_symbol = data.get("by_symbol", [])
    days = data.get("period_days", 30)

    net = summary.get("net_pnl", 0)
    realized = summary.get("realized_pnl", 0)
    funding = summary.get("funding_income", 0)
    comms = summary.get("commissions", 0)
    roi = summary.get("roi_pct", 0)

    # ── Resumen general ──
    ctx["resumen"] = (
        f"Período: {days} días. PnL neto: ${net:+.2f} (ROI {roi:+.1f}%). "
        f"Realizado: ${realized:+.2f}, funding: ${funding:+.2f}, comisiones: ${comms:+.2f}."
    )

    if net > 0:
        ctx["resultado"] = "Período rentable."
    elif net < 0:
        ctx["resultado"] = "Período con pérdidas. Revisar gestión de riesgo y selección de trades."
    else:
        ctx["resultado"] = "Período neutral."

    # ── Estadísticas de trades ──
    total = stats.get("total_trades", 0)
    wr = stats.get("win_rate", 0)
    pf = stats.get("profit_factor", 0)
    exp = stats.get("expectancy", 0)

    if total > 0:
        ctx["trades"] = (
            f"Trades: {total} ({stats.get('wins', 0)}W / {stats.get('losses', 0)}L). "
            f"Win rate: {wr:.1f}%. Profit factor: {pf}. Expectancy: ${exp:+.4f}/trade."
        )

        if wr >= 60 and (pf == "inf" or (isinstance(pf, (int, float)) and pf >= 1.5)):
            ctx["calidad_trades"] = "Excelente sistema: win rate alto + profit factor sólido."
        elif wr >= 50:
            ctx["calidad_trades"] = "Win rate aceptable. Evaluar si el profit factor compensa."
        elif wr < 40:
            ctx["calidad_trades"] = "Win rate bajo. Si profit factor > 2, puede ser aceptable (pocos trades grandes ganadores). Si no, revisar estrategia."
        else:
            ctx["calidad_trades"] = "Win rate moderado. Depende del ratio ganancia/pérdida por trade."

        avg_w = stats.get("avg_win", 0)
        avg_l = stats.get("avg_loss", 0)
        if avg_l != 0:
            ctx["ratio_gain_loss"] = f"Ganancia promedio: ${avg_w:+.4f} vs Pérdida promedio: ${avg_l:+.4f} (ratio {abs(avg_w/avg_l):.2f}x)."

        lw = stats.get("largest_win", 0)
        ll = stats.get("largest_loss", 0)
        ctx["extremos"] = f"Mayor ganancia: ${lw:+.4f}. Mayor pérdida: ${ll:+.4f}."
    else:
        ctx["trades"] = "Sin trades cerrados en el período."

    # ── Riesgo ──
    dd = risk.get("max_drawdown", 0)
    best = risk.get("best_day", 0)
    worst = risk.get("worst_day", 0)
    w_days = risk.get("winning_days", 0)
    l_days = risk.get("losing_days", 0)

    ctx["riesgo"] = (
        f"Max drawdown: ${dd:.2f}. "
        f"Días positivos: {w_days}, negativos: {l_days}. "
        f"Mejor día: ${best:+.2f}. Peor día: ${worst:+.2f}."
    )

    if dd > 0:
        cap = summary.get("estimated_starting_capital", 0)
        if cap > 0:
            dd_pct = dd / cap * 100
            if dd_pct > 20:
                ctx["riesgo_warning"] = f"Drawdown severo ({dd_pct:.1f}% del capital). Reducir tamaño de posiciones o usar SL más ceñidos."
            elif dd_pct > 10:
                ctx["riesgo_caution"] = f"Drawdown considerable ({dd_pct:.1f}% del capital). Monitorear de cerca."

    # ── Funding impact ──
    if funding != 0:
        if funding < 0:
            ctx["funding"] = f"Pagaste ${abs(funding):.2f} en funding. Esto reduce tu PnL neto."
            if realized != 0:
                fnd_pct = abs(funding) / abs(realized) * 100
                if fnd_pct > 30:
                    ctx["funding_warning"] = f"El funding representa {fnd_pct:.0f}% de tu PnL realizado. Considerar cerrar posiciones antes de pagos de funding altos."
        else:
            ctx["funding"] = f"Recibiste ${funding:.2f} en funding. Esto complementa tu PnL."

    # ── Top symbols ──
    if by_symbol:
        top_winners = [s for s in by_symbol if s["pnl"] > 0][:3]
        top_losers = [s for s in by_symbol if s["pnl"] < 0]
        top_losers = top_losers[-3:] if top_losers else []  # worst 3

        if top_winners:
            parts = [f"{s['symbol']} ${s['pnl']:+.2f} ({s['win_rate']:.0f}%WR)" for s in top_winners]
            ctx["mejores_symbols"] = f"Top ganadores: {', '.join(parts)}."
        if top_losers:
            parts = [f"{s['symbol']} ${s['pnl']:+.2f} ({s['win_rate']:.0f}%WR)" for s in top_losers]
            ctx["peores_symbols"] = f"Peores: {', '.join(parts)}."

    return ctx


# ═══════════════════════════════════════════════════
# LIQUIDATION CLUSTERS CONTEXTUALIZATION
# ═══════════════════════════════════════════════════

def contextualize_liquidation_clusters(data: dict) -> dict:
    """
    Contextualizar clusters de liquidación para trading shorts.
    
    Interpreta los niveles de liquidación estimados y destaca:
    - Zonas de cascada potencial (donde múltiples liquidaciones convergen)
    - Niveles cercanos que pueden actuar como magnetos de precio
    - Riesgos para shorts si hay liquidaciones cortas cercanas
    """
    ctx = {}
    
    current_price = data.get("current_price", 0)
    total_oi = data.get("total_oi_value_usdt", 0)
    
    long_data = data.get("long_liquidation_clusters", {})
    short_data = data.get("short_liquidation_clusters", {})
    
    long_cascades = long_data.get("cascade_risk_zones", [])
    short_cascades = short_data.get("cascade_risk_zones", [])
    
    long_nearest = long_data.get("nearest")
    short_nearest = short_data.get("nearest")
    
    # ── Resumen ejecutivo ──
    summary_parts = []
    
    # Liquidaciones long (debajo del precio)
    critical_longs = [c for c in long_cascades if c.get("risk_level") == "CRITICAL"]
    high_longs = [c for c in long_cascades if c.get("risk_level") == "HIGH"]
    
    if critical_longs:
        nearest_critical = min(critical_longs, key=lambda x: abs(x["distance_pct"]))
        summary_parts.append(
            f"🚨 ZONA CRÍTICA de liquidaciones LONG a {nearest_critical['distance_pct']:.1f}% debajo del precio actual. "
            f"Leverages afectados: {nearest_critical['leverages_affected']}. "
            f"Si el precio cae a ${nearest_critical['center']:.6f}, esperar CASCADA de liquidaciones."
        )
    elif high_longs:
        nearest_high = min(high_longs, key=lambda x: abs(x["distance_pct"]))
        summary_parts.append(
            f"⚠️ Zona de alto riesgo de liquidaciones LONG a {nearest_high['distance_pct']:.1f}% debajo. "
            f"Potencial aceleración bajista si el precio penetra ${nearest_high['center']:.6f}."
        )
    
    # Liquidaciones short (arriba del precio)
    critical_shorts = [c for c in short_cascades if c.get("risk_level") == "CRITICAL"]
    high_shorts = [c for c in short_cascades if c.get("risk_level") == "HIGH"]
    
    if critical_shorts:
        nearest_critical = min(critical_shorts, key=lambda x: abs(x["distance_pct"]))
        summary_parts.append(
            f"🚨 PELIGRO PARA SHORTS: Zona crítica de liquidaciones SHORT a {nearest_critical['distance_pct']:.1f}% arriba. "
            f"Si el precio sube a ${nearest_critical['center']:.6f}, los shorts serán liquidados en cascada. "
            "Considerar stop-loss antes de esta zona."
        )
    elif high_shorts:
        nearest_high = min(high_shorts, key=lambda x: abs(x["distance_pct"]))
        summary_parts.append(
            f"⚠️ Liquidaciones SHORT concentradas {nearest_high['distance_pct']:.1f}% arriba. "
            f"Si entras short, SL debería estar ANTES de ${nearest_high['center']:.6f} para evitar squeeze."
        )
    
    ctx["resumen"] = " ".join(summary_parts) if summary_parts else "No hay zonas de cascada críticas cercanas."
    
    # ── Interpretación para SHORT trading ──
    short_interpretation = []
    
    if long_nearest:
        dist = abs(long_nearest["distance_pct"])
        if dist < 5:
            short_interpretation.append(
                f"✅ Para SHORTS: Liquidaciones LONG muy cerca ({dist:.1f}% abajo). "
                "Si el precio cae, estas liquidaciones acelerarán la caída — FAVORABLE para shorts."
            )
        elif dist < 10:
            short_interpretation.append(
                f"📊 Liquidaciones LONG a {dist:.1f}% abajo. "
                "Zona de aceleración bajista moderada si el precio llega ahí."
            )
    
    if short_nearest:
        dist = abs(short_nearest["distance_pct"])
        if dist < 5:
            short_interpretation.append(
                f"⛔ RIESGO: Liquidaciones SHORT a solo {dist:.1f}% arriba. "
                f"Un squeeze podría activarlas. SL recomendado antes de ${short_nearest['liquidation_price']:.6f}."
            )
        elif dist < 10:
            short_interpretation.append(
                f"⚠️ Liquidaciones SHORT a {dist:.1f}% arriba. "
                "Mantener SL conservador para evitar ser parte de una cascada."
            )
    
    ctx["para_shorts"] = " ".join(short_interpretation) if short_interpretation else "Distribución de liquidaciones equilibrada."
    
    # ── Zonas de magneto (donde el precio tiende a ir) ──
    magnets = []
    
    # Las zonas de alta liquidación actúan como magnetos
    all_cascades = long_cascades + short_cascades
    for cascade in sorted(all_cascades, key=lambda x: x.get("cascade_risk_score", 0), reverse=True)[:3]:
        oi_pct = cascade.get("estimated_oi_usdt", 0) / total_oi * 100 if total_oi > 0 else 0
        magnets.append(
            f"${cascade['center']:.6f} ({cascade['side']}, {cascade['distance_pct']:+.1f}%, ~{oi_pct:.1f}% OI)"
        )
    
    if magnets:
        ctx["magnetos_precio"] = f"Zonas magneto (alta concentración OI): {', '.join(magnets)}."
    
    # ── Leverage distribution insight ──
    all_leverages = set()
    for zone in long_data.get("aggregated_zones", []):
        all_leverages.update(zone.get("leverages", []))
    for zone in short_data.get("aggregated_zones", []):
        all_leverages.update(zone.get("leverages", []))
    
    high_lev = [l for l in all_leverages if l >= 50]
    if high_lev:
        ctx["alto_apalancamiento"] = (
            f"Detectados clusters con apalancamiento {max(high_lev)}x. "
            "Posiciones de alto apalancamiento se liquidan primero — las cascadas empiezan con ellas."
        )
    
    # ── Entry zones detectadas ──
    entry_zones = data.get("high_activity_entry_zones", [])
    if entry_zones:
        top_zones = sorted(entry_zones, key=lambda x: x["volume_pct"], reverse=True)[:3]
        zone_strs = [f"${z['center']:.6f} ({z['volume_pct']:.1f}% vol)" for z in top_zones]
        ctx["zonas_entrada_detectadas"] = (
            f"Zonas de entrada probables (alto volumen histórico): {', '.join(zone_strs)}. "
            "Posiciones abiertas en estas zonas son las que generan los clusters de liquidación."
        )
    
    # ── Advertencia ──
    ctx["nota"] = (
        "⚠️ IMPORTANTE: Estos son ESTIMADOS basados en volumen histórico y OI actual. "
        "No son datos reales de posiciones. Usar como guía, no como certeza absoluta. "
        "Binance no expone datos de liquidaciones abiertas públicamente."
    )
    
    return ctx


# ═══════════════════════════════════════════════════
# BASIS TRADING CONTEXTUALIZATION
# ═══════════════════════════════════════════════════

def contextualize_basis_scan(data: dict) -> dict:
    """Contextualiza resultados del scanner de basis spot-futuros."""
    ctx = {}

    opps = data.get("opportunities", [])
    count = data.get("opportunities_found", 0)
    screened = data.get("phase1_screened", 0)
    timing = data.get("timing", {})

    # Resumen general
    if count == 0:
        ctx["resumen"] = (
            f"Scanner analizó {screened} pares y no encontró oportunidades viables. "
            "Esto es normal en mercados con poco desvío spot-futuros (baja volatilidad)."
        )
        ctx["accion"] = "Esperar a que el mercado genere divergencias. Consultar de nuevo en 30-60 min."
        return ctx

    best = opps[0] if opps else {}
    best_score = best.get("score", {}).get("total_score", 0)
    best_sym = best.get("symbol", "?")
    best_basis = best.get("basis_pct", 0)
    best_strategy = best.get("score", {}).get("primary_strategy", "NONE")

    ctx["resumen"] = (
        f"De {screened} pares escaneados, {count} tienen oportunidades de arbitraje "
        f"(scanned en {timing.get('total_sec', '?')}s). "
        f"Mejor: {best_sym} con basis {best_basis:+.4f}% y score {best_score:.0f}/100."
    )

    # Categorizar oportunidades
    excelentes = [o for o in opps if o.get("score", {}).get("total_score", 0) >= 80]
    buenas = [o for o in opps if 60 <= o.get("score", {}).get("total_score", 0) < 80]
    moderadas = [o for o in opps if 40 <= o.get("score", {}).get("total_score", 0) < 60]

    partes = []
    if excelentes:
        syms = ", ".join(o["symbol"] for o in excelentes[:3])
        partes.append(f"EXCELENTES ({len(excelentes)}): {syms}")
    if buenas:
        syms = ", ".join(o["symbol"] for o in buenas[:3])
        partes.append(f"BUENAS ({len(buenas)}): {syms}")
    if moderadas:
        syms = ", ".join(o["symbol"] for o in moderadas[:3])
        partes.append(f"MODERADAS ({len(moderadas)}): {syms}")

    if partes:
        ctx["oportunidades"] = " | ".join(partes)

    # Estrategia dominante
    strategies = {}
    for o in opps:
        s = o.get("score", {}).get("primary_strategy", "NONE")
        strategies[s] = strategies.get(s, 0) + 1

    if strategies:
        dominant = max(strategies, key=strategies.get)
        strategy_labels = {
            "CASH_CARRY": "Cash-and-Carry (capturar convergencia de basis)",
            "FUNDING_ARB": "Funding Rate Arbitrage (capturar pagos de funding)",
            "BASIS_SCALP": "Basis Scalping (aprovechar desviaciones extremas)",
        }
        ctx["estrategia_dominante"] = (
            f"Estrategia más común: {strategy_labels.get(dominant, dominant)} "
            f"({strategies[dominant]}/{len(opps)} oportunidades)."
        )

    # Acción recomendada
    if best_score >= 80:
        ctx["accion"] = (
            f"🟢 {best_sym} es una oportunidad EXCELENTE. "
            f"Prepara el trade con prepare_basis_trade para revisar los detalles."
        )
    elif best_score >= 60:
        ctx["accion"] = (
            f"🟡 {best_sym} es una buena oportunidad. Revisa el basis history "
            f"para confirmar que no es un outlier momentáneo."
        )
    elif best_score >= 40:
        ctx["accion"] = (
            "🟠 Oportunidades moderadas disponibles. Con capital <$1k, "
            "considera esperar mejores condiciones o enfocarte en funding arb."
        )
    else:
        ctx["accion"] = (
            "🔴 No hay oportunidades convincentes ahora. "
            "Esperar a picos de volatilidad o funding extremos."
        )

    # Fees warning
    fee_cfg = data.get("fee_config", {})
    rt_cost = fee_cfg.get("round_trip_cost_pct", 0)
    if rt_cost > 0.2:
        ctx["nota_fees"] = (
            f"⚠️ Round-trip fees: {rt_cost:.2f}%. Con fees estándar, "
            "necesitas basis >0.3% para ser rentable. Considera usar órdenes LIMIT (maker)."
        )

    return ctx


def contextualize_basis_single(data: dict) -> dict:
    """Contextualiza el basis de un par específico."""
    ctx = {}

    basis = data.get("basis", {})
    funding = data.get("funding", {})
    profit = data.get("profit_analysis", {})

    basis_pct = basis.get("basis_pct", 0)
    regime = basis.get("regime", "NEUTRAL")
    direction = basis.get("direction", "")

    # Interpretación del basis
    regime_labels = {
        "EXTREME_PREMIUM": "Premium EXTREMO — futuros muy por encima de spot",
        "CONTANGO": "Contango normal — futuros ligeramente sobre spot",
        "NEUTRAL": "Neutral — spot y futuros casi iguales",
        "BACKWARDATION": "Backwardation — futuros por debajo de spot",
        "EXTREME_DISCOUNT": "Descuento EXTREMO — futuros muy por debajo de spot",
    }
    ctx["basis"] = (
        f"Basis: {basis_pct:+.4f}% ({regime_labels.get(regime, regime)}). "
        f"Annualizado: {basis.get('basis_annualized_pct', 0):+.2f}%."
    )

    # Funding
    if funding and "error" not in funding:
        fr_pct = funding.get("latest_rate_pct", 0)
        annual = funding.get("annualized_pct", 0)
        short_receives = funding.get("short_receives_funding", False)
        ctx["funding"] = (
            f"Funding rate: {fr_pct:+.4f}%/8h (anualizado: {annual:+.2f}%). "
            f"{'Shorts RECIBEN funding' if short_receives else 'Shorts PAGAN funding'}."
        )

    # Profitability
    cc = profit.get("cash_and_carry", {})
    if cc:
        net = cc.get("net_profit_pct", 0)
        if cc.get("viable"):
            ctx["viabilidad"] = f"✅ Cash-and-carry viable: {net:+.4f}% neto después de fees."
        else:
            ctx["viabilidad"] = f"❌ Cash-and-carry NO viable: {net:+.4f}% neto (fees se comen la ganancia)."

    # Recomendación
    if basis_pct > 0.3 and cc.get("viable"):
        ctx["accion"] = "El basis es alto y rentable. Considerar cash-and-carry: comprar spot + short futuros."
    elif funding and funding.get("short_receives_funding") and funding.get("latest_rate_pct", 0) > 0.03:
        ctx["accion"] = "Funding rate alto. Considerar funding arb: comprar spot + short futuros y recoger funding cada 8h."
    elif basis_pct < -0.2:
        ctx["accion"] = "Backwardation: vender spot + long futuros (reverse carry). Raro pero puede ser rentable."
    else:
        ctx["accion"] = "Basis insuficiente para arbitraje rentable. Monitorear."

    return ctx


def contextualize_basis_history(data: dict) -> dict:
    """Contextualiza el historial de basis de un par."""
    ctx = {}

    stats = data.get("stats", {})
    points = data.get("data_points", 0)

    if points == 0:
        ctx["resumen"] = "Sin datos de historial de basis."
        return ctx

    current = stats.get("current_basis_pct", 0)
    avg = stats.get("avg_basis_pct", 0)
    z = stats.get("z_score", 0)
    std = stats.get("std_basis_pct", 0)

    ctx["resumen"] = (
        f"Basis actual: {current:+.4f}% vs promedio: {avg:+.4f}% "
        f"(σ={std:.4f}%, z-score={z:+.2f}). "
        f"Rango: {stats.get('min_basis_pct', 0):+.4f}% a {stats.get('max_basis_pct', 0):+.4f}%."
    )

    if z > 2:
        ctx["señal"] = (
            "📈 Basis SIGNIFICATIVAMENTE por encima de la media (>2σ). "
            "Posible oportunidad de mean-reversion: esperar convergencia."
        )
    elif z > 1:
        ctx["señal"] = (
            "📊 Basis por encima de la media (>1σ). "
            "Potencial oportunidad si la tendencia es de convergencia."
        )
    elif z < -2:
        ctx["señal"] = (
            "📉 Basis SIGNIFICATIVAMENTE por debajo de la media (<-2σ). "
            "Backwardation extrema — posible reverse carry opportunity."
        )
    elif z < -1:
        ctx["señal"] = "📊 Basis por debajo de la media. Monitorear para reverse carry."
    else:
        ctx["señal"] = "Basis dentro del rango normal (±1σ). Sin señal de arbitraje clara."

    return ctx


def contextualize_basis_trade_proposal(data: dict) -> dict:
    """Contextualiza una propuesta de trade de basis."""
    ctx = {}

    if "error" in data:
        ctx["resumen"] = f"❌ {data['error']}"
        return ctx

    symbol = data.get("symbol", "?")
    strategy = data.get("strategy", "?")
    basis = data.get("market_data", {}).get("basis", {})
    sizing = data.get("sizing", {})
    scenarios = data.get("scenarios", {})

    strategy_labels = {
        "CASH_CARRY": "Cash-and-Carry",
        "FUNDING_ARB": "Funding Rate Arbitrage",
        "BASIS_SCALP": "Basis Scalping",
    }

    ctx["resumen"] = (
        f"Propuesta: {strategy_labels.get(strategy, strategy)} en {symbol}. "
        f"Basis actual: {basis.get('basis_pct', 0):+.4f}%, "
        f"capital: ${sizing.get('allocated_usdt', 0):.2f}."
    )

    # Sizing
    spot = sizing.get("spot_leg", {})
    futures = sizing.get("futures_leg", {})
    ctx["tamaño"] = (
        f"Spot: comprar {spot.get('quantity', 0):.6f} ({spot.get('side', '?')}) "
        f"= ${spot.get('notional_usdt', 0):.2f}. "
        f"Futuros: short {futures.get('quantity', 0):.6f} con {futures.get('leverage', 1)}x "
        f"(margen: ${futures.get('margin_usdt', 0):.2f})."
    )

    # Scenarios
    full = scenarios.get("full_convergence", {})
    half = scenarios.get("half_convergence", {})
    adverse = scenarios.get("adverse_2x", {})

    ctx["escenarios"] = (
        f"Si basis converge a 0: ${full.get('net_pnl_usdt', 0):+.4f} ({full.get('net_pnl_pct', 0):+.4f}%). "
        f"Si converge 50%: ${half.get('net_pnl_usdt', 0):+.4f}. "
        f"Si se duplica en contra: ${adverse.get('net_pnl_usdt', 0):+.4f}."
    )

    # Validation
    if not sizing.get("valid", True):
        ctx["advertencia"] = f"⚠️ Tamaño inválido: {', '.join(sizing.get('errors', []))}."

    # Decision aid
    if full.get("profitable") and half.get("profitable"):
        ctx["veredicto"] = "✅ Rentable aún con convergencia parcial del 50%. Buen risk/reward."
    elif full.get("profitable"):
        ctx["veredicto"] = "⚠️ Solo rentable con convergencia TOTAL. Riesgo moderado."
    else:
        ctx["veredicto"] = "❌ No rentable ni con convergencia total. Los fees se comen la ganancia."

    return ctx


def contextualize_basis_positions(data: dict) -> dict:
    """Contextualiza las posiciones hedged activas."""
    ctx = {}

    active = data.get("active_positions", [])
    active_count = data.get("active_count", 0)
    max_pos = data.get("max_positions", 3)

    if active_count == 0:
        ctx["resumen"] = "No hay posiciones basis activas."
        return ctx

    ctx["resumen"] = f"{active_count}/{max_pos} posiciones hedged activas."

    total_pnl = 0
    for pos in active:
        upnl = pos.get("unrealized_pnl", {})
        pnl = upnl.get("net_pnl_usdt", 0)
        total_pnl += pnl

        sym = pos.get("symbol", "?")
        entry_b = pos.get("entry_basis_pct", 0)
        current_b = pos.get("current_basis_pct", 0)
        hours = pos.get("holding_hours", 0)

        status = "✅" if pnl >= 0 else "❌"
        ctx[f"pos_{sym}"] = (
            f"{status} {sym}: basis {entry_b:+.4f}% → {current_b:+.4f}% | "
            f"PnL: ${pnl:+.4f} | {hours:.1f}h holding."
        )

    ctx["pnl_total"] = f"PnL total unrealized: ${total_pnl:+.4f}."

    # Guidance
    for pos in active:
        hours = pos.get("holding_hours", 0)
        sym = pos.get("symbol", "?")
        if hours > 48:
            ctx[f"alerta_{sym}"] = (
                f"⏰ {sym} lleva {hours:.0f}h abierto. "
                "Revisar si el funding acumulado justifica seguir holdeando."
            )

    return ctx


def contextualize_basis_dashboard(data: dict) -> dict:
    """Contextualiza el dashboard completo de basis trading."""
    ctx = {}

    summary = data.get("summary", {})
    active = summary.get("active_positions", 0)
    unrealized = summary.get("total_unrealized_pnl", 0)
    realized = summary.get("total_closed_pnl", 0)
    pending = summary.get("pending_proposals", 0)

    total = unrealized + realized
    ctx["resumen"] = (
        f"Basis Trading: {active} posiciones activas, "
        f"PnL total: ${total:+.4f} (realized: ${realized:+.4f}, unrealized: ${unrealized:+.4f}). "
        f"Propuestas pendientes: {pending}."
    )

    if active == 0 and pending == 0:
        ctx["accion"] = "No hay actividad. Ejecuta scan_basis para buscar oportunidades."
    elif pending > 0:
        ctx["accion"] = f"Hay {pending} propuestas pendientes de aprobación. Revisa con prepare_basis_trade."

    return ctx


# ─────────────────────────────────────────────
# CARRY DETECTOR CONTEXTUALIZATION
# ─────────────────────────────────────────────

def contextualize_carry_scan(data: dict) -> dict:
    """Contextualiza resultados del scanner de carry trades (funding negativo persistente)."""
    ctx = {}

    opps = data.get("opportunities", [])
    count = data.get("opportunities_found", 0)
    phase1_candidates = data.get("phase1_candidates", 0)
    phase1_liquid = data.get("phase1_liquid_pairs", 0)
    timing = data.get("timing", {})

    if count == 0:
        ctx["resumen"] = (
            f"De {phase1_liquid} pares líquidos, {phase1_candidates} tenían funding negativo extremo, "
            f"pero ninguno pasó el filtro de persistencia + consolidación. "
            f"(scanned en {timing.get('total_sec', '?')}s)"
        )
        ctx["accion"] = (
            "No hay oportunidades de carry ahora. El funding no es suficientemente persistente "
            "o los precios están en caída libre. Consultar de nuevo en 4-8 horas."
        )
        return ctx

    best = opps[0]
    score = best.get("score", {})
    funding = best.get("funding", {})
    carry = best.get("carry", {})
    risk = best.get("risk", {})

    ctx["resumen"] = (
        f"De {phase1_liquid} pares, {phase1_candidates} con funding negativo → "
        f"{count} oportunidades de carry confirmadas "
        f"(scanned en {timing.get('total_sec', '?')}s). "
        f"Mejor: {best['symbol']} con score {score.get('total', 0):.0f}/100."
    )

    # Funding frequency summary across opportunities
    accel_opps = [o for o in opps if o.get("funding", {}).get("funding_interval", {}).get("interval_hours", 8) < 8]
    if accel_opps:
        freq_parts = []
        for o in accel_opps:
            fi = o["funding"]["funding_interval"]
            freq_parts.append(f"{o['symbol']} ({fi['category']} {fi['interval_hours']}h, ×{fi['frequency_multiplier']})")
        ctx["frecuencia_cobro"] = (
            f"🚀 {len(accel_opps)}/{count} con cobro acelerado: "
            + ", ".join(freq_parts[:5])
            + ". El carry diario real es mayor que un token estándar 8h con la misma tasa."
        )

    # Categorizar
    excelentes = [o for o in opps if o.get("score", {}).get("verdict") == "CARRY_EXCELENTE"]
    viables = [o for o in opps if o.get("score", {}).get("verdict") == "CARRY_VIABLE"]
    marginales = [o for o in opps if o.get("score", {}).get("verdict") == "CARRY_MARGINAL"]

    partes = []
    if excelentes:
        syms = ", ".join(o["symbol"] for o in excelentes[:3])
        partes.append(f"EXCELENTES ({len(excelentes)}): {syms}")
    if viables:
        syms = ", ".join(o["symbol"] for o in viables[:3])
        partes.append(f"VIABLES ({len(viables)}): {syms}")
    if marginales:
        syms = ", ".join(o["symbol"] for o in marginales[:3])
        partes.append(f"MARGINALES ({len(marginales)}): {syms}")
    if partes:
        ctx["oportunidades"] = " | ".join(partes)

    # Mejor oportunidad detalle
    oi_e = best.get("oi_energy", {})
    oi_detail = ""
    if oi_e.get("available"):
        trap_str = f"ENERGY_TRAP {'ACTIVO' if oi_e.get('energy_trap_active') else 'inactivo'} ({oi_e.get('trap_streak', 0)} períodos)"
        stab_str = "OI value ROC estabilizando ✅" if oi_e.get("oi_value_roc_stabilizing") else "OI value ROC aún no estabiliza"
        oi_detail = f" | OI Energy: {trap_str}, {stab_str}."

    ctx["mejor_oportunidad"] = (
        f"{best['symbol']}: funding avg {funding.get('avg_funding_pct', 0):.4f}% por 8h "
        f"({funding.get('consecutive_negative', 0)} intervalos consecutivos negativos"
        f"{', ACELERANDO' if funding.get('is_accelerating') else ''}). "
        f"Carry estimado: {carry.get('daily_carry_pct', 0):.2f}%/día, "
        f"{carry.get('weekly_carry_pct', 0):.2f}%/semana, "
        f"{carry.get('annualized_carry_pct', 0):.0f}% anualizado. "
        f"Riesgo cascade: {risk.get('risk_level', '?')} ({risk.get('risk_score', 0)}/100)."
        f"{oi_detail}"
    )

    # Acción
    verdict = score.get("verdict", "")
    if verdict == "CARRY_EXCELENTE":
        ctx["accion"] = (
            f"🟢 {best['symbol']} es EXCELENTE para carry. "
            f"Usa analyze_carry para ver el detalle completo antes de entrar. "
            f"SL recomendado: {carry.get('recommended_sl_pct', 3):.1f}% (más ancho que lo normal, el carry compensa)."
        )
    elif verdict == "CARRY_VIABLE":
        ctx["accion"] = (
            f"🟡 {best['symbol']} es viable para carry. "
            f"Confirma con analyze_carry que la consolidación se mantiene. "
            f"El riesgo principal es una liquidación en cascada."
        )
    else:
        ctx["accion"] = (
            "🟠 Oportunidades marginales. El carry no es suficiente para compensar el riesgo. "
            "Esperar a que el funding se extreme más o el precio consolide mejor."
        )

    # Warning de riesgo
    high_risk = [o for o in opps if o.get("risk", {}).get("risk_level") == "ALTO"]
    if high_risk:
        ctx["alerta_riesgo"] = (
            f"⚠️ {len(high_risk)} oportunidades tienen riesgo ALTO de cascade. "
            "Estos tokens podrían colapsar 10-15% en minutos si se disparan liquidaciones. "
            "Usar sizing conservador y SL más ancho."
        )

    # OI Energy summary across opportunities
    trap_confirmed = [
        o for o in opps
        if o.get("oi_energy", {}).get("energy_trap_active")
    ]
    triple = [
        o for o in trap_confirmed
        if o.get("oi_energy", {}).get("oi_value_roc_stabilizing")
        and o.get("funding", {}).get("is_persistent")
    ]
    if triple:
        syms = ", ".join(o["symbol"] for o in triple[:3])
        ctx["oi_energy"] = (
            f"🎯 {len(triple)} con TRIPLE CONFIRMACIÓN (funding + ENERGY_TRAP + OI estabilizando): "
            f"{syms}. Estas son las mejores candidatas para carry."
        )
    elif trap_confirmed:
        syms = ", ".join(o["symbol"] for o in trap_confirmed[:3])
        ctx["oi_energy"] = (
            f"⚡ {len(trap_confirmed)} con ENERGY_TRAP activo: {syms}. "
            "Shorts entrando masivamente — buen indicador de carry sostenible."
        )

    return ctx


def contextualize_carry_quick(data: dict) -> dict:
    """Contextualiza el scan rápido de carry (solo Phase 1)."""
    ctx = {}

    candidates = data.get("candidates", [])
    total = data.get("candidates_found", 0)
    liquid = data.get("total_liquid_pairs", 0)

    if total == 0:
        ctx["resumen"] = (
            f"De {liquid} pares líquidos, ninguno tiene funding negativo extremo ahora. "
            "El mercado no tiene condiciones para carry."
        )
        ctx["accion"] = "Sin oportunidades. Consultar más tarde."
        return ctx

    ctx["resumen"] = (
        f"De {liquid} pares líquidos, {total} tienen funding negativo extremo. "
        f"Top 3: " + ", ".join(
            f"{c['symbol']} ({c['last_funding_pct']:.4f}%)"
            for c in candidates[:3]
        ) + "."
    )

    # Highlight accelerated funding tokens
    accel = [c for c in candidates if c.get("funding_interval", {}).get("interval_hours", 8) < 8]
    if accel:
        turbo = [c for c in accel if c.get("funding_interval", {}).get("category") == "TURBO"]
        fast = [c for c in accel if c.get("funding_interval", {}).get("category") == "FAST"]
        accelerated = [c for c in accel if c.get("funding_interval", {}).get("category") == "ACCELERATED"]
        parts = []
        if turbo:
            parts.append(f"TURBO 1h ({len(turbo)}): " + ", ".join(c["symbol"] for c in turbo[:5]))
        if fast:
            parts.append(f"FAST 2h ({len(fast)}): " + ", ".join(c["symbol"] for c in fast[:5]))
        if accelerated:
            parts.append(f"ACELERADO 4h ({len(accelerated)}): " + ", ".join(c["symbol"] for c in accelerated[:5]))
        ctx["frecuencia_cobro"] = (
            f"🚀 {len(accel)}/{total} con cobro acelerado: " + " | ".join(parts) + ". "
            "Cobran funding más veces al día (×2 a ×8 vs estándar 8h), multiplicando el ingreso de carry."
        )

    ctx["nota"] = (
        "⚠️ Esto es solo Phase 1 (snapshot actual). "
        "Para confirmar persistencia y consolidación, ejecutar scan_carry (full)."
    )

    # Si hay muchos candidatos, es señal de mercado con shorts agresivos
    if total >= 10:
        ctx["mercado"] = (
            f"🔥 {total} tokens con funding negativo extremo — el mercado está lleno de shorts. "
            "Condiciones favorables para carry trades si los precios consolidan."
        )

    return ctx


def contextualize_carry_single(data: dict) -> dict:
    """Contextualiza el análisis de carry de un símbolo específico."""
    ctx = {}

    if "error" in data:
        ctx["error"] = data["error"]
        return ctx

    symbol = data.get("symbol", "?")
    score = data.get("score", {})
    funding = data.get("funding", {})
    price = data.get("price_analysis", {})
    risk = data.get("risk", {})
    carry = data.get("carry", {})
    verdict = score.get("verdict", "NO_CARRY")

    # Resumen ejecutivo
    fi = funding.get("funding_interval", {})
    interval_h = fi.get("interval_hours", 8)
    cpd = fi.get("collections_per_day", 3)
    freq_label = fi.get("category", "STANDARD")
    multiplier = fi.get("frequency_multiplier", 1)

    freq_note = ""
    if interval_h < 8:
        freq_note = f" [{freq_label} {interval_h}h, ×{multiplier} cobros]"

    ctx["resumen"] = (
        f"{symbol}: Score {score.get('total', 0):.0f}/100 → {verdict}. "
        f"Funding: {funding.get('avg_funding_pct', 0):.4f}%/{interval_h}h "
        f"({funding.get('consecutive_negative', 0)} intervalos consecutivos). "
        f"Carry: {carry.get('daily_carry_pct', 0):.2f}%/día ({cpd} cobros/día). "
        f"Riesgo: {risk.get('risk_level', '?')}.{freq_note}"
    )

    # Diagnóstico de funding
    if funding.get("is_persistent"):
        accel = " y ACELERANDO (cada vez más negativo)" if funding.get("is_accelerating") else ""
        freq_diag = ""
        if interval_h < 8:
            freq_diag = (
                f" 🚀 Cobro {freq_label}: cada {interval_h}h (×{multiplier} vs estándar 8h). "
                f"Esto multiplica {multiplier}× el ingreso de carry con la misma tasa por intervalo."
            )
        ctx["funding"] = (
            f"✅ Funding persistente: {funding.get('consecutive_negative', 0)} intervalos "
            f"consecutivos negativos, {funding.get('extreme_intervals', 0)} por debajo de -0.5%"
            f"{accel}. "
            f"Acumulado: {funding.get('cumulative_funding_pct', 0):.4f}% (esto es lo que habrías cobrado)."
            f"{freq_diag}"
        )
    else:
        ctx["funding"] = (
            f"❌ Funding NO es persistente: solo {funding.get('consecutive_negative', 0)} "
            f"intervalos consecutivos negativos (mínimo 3 requerido). "
            "Puede ser un spike puntual, no una tendencia."
        )

    # Diagnóstico de precio
    if price.get("is_consolidating"):
        ctx["precio"] = (
            f"✅ Precio consolidando: ATR ratio {price.get('atr_ratio', 0):.4f} "
            f"(< {0.06} = bajo), cambio total {price.get('total_change_pct', 0):+.2f}%, "
            f"max drawdown {price.get('max_drawdown_pct', 0):.2f}%. "
            "El precio no está en caída libre → carry es viable."
        )
    else:
        ctx["precio"] = (
            f"⚠️ Precio NO consolida: ATR ratio {price.get('atr_ratio', 0):.4f}, "
            f"cambio total {price.get('total_change_pct', 0):+.2f}%, "
            f"max drawdown {price.get('max_drawdown_pct', 0):.2f}%. "
            "El carry podría no compensar la caída del precio."
        )

    # Carry profitability
    net_daily = carry.get("net_daily_pct", 0)
    if net_daily > 0:
        ctx["carry"] = (
            f"✅ Carry neto POSITIVO: {net_daily:.4f}%/día "
            f"(funding {carry.get('daily_carry_pct', 0):.4f}%/día [{cpd} cobros] "
            f"- drift precio {abs(price.get('price_slope_pct_per_hour', 0) * 24):.4f}%/día). "
            f"Semanal: {carry.get('net_weekly_pct', 0):.4f}%. "
            f"El precio puede caer hasta {carry.get('breakeven_daily_drop_pct', 0):.4f}%/día sin perder."
        )
    else:
        ctx["carry"] = (
            f"⚠️ Carry neto NEGATIVO: {net_daily:.4f}%/día. "
            "El precio está cayendo más rápido de lo que cobra el funding. "
            "No recomendado entrar ahora."
        )

    # Riesgo
    factors = risk.get("factors", {})
    if factors:
        factor_list = ", ".join(f"{k} ({v}pts)" for k, v in factors.items())
        ctx["riesgo"] = (
            f"Riesgo cascade: {risk.get('risk_level', '?')} "
            f"({risk.get('risk_score', 0)}/100). "
            f"Factores: {factor_list}."
        )
    else:
        ctx["riesgo"] = f"Riesgo cascade: {risk.get('risk_level', 'BAJO')} — sin factores de alerta."

    # OI Energy diagnosis
    oi_e = data.get("oi_energy", {})
    if oi_e.get("available"):
        trap_active = oi_e.get("energy_trap_active", False)
        trap_streak = oi_e.get("trap_streak", 0)
        stabilizing = oi_e.get("oi_value_roc_stabilizing", False)
        cur_state = oi_e.get("current_state", "?")
        oi_score = oi_e.get("oi_energy_score", 0)

        parts = []
        if trap_active:
            parts.append(
                f"✅ ENERGY_TRAP ACTIVO ({trap_streak} períodos consecutivos): "
                "shorts están entrando masivamente (OI tokens ↑) mientras el capital se drena (OI value ↓). "
                "Estos shorts van a SEGUIR pagando funding → excelente para carry."
            )
        elif trap_streak > 0:
            parts.append(
                f"🟡 ENERGY_TRAP parcial ({trap_streak} períodos, mínimo 3 para confirmación). "
                "Shorts entrando pero aún no es persistente."
            )
        else:
            parts.append(f"Estado energético actual: {cur_state}. Sin ENERGY_TRAP activo.")

        if stabilizing:
            parts.append(
                f"✅ OI value ROC ESTABILIZANDO: pasó de "
                f"{oi_e.get('avg_older_oi_value_roc', 0):.2f}% a {oi_e.get('avg_recent_oi_value_roc', 0):.2f}%. "
                "El precio está formando un piso — el riesgo de drawdown baja."
            )
        else:
            parts.append(
                "OI value ROC aún no estabiliza — el capital sigue drenándose. "
                "Precaución con el timing de entrada."
            )

        ctx["oi_energy"] = " | ".join(parts) + f" (OI Energy Score: {oi_score}/100)"

        # Triple confirmation check
        funding_ok = funding.get("is_persistent", False)
        if funding_ok and trap_active and stabilizing:
            ctx["triple_confirmacion"] = (
                "🎯 TRIPLE CONFIRMACIÓN CARRY: "
                "(1) Funding persistente ✅ "
                "(2) ENERGY_TRAP activo ✅ "
                "(3) OI value ROC estabilizando ✅ — "
                "Las 3 condiciones ideales de entrada se cumplen."
            )
        elif funding_ok and trap_active:
            ctx["triple_confirmacion"] = (
                "🟡 2/3 CONFIRMACIONES: Funding persistente ✅ + ENERGY_TRAP ✅, "
                "pero OI value ROC aún no estabiliza. Entrada viable con precaución."
            )
    else:
        ctx["oi_energy"] = "Sin datos de OI histórico disponibles para este par."

    # Acción final
    if verdict == "CARRY_EXCELENTE":
        ctx["accion"] = (
            f"🟢 CARRY EXCELENTE. Entrar LONG en {symbol} para cobrar funding. "
            f"SL recomendado: {carry.get('recommended_sl_pct', 3):.1f}% "
            f"(más ancho que un trade normal porque el carry compensa drawdowns temporales). "
            f"Monitorear con monitor_carry_exit para detectar normalización del funding."
        )
    elif verdict == "CARRY_VIABLE":
        ctx["accion"] = (
            f"🟡 CARRY VIABLE. {symbol} tiene potencial pero con precauciones. "
            f"SL: {carry.get('recommended_sl_pct', 3):.1f}%. "
            "Verificar que la consolidación se mantenga en las próximas horas."
        )
    elif verdict == "CARRY_MARGINAL":
        ctx["accion"] = (
            f"🟠 CARRY MARGINAL. El yield no justifica el riesgo. "
            "Esperar a que las condiciones mejoren (funding más extremo o precio más estable)."
        )
    else:
        ctx["accion"] = (
            f"🔴 NO CARRY. {symbol} no cumple las condiciones para carry trade. "
            "Funding no persistente, precio inestable, o riesgo demasiado alto."
        )

    return ctx


def contextualize_carry_exit(data: dict) -> dict:
    """Contextualiza las señales de salida del carry trade."""
    ctx = {}

    if "error" in data:
        ctx["error"] = data["error"]
        return ctx

    symbol = data.get("symbol", "?")
    action = data.get("action", "?")
    current = data.get("current_funding_pct", 0)
    entry = data.get("entry_funding_pct", 0)
    signals = data.get("signals", [])

    # Resumen
    signal_names = [s["signal"] for s in signals]
    ctx["resumen"] = (
        f"{symbol}: Funding actual {current:.4f}% (entrada: {entry:.4f}%). "
        f"Señales: {', '.join(signal_names)}. "
        f"Acción: {action}."
    )

    # Detalle de cada señal
    for s in signals:
        severity = s["severity"]
        if severity == "CERRAR":
            ctx[s["signal"]] = f"🔴 {s['detail']}"
        elif severity == "PREPARAR_SALIDA":
            ctx[s["signal"]] = f"🟡 {s['detail']}"
        else:
            ctx[s["signal"]] = f"🟢 {s['detail']}"

    # Acción
    if action == "CERRAR_POSICION":
        ctx["accion"] = (
            f"🔴 CERRAR la posición de carry en {symbol}. "
            "El funding se normalizó o redujo drásticamente — el carry ya no es rentable. "
            "Tomar profits del funding acumulado."
        )
    elif action == "PREPARAR_SALIDA":
        ctx["accion"] = (
            f"🟡 PREPARAR SALIDA de {symbol}. El funding está revirtiendo. "
            "Subir el SL para proteger ganancias. Si el funding sigue subiendo, cerrar."
        )
    else:
        ctx["accion"] = (
            f"🟢 MANTENER carry en {symbol}. Funding sigue negativo y estable. "
            "Seguir cobrando. Próximo check en el siguiente intervalo de funding (8h)."
        )

    return ctx
