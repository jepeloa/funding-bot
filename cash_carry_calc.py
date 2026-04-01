#!/usr/bin/env python3
"""
Cash-and-Carry Profitability Calculator
========================================
Calcula el P&L real de una estrategia de basis trading:
  - Comprar SPOT + Short FUTURES para cobrar funding rate

Modelo de costos completo:
  - Spot fees (maker/taker)
  - Futures fees (maker/taker)
  - Slippage estimado
  - Capital inmovilizado (spot 100% + futures margin)
  - Basis risk (premium/discount al entrar y salir)
  - Funding rate promedio real (histórico 7d)
"""

import asyncio
import asyncpg
import datetime
import os
import sys

# ─── CONFIG ─────────────────────────────────────────────────────
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = int(os.getenv('DB_PORT', '5432'))
DB_NAME = os.getenv('DB_NAME', 'binance_futures')
DB_USER = os.getenv('DB_USER', 'recorder')
DB_PASS = os.getenv('DB_PASSWORD', 'recorder')

# Binance fee tiers (USDT-M Futures + Spot)
# https://www.binance.com/en/fee/trading
# VIP 0 defaults:
SPOT_MAKER_FEE = 0.001      # 0.10%  (con BNB: 0.075%)
SPOT_TAKER_FEE = 0.001      # 0.10%  (con BNB: 0.075%)
FUT_MAKER_FEE  = 0.0002     # 0.02%
FUT_TAKER_FEE  = 0.0005     # 0.05%

# Con BNB discount (25% off)
SPOT_MAKER_FEE_BNB = 0.00075
SPOT_TAKER_FEE_BNB = 0.00075
FUT_MAKER_FEE_BNB  = 0.000150   # ~0.015%
FUT_TAKER_FEE_BNB  = 0.000375   # ~0.0375%

# Slippage estimado por lado (depende de liquidez)
SLIPPAGE_PER_SIDE  = 0.0003  # 0.03% para coins líquidas

# Futures margin requirement (para calcular capital total)
FUT_MARGIN_RATIO = 0.20      # 20% margin = 5x max leverage (conservador)

# Holding periods a evaluar (días)
HOLD_DAYS = [7, 14, 30, 60, 90]

# Capital de ejemplo
CAPITAL_USD = 10_000


# ─── FEE SCENARIOS ─────────────────────────────────────────────
SCENARIOS = {
    'Taker (VIP0)': {
        'spot_entry': SPOT_TAKER_FEE,
        'spot_exit':  SPOT_TAKER_FEE,
        'fut_entry':  FUT_TAKER_FEE,
        'fut_exit':   FUT_TAKER_FEE,
        'slippage':   SLIPPAGE_PER_SIDE,
    },
    'Maker (VIP0)': {
        'spot_entry': SPOT_MAKER_FEE,
        'spot_exit':  SPOT_MAKER_FEE,
        'fut_entry':  FUT_MAKER_FEE,
        'fut_exit':   FUT_MAKER_FEE,
        'slippage':   SLIPPAGE_PER_SIDE * 0.5,  # limit orders = menos slippage
    },
    'Maker + BNB': {
        'spot_entry': SPOT_MAKER_FEE_BNB,
        'spot_exit':  SPOT_MAKER_FEE_BNB,
        'fut_entry':  FUT_MAKER_FEE_BNB,
        'fut_exit':   FUT_MAKER_FEE_BNB,
        'slippage':   SLIPPAGE_PER_SIDE * 0.5,
    },
}


def calc_roundtrip_cost(scenario: dict) -> float:
    """Total cost to open AND close the full position (spot + futures)"""
    # Entry: buy spot + sell futures
    entry_cost = (
        scenario['spot_entry'] +   # buy spot
        scenario['fut_entry'] +    # sell futures
        scenario['slippage'] * 2   # slippage on both legs
    )
    # Exit: sell spot + buy futures  
    exit_cost = (
        scenario['spot_exit'] +    # sell spot
        scenario['fut_exit'] +     # buy futures
        scenario['slippage'] * 2   # slippage on both legs
    )
    return entry_cost + exit_cost


def calc_pnl(fr_avg_8h: float, hold_days: int, scenario: dict, 
             basis_entry: float = 0.0, basis_exit: float = 0.0) -> dict:
    """
    Calculate complete P&L for a cash-and-carry position.
    
    Args:
        fr_avg_8h: Average funding rate per 8h period
        hold_days: Number of days to hold
        scenario: Fee scenario dict
        basis_entry: Futures premium when entering (positive = futures > spot)
        basis_exit: Expected futures premium when exiting (usually ~0 or same)
    
    Returns:
        Dict with complete P&L breakdown
    """
    # --- Income ---
    # Funding collected: 3 payments per day × hold_days
    funding_payments = hold_days * 3
    funding_income = fr_avg_8h * funding_payments
    
    # Basis capture: if futures premium narrows, we profit
    # When we short futures at premium and close at parity
    basis_pnl = basis_entry - basis_exit  # positive if premium shrinks
    
    total_income = funding_income + basis_pnl
    
    # --- Costs ---
    roundtrip_fees = calc_roundtrip_cost(scenario)
    
    total_cost = roundtrip_fees
    
    # --- Net P&L (as % of position size) ---
    net_pnl_pct = total_income - total_cost
    
    # --- Capital efficiency ---
    # Total capital needed: 100% for spot + margin for futures
    # Position size = what we trade on each leg
    capital_ratio = 1.0 + FUT_MARGIN_RATIO  # e.g., 1.20 = need $1.20 per $1 of position
    
    # Return on TOTAL capital deployed
    return_on_capital = net_pnl_pct / capital_ratio
    
    # Annualized return on capital
    annual_return = return_on_capital * (365 / hold_days) if hold_days > 0 else 0
    
    return {
        'hold_days': hold_days,
        'funding_income': funding_income,
        'funding_payments': funding_payments,
        'basis_pnl': basis_pnl,
        'total_income': total_income,
        'roundtrip_fees': roundtrip_fees,
        'net_pnl_pct': net_pnl_pct,
        'capital_ratio': capital_ratio,
        'return_on_capital': return_on_capital,
        'annual_return': annual_return,
    }


async def main():
    conn = await asyncpg.connect(
        host=DB_HOST, port=DB_PORT, database=DB_NAME,
        user=DB_USER, password=DB_PASS
    )
    
    latest = await conn.fetchval(
        "SELECT timestamp FROM strategy_snapshots ORDER BY timestamp DESC LIMIT 1"
    )
    print(f"Último snapshot: {latest}")
    print(f"Capital de ejemplo: ${CAPITAL_USD:,}")
    print(f"Margin ratio futuros: {FUT_MARGIN_RATIO*100:.0f}% (capital total = {(1+FUT_MARGIN_RATIO)*100:.0f}% del position size)")
    
    # ─── Get current top FR symbols ─────────────────────────────
    rows = await conn.fetch("""
        SELECT symbol, funding_rate, oi_value, mark_price 
        FROM strategy_snapshots 
        WHERE timestamp = $1 AND funding_rate > 0
        ORDER BY funding_rate DESC
        LIMIT 50
    """, latest)
    
    current_data = {
        r['symbol']: {
            'fr_now': r['funding_rate'], 
            'oi': r['oi_value'], 
            'price': r['mark_price']
        } for r in rows
    }
    symbols = [r['symbol'] for r in rows]
    
    print(f"\n{len(symbols)} symbols con FR positivo (shorts cobran).")
    print("Obteniendo FR promedio 7d...")
    
    # ─── Historical FR averages ─────────────────────────────────
    cutoff_7d = latest - datetime.timedelta(days=7)
    
    results = []
    for sym in symbols:
        avgs = await conn.fetchrow("""
            SELECT 
                AVG(funding_rate) as fr_7d,
                MIN(funding_rate) as fr_min,
                MAX(funding_rate) as fr_max,
                STDDEV(funding_rate) as fr_std,
                AVG(CASE WHEN funding_rate < 0 THEN 1.0 ELSE 0.0 END) as pct_negative,
                COUNT(*) as samples
            FROM strategy_snapshots
            WHERE symbol = $1 AND timestamp >= $2
        """, sym, cutoff_7d)
        
        d = current_data[sym]
        results.append({
            'sym': sym,
            'fr_now': d['fr_now'],
            'fr_7d': float(avgs['fr_7d'] or 0),
            'fr_min': float(avgs['fr_min'] or 0),
            'fr_max': float(avgs['fr_max'] or 0),
            'fr_std': float(avgs['fr_std'] or 0),
            'pct_neg': float(avgs['pct_negative'] or 0),
            'oi': d['oi'],
            'price': d['price'],
            'samples': avgs['samples']
        })
    
    await conn.close()
    
    # Filtrar: OI > $1M y FR_7d > 0.005%
    filtered = [r for r in results if r['oi'] >= 1_000_000 and r['fr_7d'] >= 0.00005]
    filtered.sort(key=lambda x: x['fr_7d'], reverse=True)
    
    if not filtered:
        filtered = sorted(results, key=lambda x: x['fr_7d'], reverse=True)[:15]
        print("(Sin filtro de OI, mostrando top 15)")
    
    # ═══════════════════════════════════════════════════════════
    # PARTE 1: FR Histórico
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'='*110}")
    print("  PARTE 1: FUNDING RATE HISTORICO (promedio 7 días)")
    print(f"{'='*110}")
    print(f"{'Symbol':>16} | {'FR ahora':>9} | {'FR avg7d':>9} | {'FR min':>9} | {'FR max':>9} | {'%neg':>5} | {'OI':>8} | {'Estable':>7}")
    print(f"{'-'*110}")
    
    for r in filtered[:20]:
        oi = f"{r['oi']/1e6:.1f}M" if r['oi'] >= 1e6 else f"{r['oi']/1e3:.0f}K"
        # Estabilidad: si nunca fue negativo y std es bajo = bueno
        stability = "★★★" if r['pct_neg'] == 0 and r['fr_std'] < r['fr_7d'] * 0.5 else \
                    "★★☆" if r['pct_neg'] < 0.1 else \
                    "★☆☆" if r['pct_neg'] < 0.3 else "☆☆☆"
        print(f"{r['sym']:>16} | {r['fr_now']*100:>8.4f}% | {r['fr_7d']*100:>8.4f}% | {r['fr_min']*100:>8.4f}% | {r['fr_max']*100:>8.4f}% | {r['pct_neg']*100:>4.0f}% | {oi:>8} | {stability:>7}")
    
    # ═══════════════════════════════════════════════════════════
    # PARTE 2: P&L detallado por escenario de fees
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'='*110}")
    print("  PARTE 2: COSTO DE FEES (roundtrip = abrir + cerrar)")
    print(f"{'='*110}")
    
    for name, scenario in SCENARIOS.items():
        rt = calc_roundtrip_cost(scenario)
        print(f"  {name:20s}: {rt*100:.3f}% roundtrip")
        print(f"    Entrada: spot {scenario['spot_entry']*100:.3f}% + fut {scenario['fut_entry']*100:.3f}% + slip {scenario['slippage']*100:.3f}%")
        print(f"    Salida:  spot {scenario['spot_exit']*100:.3f}% + fut {scenario['fut_exit']*100:.3f}% + slip {scenario['slippage']*100:.3f}%")

    # ═══════════════════════════════════════════════════════════
    # PARTE 3: P&L por symbol × hold period × fee scenario
    # ═══════════════════════════════════════════════════════════
    top_picks = filtered[:10]
    
    for r in top_picks:
        print(f"\n{'='*110}")
        print(f"  {r['sym']}  |  FR_7d={r['fr_7d']*100:.4f}%  |  OI=${r['oi']/1e6:.1f}M  |  Precio=${r['price']:.4f}")
        print(f"{'='*110}")
        
        # Position sizing
        position_size = CAPITAL_USD / (1 + FUT_MARGIN_RATIO)
        spot_capital = position_size
        fut_margin = position_size * FUT_MARGIN_RATIO
        
        print(f"  Capital total: ${CAPITAL_USD:,.0f}  →  Spot: ${spot_capital:,.0f} + Futures margin: ${fut_margin:,.0f}")
        print(f"  Position size (cada lado): ${position_size:,.0f}")
        print()
        
        for scenario_name, scenario in SCENARIOS.items():
            rt_cost = calc_roundtrip_cost(scenario)
            print(f"  ─── {scenario_name} (roundtrip: {rt_cost*100:.3f}%) ───")
            
            header = f"    {'Días':>6} | {'FR cobrado':>11} | {'Fees':>11} | {'Neto posición':>14} | {'Neto capital':>13} | {'Anualizado':>11} | {'P&L USD':>10}"
            print(header)
            print(f"    {'-'*100}")
            
            for days in HOLD_DAYS:
                pnl = calc_pnl(
                    fr_avg_8h=r['fr_7d'],
                    hold_days=days,
                    scenario=scenario,
                    basis_entry=0,  # asumimos 0 basis (conservador)
                    basis_exit=0,
                )
                
                pnl_usd = pnl['net_pnl_pct'] * position_size
                funding_usd = pnl['funding_income'] * position_size
                fees_usd = pnl['roundtrip_fees'] * position_size
                
                color = "✅" if pnl['net_pnl_pct'] > 0 else "❌"
                
                print(f"    {days:>5}d | ${funding_usd:>9.2f}  | ${fees_usd:>9.2f}  | "
                      f"{pnl['net_pnl_pct']*100:>+12.3f}% | "
                      f"{pnl['return_on_capital']*100:>+11.3f}% | "
                      f"{pnl['annual_return']*100:>+9.1f}%  | "
                      f"${pnl_usd:>+8.2f} {color}")
            print()
    
    # ═══════════════════════════════════════════════════════════
    # PARTE 4: RESUMEN - ¿Cuánto sacas con $10K?
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'='*110}")
    print(f"  RESUMEN: GANANCIA NETA MENSUAL (30d) con ${CAPITAL_USD:,}")
    print(f"{'='*110}")
    
    scenario_maker_bnb = SCENARIOS['Maker + BNB']
    
    print(f"\n  {'Symbol':>16} | {'FR avg 7d':>10} | {'Neto/mes':>10} | {'$/mes':>9} | {'$/año':>10} | {'ROI anual':>10} | {'Breakeven':>10}")
    print(f"  {'-'*100}")
    
    position_size = CAPITAL_USD / (1 + FUT_MARGIN_RATIO)
    
    for r in top_picks:
        pnl = calc_pnl(r['fr_7d'], 30, scenario_maker_bnb)
        pnl_usd_month = pnl['net_pnl_pct'] * position_size
        pnl_usd_year = pnl_usd_month * 12
        roi_annual = pnl['annual_return']
        
        # Breakeven days (cuantos días para cubrir fees)
        daily_fr = r['fr_7d'] * 3  # 3 payments per day
        rt_fees = calc_roundtrip_cost(scenario_maker_bnb)
        be_days = rt_fees / daily_fr if daily_fr > 0 else 999
        
        status = "✅" if pnl_usd_month > 0 else "❌"
        
        print(f"  {r['sym']:>16} | {r['fr_7d']*100:>9.4f}% | "
              f"{pnl['net_pnl_pct']*100:>+9.3f}% | "
              f"${pnl_usd_month:>+7.2f} | "
              f"${pnl_usd_year:>+8.2f} | "
              f"{roi_annual*100:>+9.1f}% | "
              f"{be_days:>8.1f}d {status}")
    
    # ═══════════════════════════════════════════════════════════
    # PARTE 5: RIESGOS Y CONSIDERACIONES
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'='*110}")
    print("  RIESGOS Y CONSIDERACIONES")
    print(f"{'='*110}")
    print("""
  ⚠️  RIESGOS PRINCIPALES:
  1. FR variable: el funding rate cambia cada 8h, puede volverse negativo
     → Si FR se vuelve negativo, VOS PAGÁS en vez de cobrar
  2. Liquidación futuros: si el precio sube mucho, tu short puede liquidarse
     → Con 5x margin (20%), liquidación aprox +80% del precio
  3. Basis risk: el premium/discount de futuros vs spot varía
     → Podés perder en el cierre si el basis cambió
  4. Spot availability: no todas las coins tienen spot líquido
  5. Withdrawal/deposit: mover fondos entre spot y futures tiene costos
  6. Contraparty risk: exchange risk (hack, freeze, etc.)

  ✅ MITIGACIONES:
  1. Solo operar coins con FR promedio ESTABLE (★★★) y OI alto (>$5M)
  2. Mantener margin ratio alto (20%+), setear stop-loss en futuros
  3. Monitorear FR cada 8h, cerrar si FR promedio 24h < 0
  4. Diversificar en 3-5 coins simultáneamente
  5. Usar limit orders (maker fees) + BNB para fees mínimos

  📊 COMPARACIÓN CON ALTERNATIVAS (referencia):
  - Lending USDT (Binance Earn): ~5-10% anual
  - Staking ETH: ~3-5% anual  
  - LP Farming (DeFi): ~10-30% anual (con impermanent loss)
  - Cash-and-carry (top picks): ~15-50% anual (con los riesgos citados)
""")


if __name__ == '__main__':
    asyncio.run(main())
