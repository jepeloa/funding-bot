"""
Basis Engine — Spot-Futures Arbitrage Calculations
===================================================
Core calculation module for basis trading:
  - Basis rate (absolute, %, annualized)
  - Fee-adjusted profitability per strategy
  - Funding carry estimation
  - Combined opportunity scoring (0-100)
  - Regime detection (contango, backwardation, extreme premium)

All functions are pure (no I/O) — data is fetched by the scanner/trader layers.
"""

from typing import Optional
import time


# ─────────────────────────────────────────────
# FEE SCHEDULE
# ─────────────────────────────────────────────

# Binance standard fees (no VIP, no BNB discount)
SPOT_TAKER_FEE = 0.001       # 0.10%
SPOT_MAKER_FEE = 0.001       # 0.10%
FUTURES_TAKER_FEE = 0.0005   # 0.05%
FUTURES_MAKER_FEE = 0.0002   # 0.02%

# With BNB discount (25% off)
SPOT_TAKER_FEE_BNB = 0.00075
SPOT_MAKER_FEE_BNB = 0.00075
FUTURES_TAKER_FEE_BNB = 0.000375
FUTURES_MAKER_FEE_BNB = 0.00018

# Strategy fee structures (round-trip = open + close both legs)
# Cash-and-carry: buy spot + short futures → sell spot + close futures
# Funding arb: same as cash-and-carry but held for multiple funding cycles


def get_fee_schedule(use_bnb: bool = False, use_maker: bool = False) -> dict:
    """Get fee rates for spot and futures based on discount tier."""
    if use_bnb:
        spot_fee = SPOT_MAKER_FEE_BNB if use_maker else SPOT_TAKER_FEE_BNB
        futures_fee = FUTURES_MAKER_FEE_BNB if use_maker else FUTURES_TAKER_FEE_BNB
    else:
        spot_fee = SPOT_MAKER_FEE if use_maker else SPOT_TAKER_FEE
        futures_fee = FUTURES_MAKER_FEE if use_maker else FUTURES_TAKER_FEE

    return {
        "spot_fee": spot_fee,
        "futures_fee": futures_fee,
        "spot_fee_pct": round(spot_fee * 100, 4),
        "futures_fee_pct": round(futures_fee * 100, 4),
        "round_trip_cost": 2 * (spot_fee + futures_fee),  # open + close both legs
        "round_trip_cost_pct": round(2 * (spot_fee + futures_fee) * 100, 4),
    }


# ─────────────────────────────────────────────
# BASIS CALCULATIONS
# ─────────────────────────────────────────────

def calculate_basis(
    spot_price: float,
    futures_price: float,
) -> dict:
    """
    Calculate spot-futures basis.

    Positive basis = futures premium (contango) → cash-and-carry opportunity
    Negative basis = futures discount (backwardation) → reverse cash-and-carry

    Returns:
        Dict with absolute, percentage, and annualized basis.
        For perpetuals, annualized uses 8h funding windows.
    """
    if spot_price <= 0:
        return {"error": "spot_price must be positive"}

    basis_abs = futures_price - spot_price
    basis_pct = (basis_abs / spot_price) * 100

    # For perpetuals, the "annualized" rate uses the current basis
    # extrapolated as if it converges every 8 hours (funding interval)
    # Annualized = basis_pct * (365 * 3) since funding is every 8h = 3x/day
    basis_annualized = basis_pct * 365 * 3

    # Regime classification
    if basis_pct > 0.5:
        regime = "EXTREME_PREMIUM"
    elif basis_pct > 0.1:
        regime = "CONTANGO"
    elif basis_pct > -0.1:
        regime = "NEUTRAL"
    elif basis_pct > -0.5:
        regime = "BACKWARDATION"
    else:
        regime = "EXTREME_DISCOUNT"

    return {
        "spot_price": spot_price,
        "futures_price": futures_price,
        "basis_absolute": round(basis_abs, 8),
        "basis_pct": round(basis_pct, 4),
        "basis_annualized_pct": round(basis_annualized, 2),
        "regime": regime,
        "direction": "PREMIUM" if basis_abs > 0 else "DISCOUNT",
    }


def calculate_fee_adjusted_profit(
    basis_pct: float,
    use_bnb: bool = False,
    use_maker: bool = False,
) -> dict:
    """
    Calculate net profit after fees for each basis strategy.

    Returns profitability for:
    - Cash-and-carry (round-trip: open + close both legs)
    - Funding arb (open both legs, hold for N funding cycles, close)
    - Basis scalp (same as cash-and-carry but directional)
    """
    fees = get_fee_schedule(use_bnb, use_maker)
    rt_cost = fees["round_trip_cost"]
    rt_cost_pct = rt_cost * 100

    # Cash-and-carry: capture the basis premium
    cc_profit_pct = abs(basis_pct) - rt_cost_pct
    cc_viable = cc_profit_pct > 0

    # Funding arb breakeven: how many 8h cycles to cover fees
    # If we only pay open fees (not close yet), that's half the round-trip
    open_cost = (fees["spot_fee"] + fees["futures_fee"])
    open_cost_pct = open_cost * 100

    return {
        "basis_pct": round(basis_pct, 4),
        "fees": fees,
        "cash_and_carry": {
            "gross_profit_pct": round(abs(basis_pct), 4),
            "round_trip_fee_pct": round(rt_cost_pct, 4),
            "net_profit_pct": round(cc_profit_pct, 4),
            "viable": cc_viable,
        },
        "funding_arb": {
            "open_fee_pct": round(open_cost_pct, 4),
            "note": "Net profit depends on accumulated funding over hold time",
        },
        "basis_scalp": {
            "gross_profit_pct": round(abs(basis_pct), 4),
            "round_trip_fee_pct": round(rt_cost_pct, 4),
            "net_profit_pct": round(cc_profit_pct, 4),
            "viable": cc_profit_pct > 0.05,  # need at least 0.05% net for scalps
        },
    }


def calculate_funding_carry(
    funding_rates: list[dict],
    holding_hours: float = 24.0,
) -> dict:
    """
    Analyze funding rate history and estimate carry from shorting futures.

    When you're short futures and funding is positive (longs pay shorts),
    you RECEIVE funding payments. This is the carry component.

    Args:
        funding_rates: List of dicts with 'funding_rate' and 'funding_time' keys
        holding_hours: Estimated hold time in hours

    Returns:
        Carry analysis with per-cycle, daily, and annualized rates.
    """
    if not funding_rates:
        return {"error": "no funding rate data"}

    rates = [f["funding_rate"] for f in funding_rates]

    # Basic stats
    avg_rate = sum(rates) / len(rates)
    max_rate = max(rates)
    min_rate = min(rates)
    latest_rate = rates[-1] if rates else 0

    # Positive funding = shorts receive (longs pay)
    # Negative funding = shorts pay (shorts pay longs)
    positive_count = sum(1 for r in rates if r > 0)
    positive_pct = (positive_count / len(rates) * 100) if rates else 0

    # Carry calculations
    # Funding happens every 8h, so per day = 3 cycles
    per_cycle_pct = latest_rate * 100
    per_day_pct = latest_rate * 3 * 100
    annualized_pct = latest_rate * 3 * 365 * 100

    # Estimated carry for holding period
    cycles_in_hold = holding_hours / 8
    estimated_carry_pct = avg_rate * cycles_in_hold * 100

    # Is it profitable for short holders (positive funding)?
    short_receives = latest_rate > 0

    return {
        "latest_rate": latest_rate,
        "latest_rate_pct": round(per_cycle_pct, 4),
        "avg_rate": round(avg_rate, 8),
        "avg_rate_pct": round(avg_rate * 100, 4),
        "max_rate_pct": round(max_rate * 100, 4),
        "min_rate_pct": round(min_rate * 100, 4),
        "per_day_pct": round(per_day_pct, 4),
        "annualized_pct": round(annualized_pct, 2),
        "positive_cycles_pct": round(positive_pct, 1),
        "short_receives_funding": short_receives,
        "estimated_carry": {
            "holding_hours": holding_hours,
            "funding_cycles": round(cycles_in_hold, 1),
            "estimated_carry_pct": round(estimated_carry_pct, 4),
        },
        "data_points": len(rates),
    }


# ─────────────────────────────────────────────
# COMBINED OPPORTUNITY SCORING
# ─────────────────────────────────────────────

def score_opportunity(
    basis_pct: float,
    funding_rate: float,
    funding_rates_history: list[float],
    volume_24h_usdt: float,
    spot_spread_bps: float = 0,
    futures_spread_bps: float = 0,
    use_bnb: bool = False,
    use_maker: bool = False,
) -> dict:
    """
    Score a basis trading opportunity from 0-100.

    Components:
    1. Basis magnitude (0-30 pts) — larger basis = more profit potential
    2. Funding carry (0-30 pts) — consistent positive funding = carry income
    3. Fee-adjusted viability (0-20 pts) — profit after fees
    4. Liquidity (0-10 pts) — higher volume = better execution
    5. Spread efficiency (0-10 pts) — tighter spreads = less slippage

    Returns:
        Score dict with total, breakdown, recommended strategy, and verdict.
    """
    fees = get_fee_schedule(use_bnb, use_maker)
    rt_cost_pct = fees["round_trip_cost"] * 100

    # ── 1. Basis magnitude (0-30) ──
    abs_basis = abs(basis_pct)
    if abs_basis > 1.0:
        basis_score = 30
    elif abs_basis > 0.5:
        basis_score = 20 + (abs_basis - 0.5) * 20
    elif abs_basis > 0.2:
        basis_score = 10 + (abs_basis - 0.2) * 33.3
    elif abs_basis > 0.05:
        basis_score = (abs_basis - 0.05) * 66.7
    else:
        basis_score = 0

    # ── 2. Funding carry (0-30) ──
    funding_score = 0
    if funding_rates_history:
        avg_funding = sum(funding_rates_history) / len(funding_rates_history)
        # Positive funding benefits short holders
        if avg_funding > 0:
            if avg_funding > 0.001:  # >0.1% per cycle = extreme
                funding_score = 30
            elif avg_funding > 0.0005:  # >0.05%
                funding_score = 20 + (avg_funding - 0.0005) * 20000
            elif avg_funding > 0.0001:  # >0.01%
                funding_score = 5 + (avg_funding - 0.0001) * 37500
            else:
                funding_score = avg_funding * 50000

        # Consistency bonus: high % of positive cycles
        positive_pct = sum(1 for r in funding_rates_history if r > 0) / len(funding_rates_history)
        if positive_pct > 0.8:
            funding_score = min(30, funding_score * 1.2)
    elif funding_rate > 0:
        funding_score = min(30, funding_rate * 30000)

    # ── 3. Fee-adjusted viability (0-20) ──
    net_profit = abs_basis - rt_cost_pct
    if net_profit > 0.5:
        fee_score = 20
    elif net_profit > 0.2:
        fee_score = 10 + (net_profit - 0.2) * 33.3
    elif net_profit > 0:
        fee_score = net_profit * 50
    else:
        fee_score = max(-10, net_profit * 20)  # Penalty if unprofitable

    # ── 4. Liquidity (0-10) ──
    if volume_24h_usdt > 100_000_000:  # >$100M
        liquidity_score = 10
    elif volume_24h_usdt > 10_000_000:  # >$10M
        liquidity_score = 7
    elif volume_24h_usdt > 1_000_000:  # >$1M
        liquidity_score = 4
    elif volume_24h_usdt > 100_000:  # >$100k
        liquidity_score = 2
    else:
        liquidity_score = 0

    # ── 5. Spread efficiency (0-10) ──
    total_spread = spot_spread_bps + futures_spread_bps
    if total_spread < 2:
        spread_score = 10
    elif total_spread < 5:
        spread_score = 7
    elif total_spread < 10:
        spread_score = 4
    elif total_spread < 20:
        spread_score = 2
    else:
        spread_score = 0

    # ── Total ──
    total = max(0, min(100, basis_score + funding_score + fee_score + liquidity_score + spread_score))

    # ── Strategy recommendation ──
    strategies = []
    if net_profit > 0.1:
        strategies.append("CASH_CARRY")
    if funding_rate > 0.0003:
        strategies.append("FUNDING_ARB")
    elif funding_rate > 0.0001 and funding_rates_history and sum(1 for r in funding_rates_history if r > 0) / len(funding_rates_history) > 0.7:
        strategies.append("FUNDING_ARB")
    if abs_basis > 0.3 and volume_24h_usdt > 5_000_000:
        strategies.append("BASIS_SCALP")

    if not strategies:
        if net_profit > 0:
            strategies.append("CASH_CARRY")
        elif funding_rate > 0:
            strategies.append("FUNDING_ARB")
        else:
            strategies.append("NONE")

    # ── Verdict ──
    if total >= 80:
        verdict = "EXCELENTE"
    elif total >= 60:
        verdict = "BUENA"
    elif total >= 40:
        verdict = "MODERADA"
    elif total >= 20:
        verdict = "DÉBIL"
    else:
        verdict = "NO_VIABLE"

    return {
        "total_score": round(total, 1),
        "verdict": verdict,
        "breakdown": {
            "basis_magnitude": round(basis_score, 1),
            "funding_carry": round(funding_score, 1),
            "fee_viability": round(fee_score, 1),
            "liquidity": round(liquidity_score, 1),
            "spread_efficiency": round(spread_score, 1),
        },
        "recommended_strategies": strategies,
        "primary_strategy": strategies[0] if strategies else "NONE",
        "key_metrics": {
            "basis_pct": round(basis_pct, 4),
            "funding_rate_pct": round(funding_rate * 100, 4),
            "net_profit_after_fees_pct": round(net_profit, 4),
            "round_trip_cost_pct": round(rt_cost_pct, 4),
            "volume_24h_usdt": round(volume_24h_usdt, 2),
        },
    }


# ─────────────────────────────────────────────
# TRADE SIZING & RISK
# ─────────────────────────────────────────────

def calculate_position_size(
    capital_usdt: float,
    spot_price: float,
    allocation_pct: float = 30.0,
    futures_leverage: int = 1,
    spot_info: Optional[dict] = None,
) -> dict:
    """
    Calculate position sizes for both legs of a basis trade.

    The spot leg requires full capital. The futures leg uses leverage.
    Total capital needed = spot_notional + futures_margin.

    Args:
        capital_usdt: Total available capital in USDT
        spot_price: Current spot price
        allocation_pct: % of capital to allocate to this trade (default 30%)
        futures_leverage: Leverage for futures leg (default 1x = same as spot)
        spot_info: Symbol info dict for min qty/notional validation

    Returns:
        Position sizing dict with quantities, margins, and validation.
    """
    allocated = capital_usdt * (allocation_pct / 100)

    # Split: spot needs full notional, futures needs notional/leverage as margin
    # For a neutral hedge: spot_qty == futures_qty
    # Total capital = spot_notional + futures_notional/leverage
    # spot_notional = qty * price
    # If spot_notional = X, futures_margin = X / leverage
    # Total = X + X/leverage = X * (1 + 1/leverage)
    # X = allocated / (1 + 1/leverage)

    spot_notional = allocated / (1 + 1 / futures_leverage)
    futures_margin = spot_notional / futures_leverage
    quantity = spot_notional / spot_price if spot_price > 0 else 0

    # Validate against minimums if info provided
    valid = True
    errors = []
    if spot_info:
        min_qty = spot_info.get("min_qty", 0)
        min_notional = spot_info.get("min_notional", 0)
        if quantity < min_qty:
            valid = False
            errors.append(f"Quantity {quantity:.8f} < min {min_qty}")
        if spot_notional < min_notional:
            valid = False
            errors.append(f"Notional ${spot_notional:.2f} < min ${min_notional}")

    return {
        "allocated_usdt": round(allocated, 2),
        "spot_leg": {
            "quantity": quantity,
            "notional_usdt": round(spot_notional, 2),
            "side": "BUY",
        },
        "futures_leg": {
            "quantity": quantity,
            "notional_usdt": round(spot_notional, 2),
            "margin_usdt": round(futures_margin, 2),
            "leverage": futures_leverage,
            "side": "SHORT",
        },
        "total_capital_used": round(spot_notional + futures_margin, 2),
        "capital_efficiency": round((spot_notional / (spot_notional + futures_margin)) * 100, 1),
        "valid": valid,
        "errors": errors,
    }


def estimate_pnl(
    entry_basis_pct: float,
    exit_basis_pct: float,
    notional_usdt: float,
    funding_collected_pct: float = 0,
    use_bnb: bool = False,
    use_maker: bool = False,
) -> dict:
    """
    Estimate PnL for a basis trade from entry to exit.

    Profit = (entry_basis - exit_basis) * notional + funding_collected - fees

    For cash-and-carry:
      - Entry: basis is high (buy spot, short futures)
      - Exit: basis converges (sell spot, close futures short)
      - Profit = basis_narrowing * notional

    Args:
        entry_basis_pct: Basis at entry (e.g. 0.5%)
        exit_basis_pct: Basis at exit (e.g. 0.1%)
        notional_usdt: Position size in USDT
        funding_collected_pct: Total funding collected as % of notional
        use_bnb: Using BNB fee discount
        use_maker: Using maker orders

    Returns:
        PnL breakdown dict.
    """
    fees = get_fee_schedule(use_bnb, use_maker)

    basis_profit_pct = entry_basis_pct - exit_basis_pct
    basis_profit_usdt = notional_usdt * (basis_profit_pct / 100)
    funding_usdt = notional_usdt * (funding_collected_pct / 100)
    fee_usdt = notional_usdt * fees["round_trip_cost"]

    net_pnl = basis_profit_usdt + funding_usdt - fee_usdt
    net_pnl_pct = (net_pnl / notional_usdt * 100) if notional_usdt > 0 else 0

    return {
        "entry_basis_pct": round(entry_basis_pct, 4),
        "exit_basis_pct": round(exit_basis_pct, 4),
        "notional_usdt": round(notional_usdt, 2),
        "basis_profit_usdt": round(basis_profit_usdt, 4),
        "funding_collected_usdt": round(funding_usdt, 4),
        "total_fees_usdt": round(fee_usdt, 4),
        "net_pnl_usdt": round(net_pnl, 4),
        "net_pnl_pct": round(net_pnl_pct, 4),
        "profitable": net_pnl > 0,
    }
