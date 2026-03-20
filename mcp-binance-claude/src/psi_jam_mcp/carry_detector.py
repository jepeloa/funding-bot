"""
Carry Detector — Funding Rate Carry Trade Scanner
==================================================
Detects tokens where persistent negative funding creates a carry opportunity
for LONG positions (shorts pay longs).

Strategy thesis:
  When funding is consistently negative (<-0.5%) over multiple intervals,
  shorts are paying a premium to maintain positions. If the price isn't in
  freefall (consolidating, low ATR), going LONG and collecting funding
  payments is a positive expected value trade.

  The key risk is a cascade liquidation event where price collapses 10-15%
  in minutes, hitting your SL before you collect enough funding.

Detection criteria:
  1. Persistent negative funding (<-0.5%) over 3+ intervals (not a spike)
  2. Price consolidation (ATR-based, not in freefall)
  3. Funding acceleration (trend getting MORE negative)
  4. ENERGY_TRAP active (OI tokens rising = shorts entering, while value drains)
  5. OI value ROC stabilizing (going from negative to positive = price floor forming)
  6. Risk assessment: cascade liquidation probability via OI + volume analysis

Two-phase approach (mirrors scanner.py / basis_scanner.py pattern):
  Phase 1: Fast — fetch all tickers + spot funding for extreme negatives
  Phase 2: Deep — for candidates, fetch funding history + klines + OI history
           to confirm persistence, consolidation, energy state, and score opportunity
"""

import asyncio
import time
import numpy as np
from typing import Optional

from .oi_chart import detect_energy_divergence


# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────

EXCLUDED_BASES = {
    "USDC", "BUSD", "TUSD", "FDUSD", "DAI", "USDP",
    "EUR", "GBP", "BRL", "TRY", "ARS",
}

DEFAULT_TOP_N = 15
DEFAULT_MIN_VOLUME = 5_000_000       # $5M 24h volume (need liquidity for carry)
DEFAULT_MIN_FUNDING_PCT = -0.5       # funding_rate * 100 < this → candidate
DEFAULT_MIN_PERSISTENCE = 3          # Consecutive intervals with extreme funding
DEFAULT_FUNDING_LIMIT = 30           # Funding history entries (3.3 days at 8h intervals)
DEFAULT_KLINE_LIMIT = 72             # 72 × 1h = 3 days of price data
DEFAULT_OI_LIMIT = 30               # OI history entries (30 × 1h = 30 hours)
DEFAULT_OI_WINDOW = 6               # ROC window for OI energy computation
MAX_CONCURRENT = 10
MAX_ATR_RATIO = 0.06                 # ATR/price < 6% → consolidating (not freefall)


# ─────────────────────────────────────────────
# CARRY DETECTOR
# ─────────────────────────────────────────────

# ─────────────────────────────────────────────
# FUNDING INTERVAL HELPERS
# ─────────────────────────────────────────────

# Valid Binance funding intervals: 1h, 2h, 4h, 8h
_VALID_INTERVALS = [1, 2, 4, 8]


def detect_funding_interval(funding_data: list[dict]) -> dict:
    """Detect the funding collection interval from timestamps.

    Binance tokens can have 8h (default), 4h, 2h, or 1h funding.
    Shorter interval → more collections per day → higher carry yield
    for the same per-interval rate.

    Returns:
        interval_hours: detected interval in hours (1, 2, 4, or 8)
        collections_per_day: how many funding events per day (24, 12, 6, or 3)
        frequency_multiplier: how many times faster vs 8h baseline (1x, 2x, 4x, 8x)
        category: "TURBO" (1h), "FAST" (2h), "ACCELERATED" (4h), "STANDARD" (8h)
    """
    if not funding_data or len(funding_data) < 2:
        return {
            "interval_hours": 8,
            "collections_per_day": 3,
            "frequency_multiplier": 1,
            "category": "STANDARD",
        }

    # Compute intervals between consecutive funding timestamps
    deltas_h = []
    for i in range(1, len(funding_data)):
        t1 = funding_data[i].get("funding_time", 0)
        t0 = funding_data[i - 1].get("funding_time", 0)
        if t1 > t0:
            deltas_h.append((t1 - t0) / 3_600_000)

    if not deltas_h:
        return {
            "interval_hours": 8,
            "collections_per_day": 3,
            "frequency_multiplier": 1,
            "category": "STANDARD",
        }

    # Median is more robust than mean (handles gaps/outliers)
    median_h = float(np.median(deltas_h))

    # Snap to nearest valid interval
    interval_h = min(_VALID_INTERVALS, key=lambda x: abs(x - median_h))
    collections = 24 // interval_h
    multiplier = 8 // interval_h

    if interval_h <= 1:
        category = "TURBO"
    elif interval_h <= 2:
        category = "FAST"
    elif interval_h <= 4:
        category = "ACCELERATED"
    else:
        category = "STANDARD"

    return {
        "interval_hours": interval_h,
        "collections_per_day": collections,
        "frequency_multiplier": multiplier,
        "category": category,
    }


class CarryDetector:
    """
    Two-phase carry trade scanner.
    Phase 1: Fast screening (all tickers → extreme negative funding)
    Phase 2: Deep analysis (funding persistence + price consolidation + risk)
    """

    def __init__(self, futures_client):
        self.client = futures_client

    # ─────────────────────────────────────────
    # PHASE 1: FAST SCREEN
    # ─────────────────────────────────────────

    async def _phase1_screen(
        self,
        min_volume: float,
        min_funding_pct: float,
    ) -> dict:
        """
        Quick screen: fetch all tickers + exchange info, then fetch last
        funding rate for liquid pairs to find extreme negatives.
        """
        t0 = time.time()

        # Parallel: all tickers + exchange info
        ticker_resp, info_resp = await asyncio.gather(
            self.client.client.get("/fapi/v1/ticker/24hr"),
            self.client.client.get("/fapi/v1/exchangeInfo"),
        )
        ticker_resp.raise_for_status()
        info_resp.raise_for_status()

        all_tickers = ticker_resp.json()
        perpetual_symbols = {
            s["symbol"]
            for s in info_resp.json().get("symbols", [])
            if s.get("contractType") == "PERPETUAL"
            and s.get("quoteAsset") == "USDT"
            and s.get("status") == "TRADING"
        }

        # Parse and filter by volume
        liquid_pairs = []
        for t in all_tickers:
            sym = t.get("symbol", "")
            if sym not in perpetual_symbols:
                continue
            base = sym.replace("USDT", "")
            if base in EXCLUDED_BASES:
                continue
            try:
                qvol = float(t.get("quoteVolume", 0))
                price = float(t.get("lastPrice", 0))
                change = float(t.get("priceChangePercent", 0))
                high = float(t.get("highPrice", 0))
                low = float(t.get("lowPrice", 0))
            except (ValueError, TypeError):
                continue
            if qvol < min_volume or price <= 0:
                continue
            liquid_pairs.append({
                "symbol": sym,
                "price": price,
                "change_pct_24h": change,
                "quote_volume": round(qvol, 2),
                "high_24h": high,
                "low_24h": low,
            })

        # Fetch last funding rate for all liquid pairs (batch via premiumIndex)
        premium_resp = await self.client.client.get("/fapi/v1/premiumIndex")
        premium_resp.raise_for_status()
        premium_data = {
            p["symbol"]: float(p.get("lastFundingRate", 0))
            for p in premium_resp.json()
        }

        # Filter: only pairs with funding below threshold
        candidates = []
        for pair in liquid_pairs:
            sym = pair["symbol"]
            funding = premium_data.get(sym, 0)
            funding_pct = funding * 100
            if funding_pct > min_funding_pct:
                continue  # Not negative enough
            pair["last_funding_rate"] = funding
            pair["last_funding_pct"] = round(funding_pct, 4)
            candidates.append(pair)

        # Sort by most extreme negative funding
        candidates.sort(key=lambda x: x["last_funding_pct"])

        # ── Detect funding interval for candidates ──
        # Fetch 5 funding entries per candidate to derive interval (lightweight)
        sem = asyncio.Semaphore(MAX_CONCURRENT)

        async def _fetch_interval(pair: dict) -> None:
            sym = pair["symbol"]
            async with sem:
                try:
                    fdata = await self.client.get_funding_rate(sym, limit=5)
                    fi = detect_funding_interval(fdata)
                    pair["funding_interval"] = fi
                except Exception:
                    pair["funding_interval"] = detect_funding_interval([])

        await asyncio.gather(*[_fetch_interval(c) for c in candidates])

        elapsed = round(time.time() - t0, 2)
        return {
            "candidates": candidates,
            "total_liquid": len(liquid_pairs),
            "total_candidates": len(candidates),
            "elapsed_sec": elapsed,
        }

    # ─────────────────────────────────────────
    # PHASE 2: DEEP ANALYSIS
    # ─────────────────────────────────────────

    async def _phase2_deep_analysis(
        self,
        candidates: list[dict],
        top_n: int,
        min_persistence: int,
        funding_limit: int,
        kline_limit: int,
    ) -> list[dict]:
        """
        For top candidates, fetch funding history + klines + OI
        to confirm persistence, consolidation, and score the opportunity.
        """
        # Limit to top candidates by funding extremity
        to_analyze = candidates[:min(top_n * 2, 40)]
        if not to_analyze:
            return []

        sem = asyncio.Semaphore(MAX_CONCURRENT)

        async def _analyze_one(pair: dict) -> Optional[dict]:
            sym = pair["symbol"]
            async with sem:
                try:
                    funding_data, klines, oi_data, oi_hist = await asyncio.gather(
                        self.client.get_funding_rate(sym, limit=funding_limit),
                        self.client.get_klines(sym, interval="1h", limit=kline_limit),
                        self._safe_oi(sym),
                        self._safe_oi_hist(sym),
                    )
                except Exception:
                    return None

            if not funding_data or len(funding_data) < min_persistence:
                return None
            if not klines or len(klines) < 20:
                return None

            # ── Funding persistence analysis ──
            funding_analysis = self._analyze_funding_persistence(
                funding_data, min_persistence
            )
            if not funding_analysis["is_persistent"]:
                return None

            # ── Price consolidation analysis ──
            price_analysis = self._analyze_price_consolidation(klines)

            # ── OI energy analysis ──
            oi_energy = self._analyze_oi_energy(oi_hist, klines)

            # ── Risk assessment ──
            risk = self._assess_cascade_risk(klines, oi_data, funding_data)

            # ── Carry profitability estimate ──
            carry = self._estimate_carry_profit(funding_analysis, price_analysis)

            # ── Composite score ──
            score = self._compute_score(funding_analysis, price_analysis, risk, carry, oi_energy)

            return {
                "symbol": sym,
                "price": pair["price"],
                "change_pct_24h": pair["change_pct_24h"],
                "quote_volume": pair["quote_volume"],
                "funding": funding_analysis,
                "price_analysis": price_analysis,
                "oi_energy": oi_energy,
                "risk": risk,
                "carry": carry,
                "score": score,
            }

        # Run all analyses in parallel
        results = await asyncio.gather(
            *[_analyze_one(p) for p in to_analyze]
        )

        # Filter None results and sort by score
        scored = [r for r in results if r is not None]
        scored.sort(key=lambda x: x["score"]["total"], reverse=True)
        return scored[:top_n]

    async def _safe_oi(self, symbol: str) -> Optional[dict]:
        """Fetch current OI with error handling."""
        try:
            return await self.client.get_open_interest(symbol)
        except Exception:
            return None

    async def _safe_oi_hist(self, symbol: str, period: str = "1h", limit: int = DEFAULT_OI_LIMIT) -> Optional[list]:
        """Fetch OI history with error handling."""
        try:
            return await self.client.get_open_interest_hist(symbol, period=period, limit=limit)
        except Exception:
            return None

    # ─────────────────────────────────────────
    # ANALYSIS FUNCTIONS
    # ─────────────────────────────────────────

    def _analyze_funding_persistence(
        self, funding_data: list[dict], min_persistence: int
    ) -> dict:
        """
        Analyze if negative funding is persistent (not just a spike).
        Returns: persistence metrics, trend direction, acceleration.
        """
        rates = [f["funding_rate"] for f in funding_data]
        rates_pct = [r * 100 for r in rates]
        times = [f["funding_time"] for f in funding_data]

        # Detect funding interval
        fi = detect_funding_interval(funding_data)

        # Count consecutive negative intervals from latest
        consecutive = 0
        for r in reversed(rates):
            if r < 0:
                consecutive += 1
            else:
                break

        # Count intervals below -0.5% threshold
        extreme_count = sum(1 for r in rates_pct if r < -0.5)

        # Is increasing in magnitude? (getting more negative)
        # Compare first half vs second half average
        n = len(rates_pct)
        if n >= 4:
            first_half = np.mean(rates_pct[:n // 2])
            second_half = np.mean(rates_pct[n // 2:])
            is_accelerating = second_half < first_half  # More negative
            acceleration = round(second_half - first_half, 4)
        else:
            is_accelerating = False
            acceleration = 0.0

        # Linear regression on funding rates → trend
        if n >= 3:
            x = np.arange(n, dtype=float)
            slope = float(np.polyfit(x, rates_pct, 1)[0])
        else:
            slope = 0.0

        # Cumulative funding paid by shorts (what longs would collect)
        cumulative_rate = sum(rates)
        cumulative_pct = round(cumulative_rate * 100, 4)

        # Time-weighted average funding (per 8h interval)
        avg_rate = np.mean(rates)
        avg_pct = round(avg_rate * 100, 4)

        # Last 3 intervals detail
        recent = []
        for f in funding_data[-min(3, len(funding_data)):]:
            recent.append({
                "time": f["funding_time"],
                "rate_pct": round(f["funding_rate"] * 100, 4),
            })

        is_persistent = (
            consecutive >= min_persistence
            and extreme_count >= min_persistence
        )

        # Compute 24h funding based on actual interval
        cpd = fi["collections_per_day"]
        funding_24h_pct = round(sum(r * 100 for r in rates[-cpd:]), 4) if n >= cpd else round(avg_pct * cpd, 4)

        return {
            "is_persistent": is_persistent,
            "consecutive_negative": consecutive,
            "extreme_intervals": extreme_count,
            "total_intervals": n,
            "avg_funding_pct": avg_pct,
            "cumulative_funding_pct": cumulative_pct,
            "is_accelerating": is_accelerating,
            "acceleration": acceleration,
            "trend_slope": round(slope, 5),
            "recent": recent,
            "funding_24h_pct": funding_24h_pct,
            "funding_interval": fi,
        }

    def _analyze_price_consolidation(self, klines: list[dict]) -> dict:
        """
        Determine if price is consolidating (good for carry) or in freefall (bad).
        Uses ATR, price range, and volatility metrics.
        """
        closes = np.array([k["close"] for k in klines])
        highs = np.array([k["high"] for k in klines])
        lows = np.array([k["low"] for k in klines])

        current_price = closes[-1]

        # ATR (14-period)
        tr = np.maximum(
            highs[1:] - lows[1:],
            np.maximum(
                np.abs(highs[1:] - closes[:-1]),
                np.abs(lows[1:] - closes[:-1])
            )
        )
        atr_14 = float(np.mean(tr[-14:])) if len(tr) >= 14 else float(np.mean(tr))
        atr_ratio = atr_14 / current_price if current_price > 0 else 0

        # Price change over analysis window
        price_start = closes[0]
        total_change_pct = ((current_price - price_start) / price_start * 100) if price_start > 0 else 0

        # Volatility (std of hourly returns)
        returns = np.diff(closes) / closes[:-1]
        hourly_vol = float(np.std(returns))
        daily_vol = hourly_vol * np.sqrt(24)  # Annualize to daily

        # Max drawdown in the window
        peak = np.maximum.accumulate(closes)
        drawdowns = (closes - peak) / peak * 100
        max_drawdown = float(np.min(drawdowns))

        # Is the price trending down steeply? (linear regression)
        x = np.arange(len(closes), dtype=float)
        slope_per_hour = float(np.polyfit(x, closes, 1)[0])
        slope_pct_per_hour = (slope_per_hour / current_price * 100) if current_price > 0 else 0

        # Consolidation check: ATR is low AND price isn't in freefall
        is_consolidating = (
            atr_ratio < MAX_ATR_RATIO
            and max_drawdown > -10  # Not more than 10% drawdown in window
            and abs(slope_pct_per_hour) < 0.15  # Less than 0.15% per hour trend
        )

        # How many hours to lose the cumulative funding collected?
        # (breakeven drawdown rate)

        return {
            "is_consolidating": is_consolidating,
            "atr_14": round(atr_14, 6),
            "atr_ratio": round(atr_ratio, 5),
            "total_change_pct": round(total_change_pct, 2),
            "max_drawdown_pct": round(max_drawdown, 2),
            "hourly_volatility": round(hourly_vol, 6),
            "daily_volatility_pct": round(daily_vol * 100, 2),
            "price_slope_pct_per_hour": round(slope_pct_per_hour, 4),
            "current_price": current_price,
        }

    def _analyze_oi_energy(
        self, oi_hist: Optional[list], klines: list[dict],
        window: int = DEFAULT_OI_WINDOW,
    ) -> dict:
        """
        Analyze OI energy state for carry trade confirmation.

        Key conditions for carry entry:
          1. ENERGY_TRAP active: OI tokens rising (shorts entering) while OI value drains
          2. OI value ROC stabilizing: going from negative to positive (price floor forming)

        Uses the same detect_energy_divergence() from oi_chart.py.
        """
        result = {
            "available": False,
            "energy_trap_active": False,
            "trap_streak": 0,
            "oi_value_roc_stabilizing": False,
            "current_state": "UNKNOWN",
            "current_oi_tokens_roc": 0.0,
            "current_oi_value_roc": 0.0,
            "current_energy_delta": 0.0,
            "oi_energy_score": 0,
            "states_history": [],
        }

        if not oi_hist or len(oi_hist) < window + 2:
            return result

        result["available"] = True

        # Compute ROC and energy states for each OI history point
        states = []
        oi_value_rocs = []
        for i in range(window, len(oi_hist)):
            cur = oi_hist[i]
            lb = oi_hist[i - window]

            cur_tok = cur.get("sum_open_interest", 0)
            lb_tok = lb.get("sum_open_interest", 0)
            cur_val = cur.get("sum_open_interest_value", 0)
            lb_val = lb.get("sum_open_interest_value", 0)

            if lb_tok > 0 and cur_tok > 0:
                oi_tokens_roc = (cur_tok / lb_tok - 1) * 100
            else:
                oi_tokens_roc = 0.0

            if lb_val > 0 and cur_val > 0:
                oi_value_roc = (cur_val / lb_val - 1) * 100
            else:
                oi_value_roc = 0.0

            energy_delta = oi_value_roc - oi_tokens_roc
            state = detect_energy_divergence(oi_value_roc, oi_tokens_roc)

            states.append({
                "timestamp": cur.get("timestamp", 0),
                "state": state,
                "oi_tokens_roc": round(oi_tokens_roc, 4),
                "oi_value_roc": round(oi_value_roc, 4),
                "energy_delta": round(energy_delta, 4),
            })
            oi_value_rocs.append(oi_value_roc)

        if not states:
            return result

        # Current state
        latest = states[-1]
        result["current_state"] = latest["state"]
        result["current_oi_tokens_roc"] = latest["oi_tokens_roc"]
        result["current_oi_value_roc"] = latest["oi_value_roc"]
        result["current_energy_delta"] = latest["energy_delta"]

        # ENERGY_TRAP streak (consecutive from end)
        trap_streak = 0
        for s in reversed(states):
            if s["state"] == "ENERGY_TRAP":
                trap_streak += 1
            else:
                break
        result["trap_streak"] = trap_streak
        result["energy_trap_active"] = trap_streak >= 3

        # Total TRAP count
        trap_count = sum(1 for s in states if s["state"] == "ENERGY_TRAP")
        result["trap_count"] = trap_count

        # OI value ROC stabilization: was negative, now turning positive
        # Check last 3-5 values for trend reversal
        if len(oi_value_rocs) >= 3:
            recent_rocs = oi_value_rocs[-3:]
            older_rocs = oi_value_rocs[-6:-3] if len(oi_value_rocs) >= 6 else oi_value_rocs[:3]
            avg_recent = float(np.mean(recent_rocs))
            avg_older = float(np.mean(older_rocs))
            # Stabilizing: older was more negative, recent is less negative or positive
            result["oi_value_roc_stabilizing"] = (
                avg_older < -0.5 and avg_recent > avg_older + 1.0
            ) or (
                avg_older < 0 and avg_recent > 0
            )
            result["avg_recent_oi_value_roc"] = round(avg_recent, 4)
            result["avg_older_oi_value_roc"] = round(avg_older, 4)

        # OI Energy Score (0-100) for carry quality
        # ENERGY_TRAP active = shorts entering = they'll keep paying funding
        energy_score = 0
        if trap_streak >= 6:
            energy_score += 50  # Strong trap
        elif trap_streak >= 3:
            energy_score += 35  # Moderate trap
        elif trap_count >= 3:
            energy_score += 15  # Historical traps but not consecutive now

        if result["oi_value_roc_stabilizing"]:
            energy_score += 30  # Price floor forming

        # Bonus: current state is TRAP (even if streak < 3)
        if latest["state"] == "ENERGY_TRAP" and trap_streak < 3:
            energy_score += 10

        # Penalty: DELEVERAGING = active crash
        if latest["state"] == "ENERGY_DELEVERAGING":
            energy_score = max(0, energy_score - 30)

        result["oi_energy_score"] = min(100, energy_score)

        # State counts
        state_counts = {}
        for s in states:
            st = s["state"]
            state_counts[st] = state_counts.get(st, 0) + 1
        result["state_counts"] = state_counts

        # Last 5 states for display
        result["states_history"] = states[-5:]

        return result

    def _assess_cascade_risk(
        self, klines: list[dict], oi_data: Optional[dict], funding_data: list[dict]
    ) -> dict:
        """
        Assess the risk of a cascade liquidation event.
        High OI + extreme funding + low volume = high cascade risk.
        """
        closes = np.array([k["close"] for k in klines])
        volumes = np.array([k["quote_volume"] for k in klines])

        # Volume trend (decaying volume = less support)
        n = len(volumes)
        if n >= 6:
            recent_vol = float(np.mean(volumes[-6:]))
            older_vol = float(np.mean(volumes[-24:-6])) if n >= 24 else float(np.mean(volumes[:n-6]))
            vol_ratio = recent_vol / older_vol if older_vol > 0 else 1.0
        else:
            vol_ratio = 1.0
            recent_vol = float(np.mean(volumes))
            older_vol = recent_vol

        # OI relative to volume (crowded trade indicator)
        oi_value = 0
        oi_vol_ratio = 0
        if oi_data:
            oi_value = oi_data.get("open_interest", 0) * closes[-1]
            daily_vol = float(np.sum(volumes[-24:])) if n >= 24 else float(np.sum(volumes))
            oi_vol_ratio = oi_value / daily_vol if daily_vol > 0 else 0

        # Funding extremity (more extreme = more crowded shorts = more risk)
        last_funding = funding_data[-1]["funding_rate"] if funding_data else 0
        avg_funding = np.mean([f["funding_rate"] for f in funding_data])

        # Risk score 0-100 (higher = more dangerous)
        risk_factors = []

        # F1: Volume declining → less liquidity to absorb cascades
        if vol_ratio < 0.5:
            risk_factors.append(("declining_volume", 25))
        elif vol_ratio < 0.8:
            risk_factors.append(("declining_volume", 10))

        # F2: OI/Volume ratio too high → crowded
        if oi_vol_ratio > 5:
            risk_factors.append(("crowded_oi", 30))
        elif oi_vol_ratio > 3:
            risk_factors.append(("crowded_oi", 15))
        elif oi_vol_ratio > 1.5:
            risk_factors.append(("crowded_oi", 5))

        # F3: Extremely negative funding → potential for short squeeze OR cascade
        if abs(last_funding) > 0.03:  # >3%
            risk_factors.append(("extreme_funding", 20))
        elif abs(last_funding) > 0.01:  # >1%
            risk_factors.append(("extreme_funding", 10))

        # F4: Price already dropped significantly
        total_drop = ((closes[-1] - closes[0]) / closes[0] * 100) if closes[0] > 0 else 0
        if total_drop < -8:
            risk_factors.append(("steep_decline", 25))
        elif total_drop < -5:
            risk_factors.append(("steep_decline", 10))

        total_risk = min(100, sum(r[1] for r in risk_factors))

        # Classify
        if total_risk >= 60:
            risk_level = "ALTO"
        elif total_risk >= 30:
            risk_level = "MEDIO"
        else:
            risk_level = "BAJO"

        return {
            "risk_score": total_risk,
            "risk_level": risk_level,
            "factors": {name: score for name, score in risk_factors},
            "volume_trend_ratio": round(vol_ratio, 3),
            "oi_notional_usd": round(oi_value, 2),
            "oi_to_daily_vol": round(oi_vol_ratio, 3),
        }

    def _estimate_carry_profit(self, funding: dict, price: dict) -> dict:
        """
        Estimate carry profitability: funding income vs price risk.
        Uses actual funding interval (1h/2h/4h/8h) for correct daily calculation.
        """
        fi = funding.get("funding_interval", {})
        interval_h = fi.get("interval_hours", 8)
        cpd = fi.get("collections_per_day", 3)

        avg_funding_per_interval = abs(funding["avg_funding_pct"])  # % per interval
        daily_funding = avg_funding_per_interval * cpd
        weekly_funding = daily_funding * 7
        monthly_funding = daily_funding * 30
        annualized = daily_funding * 365

        # Net carry (funding collected - price movement cost)
        price_cost_daily = abs(price["price_slope_pct_per_hour"]) * 24
        net_daily = daily_funding - price_cost_daily
        net_weekly = net_daily * 7

        # Breakeven
        breakeven_drop_daily = daily_funding

        # SL recommendation: wider than normal since carry compensates
        recommended_sl_pct = max(3.0, daily_funding * 3)

        return {
            "avg_funding_per_interval_pct": round(avg_funding_per_interval, 4),
            "interval_hours": interval_h,
            "collections_per_day": cpd,
            "daily_carry_pct": round(daily_funding, 4),
            "weekly_carry_pct": round(weekly_funding, 4),
            "monthly_carry_pct": round(monthly_funding, 2),
            "annualized_carry_pct": round(annualized, 2),
            "net_daily_pct": round(net_daily, 4),
            "net_weekly_pct": round(net_weekly, 4),
            "breakeven_daily_drop_pct": round(breakeven_drop_daily, 4),
            "recommended_sl_pct": round(recommended_sl_pct, 2),
        }

    def _compute_score(self, funding: dict, price: dict, risk: dict, carry: dict,
                        oi_energy: Optional[dict] = None) -> dict:
        """
        Composite score 0-100 for carry opportunity quality.
        Weights: Persistence (25), Consolidation (20), Carry yield (20),
                 OI Energy (20), Risk inv (15)
        """
        # Persistence score (0-100)
        persist = min(100, (
            min(funding["consecutive_negative"] / 6, 1.0) * 50 +
            min(funding["extreme_intervals"] / 8, 1.0) * 30 +
            (20 if funding["is_accelerating"] else 0)
        ))

        # Consolidation score (0-100)
        if price["is_consolidating"]:
            consol = 70 + min(30, (1 - price["atr_ratio"] / MAX_ATR_RATIO) * 30)
        else:
            consol = max(0, 50 - abs(price["total_change_pct"]) * 5)

        # Carry yield score (0-100)
        daily = carry["daily_carry_pct"]
        if daily >= 3.0:
            yield_score = 100
        elif daily >= 1.5:
            yield_score = 70 + (daily - 1.5) / 1.5 * 30
        elif daily >= 0.5:
            yield_score = 30 + (daily - 0.5) / 1.0 * 40
        else:
            yield_score = daily / 0.5 * 30

        # OI Energy score (0-100)
        # ENERGY_TRAP = shorts entering = they keep paying funding = great for carry
        # OI value ROC stabilizing = price floor forming = less drawdown risk
        if oi_energy and oi_energy.get("available"):
            oi_score = oi_energy["oi_energy_score"]
        else:
            oi_score = 40  # Neutral if OI data unavailable

        # Inverse risk score (low risk = high score)
        inv_risk = 100 - risk["risk_score"]

        # Weighted total (5 dimensions)
        total = (
            persist * 0.25 +
            consol * 0.20 +
            yield_score * 0.20 +
            oi_score * 0.20 +
            inv_risk * 0.15
        )

        # Determine verdict
        if total >= 75:
            verdict = "CARRY_EXCELENTE"
        elif total >= 55:
            verdict = "CARRY_VIABLE"
        elif total >= 35:
            verdict = "CARRY_MARGINAL"
        else:
            verdict = "NO_CARRY"

        return {
            "total": round(total, 1),
            "persistence": round(persist, 1),
            "consolidation": round(consol, 1),
            "yield_score": round(yield_score, 1),
            "oi_energy": round(oi_score, 1),
            "inverse_risk": round(inv_risk, 1),
            "verdict": verdict,
        }

    # ─────────────────────────────────────────
    # PUBLIC API
    # ─────────────────────────────────────────

    async def scan(
        self,
        top_n: int = DEFAULT_TOP_N,
        min_volume: float = DEFAULT_MIN_VOLUME,
        min_funding_pct: float = DEFAULT_MIN_FUNDING_PCT,
        min_persistence: int = DEFAULT_MIN_PERSISTENCE,
        funding_limit: int = DEFAULT_FUNDING_LIMIT,
        kline_limit: int = DEFAULT_KLINE_LIMIT,
    ) -> dict:
        """
        Full carry scan pipeline.

        Phase 1: Fetch all tickers + premiumIndex, filter by extreme negative funding.
        Phase 2: For candidates, fetch funding history + klines + OI,
                 confirm persistence, analyze consolidation, score.

        Returns dict with ranked opportunities and scan metadata.
        """
        t_start = time.time()

        # Phase 1
        phase1 = await self._phase1_screen(min_volume, min_funding_pct)

        # Phase 2
        opportunities = await self._phase2_deep_analysis(
            candidates=phase1["candidates"],
            top_n=top_n,
            min_persistence=min_persistence,
            funding_limit=funding_limit,
            kline_limit=kline_limit,
        )

        elapsed = round(time.time() - t_start, 2)

        return {
            "scan_type": "carry_detector",
            "timestamp": int(time.time() * 1000),
            "phase1_liquid_pairs": phase1["total_liquid"],
            "phase1_candidates": phase1["total_candidates"],
            "opportunities_found": len(opportunities),
            "opportunities": opportunities,
            "parameters": {
                "min_volume_usd": min_volume,
                "min_funding_pct": min_funding_pct,
                "min_persistence_intervals": min_persistence,
                "funding_history_intervals": funding_limit,
                "kline_hours": kline_limit,
            },
            "timing": {
                "phase1_sec": phase1["elapsed_sec"],
                "total_sec": elapsed,
            },
        }

    async def quick_scan(
        self,
        min_volume: float = DEFAULT_MIN_VOLUME,
        min_funding_pct: float = DEFAULT_MIN_FUNDING_PCT,
        top_n: int = 30,
    ) -> dict:
        """
        Phase 1 only — quick snapshot of tokens with extreme negative funding.
        No persistence check, no scoring. Good for a fast overview.
        """
        phase1 = await self._phase1_screen(min_volume, min_funding_pct)

        return {
            "scan_type": "carry_quick",
            "timestamp": int(time.time() * 1000),
            "total_liquid_pairs": phase1["total_liquid"],
            "candidates_found": phase1["total_candidates"],
            "candidates": phase1["candidates"][:top_n],
            "parameters": {
                "min_volume_usd": min_volume,
                "min_funding_pct": min_funding_pct,
            },
            "timing": {
                "elapsed_sec": phase1["elapsed_sec"],
            },
        }

    async def analyze_symbol(
        self,
        symbol: str,
        funding_limit: int = DEFAULT_FUNDING_LIMIT,
        kline_limit: int = DEFAULT_KLINE_LIMIT,
    ) -> dict:
        """
        Deep carry analysis for a single symbol.
        Returns full breakdown even if it doesn't meet scanner thresholds.
        """
        t_start = time.time()
        symbol = symbol.upper()

        # Fetch all data in parallel
        try:
            funding_data, klines, oi_data, oi_hist, ticker = await asyncio.gather(
                self.client.get_funding_rate(symbol, limit=funding_limit),
                self.client.get_klines(symbol, interval="1h", limit=kline_limit),
                self._safe_oi(symbol),
                self._safe_oi_hist(symbol),
                self.client.get_ticker_24h(symbol),
            )
        except Exception as e:
            return {"error": f"No se pudo obtener datos para {symbol}: {str(e)}"}

        if not funding_data:
            return {"error": f"Sin historial de funding para {symbol}"}
        if not klines or len(klines) < 10:
            return {"error": f"Sin suficientes klines para {symbol}"}

        funding_analysis = self._analyze_funding_persistence(funding_data, 1)
        price_analysis = self._analyze_price_consolidation(klines)
        oi_energy = self._analyze_oi_energy(oi_hist, klines)
        risk = self._assess_cascade_risk(klines, oi_data, funding_data)
        carry = self._estimate_carry_profit(funding_analysis, price_analysis)
        score = self._compute_score(funding_analysis, price_analysis, risk, carry, oi_energy)

        elapsed = round(time.time() - t_start, 2)

        return {
            "symbol": symbol,
            "price": ticker.get("last_price", 0),
            "change_pct_24h": ticker.get("price_change_pct", 0),
            "quote_volume": ticker.get("quote_volume", 0),
            "funding": funding_analysis,
            "price_analysis": price_analysis,
            "oi_energy": oi_energy,
            "risk": risk,
            "carry": carry,
            "score": score,
            "timing": {"elapsed_sec": elapsed},
        }

    async def monitor_exit(
        self,
        symbol: str,
        entry_funding_pct: float,
        funding_limit: int = 10,
    ) -> dict:
        """
        Monitor a carry position for exit signals.
        Key exit signal: funding normalizing (becoming less negative).

        Args:
            symbol: The pair being carried
            entry_funding_pct: The avg funding % when the trade was entered
            funding_limit: How many intervals to check
        """
        symbol = symbol.upper()

        try:
            funding_data = await self.client.get_funding_rate(symbol, limit=funding_limit)
        except Exception as e:
            return {"error": f"No se pudo obtener funding para {symbol}: {str(e)}"}

        if not funding_data:
            return {"error": f"Sin datos de funding para {symbol}"}

        rates_pct = [f["funding_rate"] * 100 for f in funding_data]
        current_pct = rates_pct[-1]
        avg_recent = np.mean(rates_pct[-3:]) if len(rates_pct) >= 3 else current_pct

        # Exit signals
        signals = []

        # S1: Funding normalized (close to 0 or positive)
        if current_pct > -0.1:
            signals.append({
                "signal": "FUNDING_NORMALIZADO",
                "severity": "CERRAR",
                "detail": f"Funding actual: {current_pct:.4f}% — ya no es negativo, el carry terminó.",
            })

        # S2: Funding reducing significantly (from entry)
        entry_abs = abs(entry_funding_pct)
        current_abs = abs(current_pct)
        if entry_abs > 0 and current_abs < entry_abs * 0.3:
            signals.append({
                "signal": "FUNDING_REDUCIDO",
                "severity": "CERRAR",
                "detail": (
                    f"Funding se redujo de {entry_funding_pct:.4f}% a {current_pct:.4f}% "
                    f"(−{((1 - current_abs/entry_abs) * 100):.0f}% de intensidad). El carry ya no compensa."
                ),
            })

        # S3: Funding flipping direction (was negative, trending positive)
        if len(rates_pct) >= 3:
            trend = rates_pct[-1] - rates_pct[-3]
            if trend > 0.3:  # Trending positive fast
                signals.append({
                    "signal": "FUNDING_REVIRTIENDO",
                    "severity": "PREPARAR_SALIDA",
                    "detail": (
                        f"Funding subiendo rápido: {rates_pct[-3]:.4f}% → {current_pct:.4f}% "
                        f"(+{trend:.4f}% en 3 intervalos). Short squeeze inminente."
                    ),
                })

        # S4: Funding still strong — hold
        if not signals:
            signals.append({
                "signal": "CARRY_ACTIVO",
                "severity": "MANTENER",
                "detail": (
                    f"Funding sigue negativo: {current_pct:.4f}% (media reciente: {avg_recent:.4f}%). "
                    f"Carry sigue siendo rentable."
                ),
            })

        # Determine overall action
        if any(s["severity"] == "CERRAR" for s in signals):
            action = "CERRAR_POSICION"
        elif any(s["severity"] == "PREPARAR_SALIDA" for s in signals):
            action = "PREPARAR_SALIDA"
        else:
            action = "MANTENER"

        return {
            "symbol": symbol,
            "current_funding_pct": round(current_pct, 4),
            "avg_recent_funding_pct": round(avg_recent, 4),
            "entry_funding_pct": entry_funding_pct,
            "action": action,
            "signals": signals,
            "funding_history": [
                {"time": f["funding_time"], "rate_pct": round(f["funding_rate"] * 100, 4)}
                for f in funding_data
            ],
        }
