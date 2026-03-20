"""
Altcoin Scanner Module
======================
Scans all USDT-M perpetual futures on Binance, applies JAM regime analysis
and 24h pump detection to rank altcoins by impulse strength.

Returns a ranked top-N list with:
- 24h price change & volume
- JAM regime classification (A/B/Neutral)
- Langevin parameters (F_ext, γ, κ)
- Composite pump score

Design: two-phase scan
  Phase 1 (fast): Fetch all 24h tickers in a single API call, pre-filter
                  by price change and volume thresholds.
  Phase 2 (deep): For top candidates, fetch klines and run JAM analysis
                  concurrently with bounded parallelism.
"""

import asyncio
import time
import numpy as np
from typing import Optional
from . import analysis
from . import technical_analysis as ta


# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────

# Symbols to exclude from altcoin scan (stablecoins, BTC itself, etc.)
EXCLUDED_BASES = {
    "USDC", "BUSD", "TUSD", "FDUSD", "DAI", "USDP",
    "EUR", "GBP", "BRL", "TRY", "ARS",
}

DEFAULT_TOP_N = 40
DEFAULT_MIN_CHANGE_PCT = 2.0   # Minimum 24h change to consider
DEFAULT_MIN_QUOTE_VOL = 5_000_000  # Minimum 24h quote volume (USDT)
MAX_CONCURRENT_KLINES = 10    # Max parallel kline fetches
KLINE_LIMIT = 100             # Candles for JAM (1h → ~4 days)
KLINE_INTERVAL = "1h"

# Opportunity filter defaults
DEFAULT_SIGMA_THRESHOLD = 2.0   # Only excursions > mean + N*sigma
DEFAULT_MIN_JAM_SCORE = 0.60    # Minimum JAM pump_score (60%)


# ─────────────────────────────────────────────
# SCANNER
# ─────────────────────────────────────────────

class AltcoinScanner:
    """
    Two-phase altcoin scanner:
    Phase 1 — Fast ticker screening (single API call)
    Phase 2 — JAM regime analysis on pre-filtered candidates
    """

    def __init__(self, binance_client):
        self.client = binance_client

    async def scan(
        self,
        top_n: int = DEFAULT_TOP_N,
        min_change_pct: float = DEFAULT_MIN_CHANGE_PCT,
        min_quote_volume: float = DEFAULT_MIN_QUOTE_VOL,
        include_btc: bool = False,
        max_candidates: int = 80,
        kline_interval: str = KLINE_INTERVAL,
        kline_limit: int = KLINE_LIMIT,
        sigma_threshold: float = DEFAULT_SIGMA_THRESHOLD,
        min_jam_score: float = DEFAULT_MIN_JAM_SCORE,
    ) -> dict:
        """
        Full scan pipeline with opportunity filtering.

        Only returns pairs that meet BOTH criteria:
        1. Positive 24h excursion > mean + sigma_threshold * std (statistical outlier)
        2. JAM pump_score >= min_jam_score within current analysis window

        Args:
            top_n: Number of top altcoins to return
            min_change_pct: Minimum absolute 24h price change % (pre-filter)
            min_quote_volume: Minimum 24h quote volume in USDT
            include_btc: Include BTCUSDT in results
            max_candidates: Max symbols to run JAM on (controls API usage)
            kline_interval: Timeframe for JAM klines
            kline_limit: Number of candles to fetch per symbol
            sigma_threshold: Only keep positive excursions > mean + N*sigma (default 2.0)
            min_jam_score: Minimum JAM pump_score to qualify (default 0.60)

        Returns:
            Dict with ranked results, scan metadata, and timing info
        """
        t_start = time.time()

        # ── PHASE 1: Fast ticker screening + 2σ filter ──
        phase1_result = await self._phase1_ticker_screen(
            min_change_pct=min_change_pct,
            min_quote_volume=min_quote_volume,
            include_btc=include_btc,
            sigma_threshold=sigma_threshold,
        )
        candidates = phase1_result["candidates"][:max_candidates]
        t_phase1 = time.time()

        # ── PHASE 2: JAM analysis on candidates ──
        ranked = await self._phase2_jam_analysis(
            candidates=candidates,
            kline_interval=kline_interval,
            kline_limit=kline_limit,
        )
        t_phase2 = time.time()

        # ── PHASE 3: Opportunity filter — only pump_score >= min_jam_score ──
        pre_filter_count = len(ranked)
        ranked = [r for r in ranked if r.get("pump_score", 0) >= min_jam_score]

        # Sort by composite pump score (descending)
        ranked.sort(key=lambda x: x["pump_score"], reverse=True)
        top_results = ranked[:top_n]

        # Assign rank
        for i, r in enumerate(top_results, 1):
            r["rank"] = i

        return {
            "scan_timestamp": int(time.time() * 1000),
            "scan_config": {
                "top_n": top_n,
                "min_change_pct": min_change_pct,
                "min_quote_volume_usdt": min_quote_volume,
                "kline_interval": kline_interval,
                "kline_limit": kline_limit,
                "sigma_threshold": sigma_threshold,
                "min_jam_score": min_jam_score,
            },
            "scan_stats": {
                "total_symbols_screened": phase1_result["total_screened"],
                "phase1_candidates_pre_sigma": phase1_result["pre_sigma_count"],
                "phase1_candidates_post_sigma": len(phase1_result["candidates"]),
                "sigma_mean_pct": phase1_result["sigma_stats"]["mean_change_pct"],
                "sigma_std_pct": phase1_result["sigma_stats"]["std_change_pct"],
                "sigma_cutoff_pct": phase1_result["sigma_stats"]["cutoff_pct"],
                "phase2_analyzed": pre_filter_count,
                "phase3_jam_qualified": len(ranked),
                "returned": len(top_results),
                "phase1_time_sec": round(t_phase1 - t_start, 2),
                "phase2_time_sec": round(t_phase2 - t_phase1, 2),
                "total_time_sec": round(t_phase2 - t_start, 2),
            },
            "top_altcoins": top_results,
        }

    # ─────────────────────────────────────────────
    # PHASE 1: TICKER SCREENING
    # ─────────────────────────────────────────────

    async def _phase1_ticker_screen(
        self,
        min_change_pct: float,
        min_quote_volume: float,
        include_btc: bool,
        sigma_threshold: float = DEFAULT_SIGMA_THRESHOLD,
    ) -> dict:
        """
        Fetch all 24h tickers in a single API call and apply 2σ filter.

        Two-step filtering:
        1. Basic pre-filter (volume, excluded bases)
        2. Statistical σ filter: only keep POSITIVE excursions > mean + sigma_threshold * std

        Returns candidates sorted by pump intensity (descending).
        """
        # Get all tickers at once
        try:
            resp = await self.client.client.get("/fapi/v1/ticker/24hr")
            resp.raise_for_status()
            all_tickers = resp.json()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch tickers from Binance: {e}")

        # Get valid perpetual symbols
        try:
            exchange_info = await self.client.client.get("/fapi/v1/exchangeInfo")
            exchange_info.raise_for_status()
            perpetual_symbols = {
                s["symbol"]
                for s in exchange_info.json().get("symbols", [])
                if s.get("contractType") == "PERPETUAL"
                and s.get("quoteAsset") == "USDT"
                and s.get("status") == "TRADING"
            }
        except Exception as e:
            raise RuntimeError(f"Failed to fetch exchange info from Binance: {e}")

        # ── Step 1: Collect all valid tickers for σ computation ──
        all_parsed = []
        total = 0

        for t in all_tickers:
            sym = t.get("symbol", "")
            if sym not in perpetual_symbols:
                continue

            base = sym.replace("USDT", "")
            if base in EXCLUDED_BASES:
                continue
            if not include_btc and base == "BTC":
                continue

            total += 1

            try:
                change_pct = float(t.get("priceChangePercent", 0))
                quote_vol = float(t.get("quoteVolume", 0))
                last_price = float(t.get("lastPrice", 0))
                volume = float(t.get("volume", 0))
                high = float(t.get("highPrice", 0))
                low = float(t.get("lowPrice", 0))
                trades = int(t.get("count", 0))
            except (ValueError, TypeError):
                continue

            if quote_vol < min_quote_volume:
                continue

            all_parsed.append({
                "symbol": sym,
                "base": base,
                "change_pct": change_pct,
                "quote_vol": quote_vol,
                "last_price": last_price,
                "volume": volume,
                "high": high,
                "low": low,
                "trades": trades,
            })

        # ── Step 2: Compute σ threshold on ALL changes (robust) ──
        # Use ALL changes for distribution, then filter positive outliers.
        # This avoids inflated cutoffs from using only the positive tail.
        all_changes = [p["change_pct"] for p in all_parsed]
        all_positive_changes = [c for c in all_changes if c > 0]

        if len(all_changes) >= 10:
            # Robust: use median + MAD (less sensitive to extreme outliers)
            median_change = float(np.median(all_changes))
            mad = float(np.median(np.abs(np.array(all_changes) - median_change)))
            # Convert MAD to σ-equivalent (MAD ≈ 0.6745 × σ for normal)
            std_equiv = mad / 0.6745 if mad > 0 else float(np.std(all_changes))
            mean_change = float(np.mean(all_positive_changes)) if all_positive_changes else median_change
            std_change = std_equiv
        elif len(all_changes) >= 3:
            mean_change = float(np.mean(all_changes))
            std_change = float(np.std(all_changes))
        else:
            mean_change = 0.0
            std_change = 1.0

        sigma_cutoff = median_change + sigma_threshold * std_change if len(all_changes) >= 10 else mean_change + sigma_threshold * std_change
        # Ensure cutoff is at least min_change_pct to avoid passing noise
        sigma_cutoff = max(sigma_cutoff, min_change_pct)

        # ── Step 3: Apply σ filter — only large positive excursions ──
        pre_sigma_count = len(all_parsed)
        candidates = []

        for p in all_parsed:
            # Only positive excursions above σ cutoff
            if p["change_pct"] < sigma_cutoff:
                continue

            change_pct = p["change_pct"]
            quote_vol = p["quote_vol"]
            high = p["high"]
            low = p["low"]

            # Pump intensity = magnitude of move × volume weight
            vol_weight = min(quote_vol / 50_000_000, 3.0)  # Cap at 3x
            pump_intensity = abs(change_pct) * (1 + vol_weight * 0.3)

            # Intraday range as % of price
            range_pct = ((high - low) / low * 100) if low > 0 else 0

            candidates.append({
                "symbol": p["symbol"],
                "base": p["base"],
                "price": p["last_price"],
                "change_pct_24h": round(change_pct, 2),
                "abs_change_pct": round(abs(change_pct), 2),
                "sigma_above_mean": round((change_pct - mean_change) / (std_change + 1e-12), 2),
                "volume_24h": round(p["volume"], 2),
                "quote_volume_24h": round(quote_vol, 2),
                "high_24h": high,
                "low_24h": low,
                "range_pct_24h": round(range_pct, 2),
                "trades_24h": p["trades"],
                "pump_intensity": round(pump_intensity, 2),
            })

        # ── Step 4: Adaptive fallback ──
        # If sigma filter is too aggressive (< 5 candidates), relax to top positive movers
        MIN_CANDIDATES = 5
        if len(candidates) < MIN_CANDIDATES:
            # Fall back to top positive movers by change_pct (above min_change_pct)
            fallback_pool = [
                p for p in all_parsed
                if p["change_pct"] >= min_change_pct and p["symbol"] not in {c["symbol"] for c in candidates}
            ]
            fallback_pool.sort(key=lambda x: x["change_pct"], reverse=True)

            for p in fallback_pool[:MIN_CANDIDATES - len(candidates)]:
                change_pct = p["change_pct"]
                quote_vol = p["quote_vol"]
                high = p["high"]
                low = p["low"]
                vol_weight = min(quote_vol / 50_000_000, 3.0)
                pump_intensity = abs(change_pct) * (1 + vol_weight * 0.3)
                range_pct = ((high - low) / low * 100) if low > 0 else 0

                candidates.append({
                    "symbol": p["symbol"],
                    "base": p["base"],
                    "price": p["last_price"],
                    "change_pct_24h": round(change_pct, 2),
                    "abs_change_pct": round(abs(change_pct), 2),
                    "sigma_above_mean": round((change_pct - mean_change) / (std_change + 1e-12), 2),
                    "volume_24h": round(p["volume"], 2),
                    "quote_volume_24h": round(quote_vol, 2),
                    "high_24h": high,
                    "low_24h": low,
                    "range_pct_24h": round(range_pct, 2),
                    "trades_24h": p["trades"],
                    "pump_intensity": round(pump_intensity, 2),
                    "fallback": True,  # Flag: didn't pass sigma filter
                })

        # Sort by pump intensity (biggest movers first)
        candidates.sort(key=lambda x: x["pump_intensity"], reverse=True)

        return {
            "total_screened": total,
            "pre_sigma_count": pre_sigma_count,
            "sigma_stats": {
                "mean_change_pct": round(mean_change, 2),
                "std_change_pct": round(std_change, 2),
                "median_change_pct": round(median_change, 2) if len(all_changes) >= 10 else None,
                "sigma_threshold": sigma_threshold,
                "cutoff_pct": round(sigma_cutoff, 2),
                "positive_symbols_for_stats": len(all_positive_changes),
                "fallback_used": any(c.get("fallback") for c in candidates),
            },
            "candidates": candidates,
        }

    # ─────────────────────────────────────────────
    # PHASE 2: JAM ANALYSIS
    # ─────────────────────────────────────────────

    async def _phase2_jam_analysis(
        self,
        candidates: list[dict],
        kline_interval: str,
        kline_limit: int,
    ) -> list[dict]:
        """
        Run JAM regime analysis on pre-filtered candidates.
        Uses bounded concurrency to avoid API rate limits.
        """
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_KLINES)
        results = []

        async def analyze_one(candidate: dict) -> dict:
            async with semaphore:
                return await self._analyze_single(candidate, kline_interval, kline_limit)

        tasks = [analyze_one(c) for c in candidates]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out failures
        valid = []
        for r in results:
            if isinstance(r, Exception):
                continue
            if r is not None:
                valid.append(r)

        return valid

    async def _analyze_single(
        self,
        candidate: dict,
        kline_interval: str,
        kline_limit: int,
    ) -> Optional[dict]:
        """Analyze a single symbol: fetch klines + run JAM + short-signal metrics."""
        symbol = candidate["symbol"]

        try:
            klines = await self.client.get_klines(
                symbol=symbol,
                interval=kline_interval,
                limit=kline_limit,
            )

            if len(klines) < 40:  # Need minimum data for JAM
                return self._build_result(candidate, jam=None, short_metrics=None, reason="insufficient_klines")

            closes = np.array([k["close"] for k in klines])
            opens = np.array([k["open"] for k in klines])
            volumes = np.array([k["volume"] for k in klines])
            taker_buy_vols = np.array([k["taker_buy_volume"] for k in klines])
            highs = np.array([k["high"] for k in klines])
            lows = np.array([k["low"] for k in klines])

            # Run JAM regime analysis
            jam = analysis.jam_regime_analysis(
                closes, volumes, taker_buy_vols, highs, lows, window=20,
            )

            # ── Compute short-signal metrics ──
            short_metrics = self._compute_short_metrics(
                closes, opens, highs, lows, volumes, taker_buy_vols,
            )

            return self._build_result(candidate, jam=jam, short_metrics=short_metrics)

        except Exception:
            return self._build_result(candidate, jam=None, short_metrics=None, reason="fetch_error")

    def _compute_short_metrics(
        self,
        closes: np.ndarray,
        opens: np.ndarray,
        highs: np.ndarray,
        lows: np.ndarray,
        volumes: np.ndarray,
        taker_buy_vols: np.ndarray,
    ) -> dict:
        """
        Compute metrics specifically useful for short entry decisions:
        - Volatility (ATR%) current and change over time
        - Bollinger squeeze detection
        - RSI (overbought = reversal signal)
        - Distance from 24h high (already reversing?)
        - Volume decay (momentum fading)
        - Taker buy ratio trend (buyers losing dominance)
        - Upper wick rejection analysis
        - Composite short conviction score
        """
        n = len(closes)
        price = closes[-1]

        # ─── 1. VOLATILITY (ATR-based) ───
        atr_data = ta.compute_atr(highs, lows, closes, period=14)
        atr_pct = atr_data.get("atr_pct", 0) or 0
        vol_regime = atr_data.get("volatility_regime", "NORMAL")

        # ATR series for trend analysis
        atr_series = atr_data.get("atr_series", [])

        # Volatility change: compare last 6 candles vs prior 6 candles
        # (on 1h klines: last 6h vs prior 6h)
        if len(atr_series) >= 12:
            recent_atr = np.mean(atr_series[-6:])
            prior_atr = np.mean(atr_series[-12:-6])
            vol_change_pct = ((recent_atr - prior_atr) / (prior_atr + 1e-12)) * 100
        elif len(atr_series) >= 6:
            recent_atr = np.mean(atr_series[-3:])
            prior_atr = np.mean(atr_series[:-3])
            vol_change_pct = ((recent_atr - prior_atr) / (prior_atr + 1e-12)) * 100
        else:
            recent_atr = 0
            prior_atr = 0
            vol_change_pct = 0

        # Volatility compression flag (pre-drop signal)
        vol_compressing = bool(vol_change_pct < -15)  # >15% drop in volatility

        # ─── 2. BOLLINGER BANDS ───
        bb_data = ta.compute_bollinger(closes, period=20, std_dev=2.0)
        bb_position = bb_data.get("position", "MIDDLE")
        bb_pct_b = bb_data.get("percent_b", 0.5) or 0.5
        bb_squeeze = bb_data.get("squeeze", False)
        bb_bandwidth = bb_data.get("bandwidth", 0) or 0

        # ─── 3. RSI ───
        rsi_data = ta.compute_rsi(closes, period=14)
        rsi = rsi_data.get("rsi", 50) or 50
        rsi_zone = rsi_data.get("zone", "NEUTRAL")

        # RSI divergence: price making highs but RSI declining
        rsi_series = rsi_data.get("rsi_series", [])
        rsi_declining = False
        if len(rsi_series) >= 6:
            rsi_recent = np.mean(rsi_series[-3:])
            rsi_prior = np.mean(rsi_series[-6:-3])
            rsi_declining = bool(rsi_recent < rsi_prior and rsi_prior > 55)

        # ─── 4. DISTANCE FROM HIGH ───
        high_24h = np.max(highs[-24:]) if n >= 24 else np.max(highs)
        distance_from_high_pct = ((high_24h - price) / high_24h) * 100 if high_24h > 0 else 0

        # ─── 5. VOLUME DECAY ───
        # Compare last 3 candles volume vs peak 3 candles volume in window
        if n >= 12:
            recent_vol = np.mean(volumes[-3:])
            peak_vol = np.max([np.mean(volumes[i:i+3]) for i in range(max(0, n-12), n-2)])
            vol_decay_pct = ((recent_vol - peak_vol) / (peak_vol + 1e-12)) * 100
        else:
            vol_decay_pct = 0

        # ─── 6. TAKER BUY RATIO TREND ───
        # Are buyers losing dominance?
        taker_ratio = taker_buy_vols / (volumes + 1e-12)
        if n >= 6:
            taker_recent = float(np.mean(taker_ratio[-3:]))
            taker_prior = float(np.mean(taker_ratio[-6:-3]))
            taker_delta = taker_recent - taker_prior
            buyers_weakening = bool(taker_delta < -0.02)  # Buy ratio dropped >2%
        else:
            taker_recent = float(np.mean(taker_ratio[-3:])) if n >= 3 else float(taker_ratio[-1])
            taker_prior = taker_recent
            taker_delta = 0
            buyers_weakening = False

        # ─── 7. UPPER WICK REJECTION ───
        # Upper wick = high - max(open, close), as % of candle range
        # Long upper wicks = price rejection at highs (bearish)
        wick_window = min(3, n)
        candle_ranges = highs[-wick_window:] - lows[-wick_window:]
        body_tops = np.maximum(opens[-wick_window:], closes[-wick_window:])
        upper_wicks = highs[-wick_window:] - body_tops
        # Avoid div-by-zero for doji candles
        safe_ranges = np.where(candle_ranges > 0, candle_ranges, 1e-12)
        upper_wick_ratios = upper_wicks / safe_ranges
        avg_upper_wick_pct = float(np.mean(upper_wick_ratios)) * 100  # 0-100%
        # Count candles with significant upper wicks (>30% of range)
        wicks_with_rejection = int(np.sum(upper_wick_ratios > 0.30))
        strong_wicks = wicks_with_rejection >= 2  # 2+ of last 3 candles

        # ─── 8. SHORT CONVICTION SCORE ───
        # Each factor 0-1, weighted, higher = stronger short signal
        #   RSI overbought
        rsi_score = max(0, min((rsi - 50) / 30, 1.0))  # 50→0, 80→1
        #   Volume decay (negative = fading)
        decay_score = max(0, min(-vol_decay_pct / 50, 1.0))  # -50%→1
        #   Volatility compression (negative change = compressing)
        vol_compress_score = max(0, min(-vol_change_pct / 30, 1.0))  # -30%→1
        #   Distance from high (already reversing)
        dist_score = max(0, min(distance_from_high_pct / 10, 1.0))  # 10%→1
        #   Buyers weakening
        buyer_weak_score = max(0, min(-taker_delta * 10, 1.0))  # -0.1→1
        #   Bollinger overbought position
        bb_score = max(0, min((bb_pct_b - 0.5) * 2, 1.0))  # %B 1.0→1
        #   Upper wick rejection
        wick_score = min(avg_upper_wick_pct / 40, 1.0)  # 40% avg wick → 1.0

        short_conviction = (
            rsi_score * 0.18
            + decay_score * 0.18
            + vol_compress_score * 0.18
            + dist_score * 0.13
            + buyer_weak_score * 0.13
            + bb_score * 0.10
            + wick_score * 0.10
        )

        # Qualitative assessment
        if short_conviction >= 0.65:
            short_signal = "STRONG"
        elif short_conviction >= 0.45:
            short_signal = "MODERATE"
        elif short_conviction >= 0.25:
            short_signal = "WEAK"
        else:
            short_signal = "NONE"

        return {
            "volatility": {
                "atr_pct": round(float(atr_pct), 4),
                "regime": vol_regime,
                "recent_6h_atr": round(float(recent_atr), 6),
                "prior_6h_atr": round(float(prior_atr), 6),
                "vol_change_pct": round(float(vol_change_pct), 2),
                "compressing": vol_compressing,
            },
            "bollinger": {
                "position": bb_position,
                "pct_b": round(float(bb_pct_b), 4),
                "bandwidth": round(float(bb_bandwidth), 4),
                "squeeze": bb_squeeze,
            },
            "rsi": {
                "value": round(float(rsi), 2),
                "zone": rsi_zone,
                "declining": rsi_declining,
            },
            "price_action": {
                "distance_from_high_pct": round(float(distance_from_high_pct), 2),
                "high_24h": round(float(high_24h), 6),
            },
            "momentum_decay": {
                "volume_decay_pct": round(float(vol_decay_pct), 2),
                "taker_buy_ratio_recent": round(float(taker_recent), 4),
                "taker_buy_ratio_prior": round(float(taker_prior), 4),
                "taker_delta": round(float(taker_delta), 4),
                "buyers_weakening": buyers_weakening,
            },
            "upper_wicks": {
                "avg_wick_pct": round(float(avg_upper_wick_pct), 2),
                "rejection_candles": wicks_with_rejection,
                "strong_wicks": strong_wicks,
            },
            "short_conviction": {
                "score": round(float(short_conviction), 4),
                "signal": short_signal,
                "components": {
                    "rsi_score": round(float(rsi_score), 3),
                    "vol_decay_score": round(float(decay_score), 3),
                    "vol_compress_score": round(float(vol_compress_score), 3),
                    "dist_from_high_score": round(float(dist_score), 3),
                    "buyer_weak_score": round(float(buyer_weak_score), 3),
                    "bb_score": round(float(bb_score), 3),
                    "wick_score": round(float(wick_score), 3),
                },
            },
        }

    def _build_result(
        self,
        candidate: dict,
        jam: Optional[dict],
        short_metrics: Optional[dict] = None,
        reason: Optional[str] = None,
    ) -> dict:
        """Build final ranked result combining ticker data + JAM + short-signal analysis."""
        result = {
            "symbol": candidate["symbol"],
            "price": candidate["price"],
            "change_pct_24h": candidate["change_pct_24h"],
            "quote_volume_24h": candidate["quote_volume_24h"],
            "range_pct_24h": candidate["range_pct_24h"],
            "trades_24h": candidate["trades_24h"],
        }

        if jam and "error" not in jam:
            regime = jam.get("regime", "NEUTRAL")
            state = jam.get("current_state", {})
            langevin = jam.get("langevin_params", {})

            # ─── Pump Score Calculation ───
            # Components (0-1 scale each):
            #   1. Price change magnitude (capped at 30%)
            change_score = min(abs(candidate["change_pct_24h"]) / 30, 1.0)

            #   2. Volume surge (vol_ratio, capped at 5x)
            vol_ratio = state.get("vol_ratio_mean_last5", 1.0)
            vol_score = min(vol_ratio / 5.0, 1.0)

            #   3. Delta directionality (how far from 0.5)
            delta = state.get("delta_mean_last5", 0.5)
            delta_score = min(abs(delta - 0.5) * 4, 1.0)

            #   4. Retention (direct)
            retention_score = max(0, state.get("retention", 0))

            #   5. Regime bonus: A = 1.0, B = 0.4, Neutral = 0.2
            regime_bonus = {"A": 1.0, "B": 0.4}.get(regime, 0.2)

            #   6. F_ext (external force, capped at 3)
            f_ext = langevin.get("F_ext_force", 0)
            f_ext_score = min(f_ext / 3.0, 1.0)

            # Weighted composite
            pump_score = (
                change_score * 0.20
                + vol_score * 0.20
                + delta_score * 0.15
                + retention_score * 0.15
                + regime_bonus * 0.15
                + f_ext_score * 0.15
            )

            # Direction: positive change = bullish pump, negative = dump
            direction = "PUMP" if candidate["change_pct_24h"] > 0 else "DUMP"

            result.update({
                "pump_score": round(pump_score, 4),
                "direction": direction,
                "jam_regime": regime,
                "jam_description": jam.get("regime_description", ""),
                "jam_state": {
                    "vol_ratio": state.get("vol_ratio_mean_last5"),
                    "delta": state.get("delta_mean_last5"),
                    "retention": state.get("retention"),
                    "absorption": state.get("absorption"),
                },
                "langevin": {
                    "F_ext": langevin.get("F_ext_force"),
                    "gamma_damping": langevin.get("gamma_eff_damping"),
                    "kappa_restoring": langevin.get("kappa_eff_restoring"),
                },
                "pump_components": {
                    "change_score": round(change_score, 3),
                    "vol_score": round(vol_score, 3),
                    "delta_score": round(delta_score, 3),
                    "retention_score": round(retention_score, 3),
                    "regime_bonus": round(regime_bonus, 3),
                    "f_ext_score": round(f_ext_score, 3),
                },
            })
        else:
            # No JAM data — score based on ticker only
            change_score = min(abs(candidate["change_pct_24h"]) / 30, 1.0)
            direction = "PUMP" if candidate["change_pct_24h"] > 0 else "DUMP"
            result.update({
                "pump_score": round(change_score * 0.5, 4),  # Penalized score
                "direction": direction,
                "jam_regime": "UNKNOWN",
                "jam_description": reason or "JAM analysis unavailable",
                "jam_state": None,
                "langevin": None,
                "pump_components": {"change_score_only": round(change_score, 3)},
            })

        # ── Short-signal metrics ──
        if short_metrics:
            result["short_analysis"] = short_metrics
        else:
            result["short_analysis"] = None

        return result
