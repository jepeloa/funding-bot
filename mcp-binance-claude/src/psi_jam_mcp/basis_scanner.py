"""
Basis Scanner — Multi-pair Spot-Futures Arbitrage Opportunity Finder
====================================================================
Scans 50+ USDT-M perpetual pairs for basis trading opportunities.

Two-phase approach (mirrors scanner.py pattern):
  Phase 1 (fast): Single API calls for all spot prices + futures tickers
                  Pre-filter by volume, compute raw basis for all pairs
  Phase 2 (deep): For top candidates, fetch funding history and spread data
                  Score with the full basis engine

Returns ranked opportunities with strategy recommendations.
"""

import asyncio
import time
from typing import Optional

from . import basis_engine as engine


# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────

# Symbols to exclude (stablecoins, delisted, illiquid)
EXCLUDED_BASES = {
    "USDC", "BUSD", "TUSD", "FDUSD", "DAI", "USDP",
    "EUR", "GBP", "BRL", "TRY", "ARS",
}

DEFAULT_TOP_N = 20
DEFAULT_MIN_VOLUME = 1_000_000       # $1M 24h volume minimum
DEFAULT_MIN_BASIS_PCT = 0.05         # 0.05% minimum basis to consider
MAX_SANE_BASIS_PCT = 15.0            # Discard pairs with basis > ±15% (likely delisted/stale)
MAX_CONCURRENT = 10                  # Max parallel API calls in phase 2
DEFAULT_FUNDING_LIMIT = 30           # Funding rate history entries to fetch


# ─────────────────────────────────────────────
# SCANNER
# ─────────────────────────────────────────────

class BasisScanner:
    """
    Two-phase basis opportunity scanner.
    Phase 1: Fast screening (2 API calls: all spot prices + all futures tickers)
    Phase 2: Deep analysis with funding + spreads for top candidates
    """

    def __init__(self, futures_client, spot_client):
        """
        Args:
            futures_client: BinanceClient instance (for futures data)
            spot_client: SpotClient instance (for spot prices)
        """
        self.futures = futures_client
        self.spot = spot_client

    async def scan(
        self,
        top_n: int = DEFAULT_TOP_N,
        min_volume: float = DEFAULT_MIN_VOLUME,
        min_basis_pct: float = DEFAULT_MIN_BASIS_PCT,
        include_btc: bool = True,
        max_candidates: int = 50,
        use_bnb: bool = False,
        use_maker: bool = False,
        funding_limit: int = DEFAULT_FUNDING_LIMIT,
    ) -> dict:
        """
        Full basis scan pipeline.

        Phase 1: Get all spot prices + futures tickers in 2 API calls,
                 compute raw basis for each pair, filter by volume & basis.
        Phase 2: For top candidates, fetch funding history in parallel,
                 compute full scoring.

        Returns:
            Dict with ranked opportunities, scan metadata, timing.
        """
        t_start = time.time()

        # ── PHASE 1: Fast screening ──
        phase1 = await self._phase1_screening(
            min_volume=min_volume,
            min_basis_pct=min_basis_pct,
            include_btc=include_btc,
        )

        t_phase1 = time.time()

        if not phase1["candidates"]:
            return {
                "scan_timestamp": int(time.time() * 1000),
                "mode": "full_basis_scan",
                "phase1_screened": phase1["total_screened"],
                "phase1_matched": 0,
                "opportunities": [],
                "timing": {
                    "phase1_sec": round(t_phase1 - t_start, 2),
                    "total_sec": round(t_phase1 - t_start, 2),
                },
            }

        # ── PHASE 2: Deep analysis on top candidates ──
        candidates = phase1["candidates"][:max_candidates]
        opportunities = await self._phase2_deep_analysis(
            candidates=candidates,
            use_bnb=use_bnb,
            use_maker=use_maker,
            funding_limit=funding_limit,
        )

        t_phase2 = time.time()

        # Sort by score descending
        opportunities.sort(key=lambda x: x.get("score", {}).get("total_score", 0), reverse=True)

        # Top N
        top = opportunities[:top_n]
        for i, opp in enumerate(top, 1):
            opp["rank"] = i

        return {
            "scan_timestamp": int(time.time() * 1000),
            "mode": "full_basis_scan",
            "phase1_screened": phase1["total_screened"],
            "phase1_matched": len(phase1["candidates"]),
            "phase2_analyzed": len(candidates),
            "opportunities_found": len([o for o in opportunities if o.get("score", {}).get("total_score", 0) > 20]),
            "count": len(top),
            "opportunities": top,
            "fee_config": engine.get_fee_schedule(use_bnb, use_maker),
            "timing": {
                "phase1_sec": round(t_phase1 - t_start, 2),
                "phase2_sec": round(t_phase2 - t_phase1, 2),
                "total_sec": round(t_phase2 - t_start, 2),
            },
        }

    async def quick_scan(
        self,
        top_n: int = 40,
        min_volume: float = 500_000,
        include_btc: bool = True,
    ) -> dict:
        """
        Quick scan — Phase 1 only (no funding analysis).
        Returns all pairs sorted by absolute basis in ~1 second.
        """
        t_start = time.time()

        phase1 = await self._phase1_screening(
            min_volume=min_volume,
            min_basis_pct=0,
            include_btc=include_btc,
        )

        candidates = phase1["candidates"][:top_n]
        for i, c in enumerate(candidates, 1):
            c["rank"] = i

        return {
            "scan_timestamp": int(time.time() * 1000),
            "mode": "quick_basis_scan (no funding analysis)",
            "total_screened": phase1["total_screened"],
            "count": len(candidates),
            "opportunities": candidates,
            "timing": {"total_sec": round(time.time() - t_start, 2)},
        }

    async def get_basis(self, symbol: str) -> dict:
        """
        Get current basis for a specific symbol.
        Fetches both spot and futures price, computes basis + funding.
        """
        symbol = symbol.upper()
        if not symbol.endswith("USDT"):
            symbol += "USDT"

        try:
            # Parallel fetch: spot price + futures ticker + funding
            spot_task = self.spot.get_price(symbol)
            futures_task = self.futures.get_ticker_24h(symbol)
            funding_task = self.futures.get_funding_rate(symbol, limit=30)

            spot_data, futures_data, funding_data = await asyncio.gather(
                spot_task, futures_task, funding_task,
                return_exceptions=True,
            )

            if isinstance(spot_data, Exception):
                return {"error": f"Failed to get spot price: {spot_data}", "symbol": symbol}
            if isinstance(futures_data, Exception):
                return {"error": f"Failed to get futures data: {futures_data}", "symbol": symbol}

            spot_price = spot_data["price"]
            futures_price = futures_data["last_price"]

            # Basis calculation
            basis = engine.calculate_basis(spot_price, futures_price)

            # Fee-adjusted profit
            profit = engine.calculate_fee_adjusted_profit(basis["basis_pct"])

            # Funding rates list for scoring
            funding_rates = []
            latest_funding = 0.0
            if not isinstance(funding_data, Exception) and funding_data:
                funding_rates = [f["funding_rate"] for f in funding_data]
                latest_funding = funding_rates[-1] if funding_rates else 0

            # Funding carry analysis
            funding = {}
            if funding_rates:
                funding = engine.calculate_funding_carry(funding_data, holding_hours=24)

            # Score
            score = engine.score_opportunity(
                basis_pct=basis["basis_pct"],
                funding_rate=latest_funding,
                funding_rates_history=funding_rates,
                volume_24h_usdt=futures_data.get("quote_volume", 0),
            )

            return {
                "symbol": symbol,
                "spot_price": spot_price,
                "futures_price": futures_price,
                "basis": basis,
                "funding": funding,
                "profit_analysis": profit,
                "score": score,
                "futures_volume_24h": futures_data.get("quote_volume", 0),
                "timestamp": int(time.time() * 1000),
            }

        except Exception as e:
            return {"error": str(e), "symbol": symbol}

    # ─────────────────────────────────────────────
    # PHASE 1: Fast screening
    # ─────────────────────────────────────────────

    async def _phase1_screening(
        self,
        min_volume: float = DEFAULT_MIN_VOLUME,
        min_basis_pct: float = DEFAULT_MIN_BASIS_PCT,
        include_btc: bool = True,
    ) -> dict:
        """
        Phase 1: Two API calls to get all spot prices + all futures tickers.
        Compute basis for matching pairs, filter by volume and basis threshold.
        """
        # Parallel: get all spot prices + all futures tickers + futures exchange info
        spot_prices_task = self.spot.get_all_prices()
        futures_tickers_task = self.futures.client.get("/fapi/v1/ticker/24hr")
        futures_info_task = self.futures.client.get("/fapi/v1/exchangeInfo")

        spot_prices_raw, futures_resp, info_resp = await asyncio.gather(
            spot_prices_task, futures_tickers_task, futures_info_task
        )

        futures_resp.raise_for_status()
        info_resp.raise_for_status()

        futures_tickers = futures_resp.json()
        futures_info = info_resp.json()

        # Build set of USDT-M perpetual symbols
        perpetual_symbols = {
            s["symbol"]
            for s in futures_info.get("symbols", [])
            if s.get("contractType") == "PERPETUAL"
            and s.get("quoteAsset") == "USDT"
            and s.get("status") == "TRADING"
        }

        # Build spot price lookup
        spot_lookup = {}
        for sp in spot_prices_raw:
            sym = sp["symbol"]
            if sym.endswith("USDT"):
                spot_lookup[sym] = sp["price"]

        # Build futures data lookup
        futures_lookup = {}
        for t in futures_tickers:
            sym = t.get("symbol", "")
            if sym in perpetual_symbols:
                try:
                    futures_lookup[sym] = {
                        "price": float(t.get("lastPrice", 0)),
                        "change_pct": float(t.get("priceChangePercent", 0)),
                        "volume": float(t.get("quoteVolume", 0)),
                        "high": float(t.get("highPrice", 0)),
                        "low": float(t.get("lowPrice", 0)),
                    }
                except (ValueError, TypeError):
                    continue

        # Match pairs and compute basis
        candidates = []
        total_screened = 0

        for symbol in perpetual_symbols:
            if symbol not in spot_lookup:
                continue
            if symbol not in futures_lookup:
                continue

            # Extract base asset
            base = symbol.replace("USDT", "")
            if base in EXCLUDED_BASES:
                continue
            if not include_btc and base == "BTC":
                continue

            total_screened += 1

            spot_price = spot_lookup[symbol]
            fd = futures_lookup[symbol]
            futures_price = fd["price"]

            if spot_price <= 0 or futures_price <= 0:
                continue

            # Volume filter
            if fd["volume"] < min_volume:
                continue

            # Calculate basis
            basis = engine.calculate_basis(spot_price, futures_price)
            abs_basis = abs(basis["basis_pct"])

            # Basis filter
            if abs_basis < min_basis_pct:
                continue

            # Sanity filter: discard pairs with absurd basis (delisted/stale spot prices)
            if abs_basis > MAX_SANE_BASIS_PCT:
                continue

            candidates.append({
                "symbol": symbol,
                "spot_price": spot_price,
                "futures_price": futures_price,
                "basis_pct": basis["basis_pct"],
                "basis_abs_pct": abs_basis,
                "basis_annualized_pct": basis["basis_annualized_pct"],
                "regime": basis["regime"],
                "direction": basis["direction"],
                "volume_24h_usdt": fd["volume"],
                "change_24h_pct": fd["change_pct"],
            })

        # Sort by absolute basis descending
        candidates.sort(key=lambda x: x["basis_abs_pct"], reverse=True)

        return {
            "total_screened": total_screened,
            "candidates": candidates,
        }

    # ─────────────────────────────────────────────
    # PHASE 2: Deep analysis
    # ─────────────────────────────────────────────

    async def _phase2_deep_analysis(
        self,
        candidates: list[dict],
        use_bnb: bool = False,
        use_maker: bool = False,
        funding_limit: int = DEFAULT_FUNDING_LIMIT,
    ) -> list[dict]:
        """
        Phase 2: For each candidate, fetch funding rate history
        and compute full opportunity score.
        """
        semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        results = []

        async def _analyze_one(candidate: dict) -> dict:
            async with semaphore:
                symbol = candidate["symbol"]
                try:
                    # Fetch funding rate history
                    funding_data = await self.futures.get_funding_rate(
                        symbol, limit=funding_limit
                    )

                    funding_rates = [f["funding_rate"] for f in funding_data]
                    latest_funding = funding_rates[-1] if funding_rates else 0

                    # Funding carry analysis
                    funding_carry = engine.calculate_funding_carry(
                        funding_data, holding_hours=24
                    )

                    # Full scoring
                    score = engine.score_opportunity(
                        basis_pct=candidate["basis_pct"],
                        funding_rate=latest_funding,
                        funding_rates_history=funding_rates,
                        volume_24h_usdt=candidate["volume_24h_usdt"],
                        use_bnb=use_bnb,
                        use_maker=use_maker,
                    )

                    # Fee-adjusted profit
                    profit = engine.calculate_fee_adjusted_profit(
                        candidate["basis_pct"],
                        use_bnb=use_bnb,
                        use_maker=use_maker,
                    )

                    return {
                        **candidate,
                        "funding": {
                            "latest_rate_pct": funding_carry.get("latest_rate_pct", 0),
                            "avg_rate_pct": funding_carry.get("avg_rate_pct", 0),
                            "annualized_pct": funding_carry.get("annualized_pct", 0),
                            "positive_cycles_pct": funding_carry.get("positive_cycles_pct", 0),
                            "short_receives": funding_carry.get("short_receives_funding", False),
                        },
                        "profit_analysis": profit,
                        "score": score,
                    }

                except Exception as e:
                    return {
                        **candidate,
                        "funding": {"error": str(e)},
                        "score": {"total_score": 0, "error": str(e)},
                    }

        tasks = [_analyze_one(c) for c in candidates]
        results = await asyncio.gather(*tasks)
        return list(results)

    # ─────────────────────────────────────────────
    # BASIS HISTORY (for a specific symbol)
    # ─────────────────────────────────────────────

    async def get_basis_history(
        self,
        symbol: str,
        interval: str = "1h",
        limit: int = 24,
    ) -> dict:
        """
        Get historical basis by comparing spot and futures klines.
        Uses kline close prices to reconstruct basis over time.

        Note: Spot klines are NOT available on fapi — we use api.binance.com.
        """
        symbol = symbol.upper()
        if not symbol.endswith("USDT"):
            symbol += "USDT"

        try:
            # Fetch spot and futures klines in parallel
            spot_klines_task = self.spot.client.get(
                "/api/v3/klines",
                params={"symbol": symbol, "interval": interval, "limit": limit}
            )
            futures_klines_task = self.futures.get_klines(
                symbol=symbol, interval=interval, limit=limit
            )

            spot_resp, futures_klines = await asyncio.gather(
                spot_klines_task, futures_klines_task
            )
            spot_resp.raise_for_status()
            spot_raw = spot_resp.json()

            # Parse spot klines
            spot_klines = []
            for k in spot_raw:
                spot_klines.append({
                    "open_time": int(k[0]),
                    "close": float(k[4]),
                    "volume": float(k[5]),
                })

            # Match by open_time and compute basis series
            futures_by_time = {k["open_time"]: k for k in futures_klines}
            basis_series = []

            for sk in spot_klines:
                t = sk["open_time"]
                if t in futures_by_time:
                    fk = futures_by_time[t]
                    sp = sk["close"]
                    fp = fk["close"]
                    if sp > 0:
                        basis_pct = ((fp - sp) / sp) * 100
                        basis_series.append({
                            "time": t,
                            "spot_close": sp,
                            "futures_close": fp,
                            "basis_pct": round(basis_pct, 4),
                            "basis_annualized_pct": round(basis_pct * 365 * 3, 2),
                        })

            # Stats
            if basis_series:
                basis_values = [b["basis_pct"] for b in basis_series]
                avg_basis = sum(basis_values) / len(basis_values)
                max_basis = max(basis_values)
                min_basis = min(basis_values)
                current = basis_values[-1]
                std_basis = (sum((b - avg_basis) ** 2 for b in basis_values) / len(basis_values)) ** 0.5
            else:
                avg_basis = max_basis = min_basis = current = std_basis = 0

            return {
                "symbol": symbol,
                "interval": interval,
                "data_points": len(basis_series),
                "stats": {
                    "current_basis_pct": round(current, 4),
                    "avg_basis_pct": round(avg_basis, 4),
                    "max_basis_pct": round(max_basis, 4),
                    "min_basis_pct": round(min_basis, 4),
                    "std_basis_pct": round(std_basis, 4),
                    "current_vs_avg": round(current - avg_basis, 4),
                    "z_score": round((current - avg_basis) / std_basis, 2) if std_basis > 0 else 0,
                },
                "series": basis_series,
                "timestamp": int(time.time() * 1000),
            }

        except Exception as e:
            return {"error": str(e), "symbol": symbol}
