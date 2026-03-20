"""
GPU-accelerated liquidation level estimation engine.

Pipeline:
  1. Query ohlcv_1m (360 rows/6h) + mark_price + open_interest
  2. Build volume profile on GPU (price × time matrix)
  3. Estimate liquidation prices per leverage tier (scatter-add on GPU)
  4. Subtract realized liquidations
  5. Return liquidation density arrays for rendering

Falls back to NumPy on CPU if CuPy is unavailable.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Optional

import asyncpg
import numpy as np

from .config import (
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD,
    DB_POOL_MIN, DB_POOL_MAX,
    LOOKBACK_HOURS, PRICE_RANGE_PCT, PRICE_BINS, TIME_BINS,
    LEVERAGE_TIERS, MAINTENANCE_MARGIN, GPU_DEVICES, FORCE_CPU,
)

log = logging.getLogger("heatmap.engine")

# ── GPU / CPU backend selection ──────────────────────────────────

_use_gpu = False
xp = np  # array module — numpy or cupy

if not FORCE_CPU:
    try:
        import cupy as cp
        _n_gpus = cp.cuda.runtime.getDeviceCount()
        if _n_gpus > 0:
            xp = cp
            _use_gpu = True
            log.info("CuPy available — %d GPU(s) detected", _n_gpus)
        else:
            log.warning("CuPy found but no GPUs detected — falling back to CPU")
    except ImportError:
        log.warning("CuPy not installed — running on CPU (NumPy)")
    except Exception as exc:
        log.warning("CuPy init failed (%s) — falling back to CPU", exc)


def _to_numpy(arr) -> np.ndarray:
    """Convert CuPy array to NumPy (no-op if already NumPy)."""
    if _use_gpu and hasattr(arr, "get"):
        return arr.get()
    return np.asarray(arr)


# ══════════════════════════════════════════════════════════════════
#  DATA CLASSES
# ══════════════════════════════════════════════════════════════════

@dataclass
class HeatmapData:
    """Output of the engine — everything the renderer needs."""
    symbol: str
    # Price axis
    price_min: float
    price_max: float
    price_bins: int
    mark_price: float
    # Liquidation density (NumPy)
    liq_long: np.ndarray     # shape (PRICE_BINS,) — liq of longs (below price)
    liq_short: np.ndarray    # shape (PRICE_BINS,) — liq of shorts (above price)
    # 2D heatmap: time × price (for full heatmap rendering)
    heatmap_long: np.ndarray   # shape (TIME_BINS, PRICE_BINS)
    heatmap_short: np.ndarray  # shape (TIME_BINS, PRICE_BINS)
    # OHLC arrays for candlestick overlay
    opens: np.ndarray          # shape (n_candles,)
    highs_arr: np.ndarray      # shape (n_candles,)
    lows_arr: np.ndarray       # shape (n_candles,)
    closes: np.ndarray         # shape (n_candles,)
    timestamps: np.ndarray     # shape (n_candles,)  epoch seconds
    volumes: np.ndarray        # shape (n_candles,)  USDT volume per candle
    # Time axis
    time_start: float        # epoch
    time_end: float          # epoch
    time_bins: int
    # Metadata
    oi_value: float
    lookback_hours: int
    computed_at: float
    compute_time_ms: float


# ══════════════════════════════════════════════════════════════════
#  ENGINE
# ══════════════════════════════════════════════════════════════════

class LiquidationHeatmapEngine:
    """Generates liquidation heatmaps using GPU-accelerated computation."""

    def __init__(self):
        self._pool: Optional[asyncpg.Pool] = None
        self._gpu_counter = 0  # round-robin across GPUs

    async def start(self):
        self._pool = await asyncpg.create_pool(
            host=DB_HOST, port=DB_PORT, database=DB_NAME,
            user=DB_USER, password=DB_PASSWORD,
            min_size=DB_POOL_MIN, max_size=DB_POOL_MAX,
            command_timeout=60,
        )
        log.info("Engine started — pool=%d-%d, gpu=%s",
                 DB_POOL_MIN, DB_POOL_MAX,
                 f"{len(GPU_DEVICES)} GPU(s)" if _use_gpu else "CPU")

    async def stop(self):
        if self._pool:
            await self._pool.close()

    # ── Main entry point ─────────────────────────────────────────

    async def generate(self, symbol: str,
                       hours: int = LOOKBACK_HOURS) -> HeatmapData:
        """Generate liquidation heatmap data for a symbol."""
        t0 = time.perf_counter()
        symbol = symbol.upper()

        # Select GPU (round-robin)
        gpu_id = self._pick_gpu()

        # Phase 1 — Parallel DB queries
        ohlcv, mark_price, oi_value, realized_liqs = await self._fetch_data(
            symbol, hours
        )

        if len(ohlcv) == 0:
            raise ValueError(f"No OHLCV data for {symbol} in last {hours}h")

        # Phase 2+3 — GPU computation
        data = await asyncio.to_thread(
            self._compute_on_device, gpu_id, symbol,
            ohlcv, mark_price, oi_value, realized_liqs, hours,
        )

        elapsed_ms = (time.perf_counter() - t0) * 1000
        data.compute_time_ms = elapsed_ms
        log.info("%s heatmap generated in %.1fms (gpu=%s, ohlcv=%d rows, oi=%.0f)",
                 symbol, elapsed_ms,
                 f"GPU:{gpu_id}" if _use_gpu else "CPU",
                 len(ohlcv), oi_value)
        return data

    async def get_symbols(self) -> list[str]:
        """Return all symbols that have OHLCV data."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT DISTINCT symbol FROM ohlcv_1m "
                "WHERE bucket >= now() - interval '6 hours' "
                "ORDER BY symbol"
            )
        return [r["symbol"] for r in rows]

    async def get_summary(self, top_n: int = 20) -> list[dict]:
        """Return top N symbols by estimated liquidation density."""
        symbols = await self.get_symbols()
        results = []

        # Process in small batches to avoid overwhelming GPUs
        batch_size = 10
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            tasks = [self._safe_generate(s) for s in batch]
            batch_results = await asyncio.gather(*tasks)
            for sym, res in zip(batch, batch_results):
                if res is not None:
                    total_liq = float(np.sum(res.liq_long) + np.sum(res.liq_short))
                    results.append({
                        "symbol": sym,
                        "total_liq_density": round(total_liq, 2),
                        "mark_price": res.mark_price,
                        "oi_value": res.oi_value,
                        "compute_ms": round(res.compute_time_ms, 1),
                    })

        results.sort(key=lambda x: x["total_liq_density"], reverse=True)
        return results[:top_n]

    # ── Private — Data fetching ──────────────────────────────────

    async def _fetch_data(self, symbol: str, hours: int):
        """Fetch all needed data in parallel queries.

        Each query runs on its own connection (asyncpg forbids concurrent
        operations on a single connection).
        """

        async def _q_ohlcv():
            async with self._pool.acquire() as c:
                return await c.fetch(
                    "SELECT bucket, open, high, low, close, "
                    "volume_usdt, taker_buy_volume, taker_sell_volume "
                    "FROM ohlcv_1m "
                    "WHERE symbol = $1 AND bucket >= now() - make_interval(hours => $2) "
                    "ORDER BY bucket ASC",
                    symbol, hours,
                )

        async def _q_mark():
            async with self._pool.acquire() as c:
                return await c.fetchrow(
                    "SELECT mark_price FROM mark_prices "
                    "WHERE symbol = $1 ORDER BY event_time DESC LIMIT 1",
                    symbol,
                )

        async def _q_oi():
            async with self._pool.acquire() as c:
                return await c.fetchrow(
                    "SELECT oi_value FROM open_interest "
                    "WHERE symbol = $1 ORDER BY polled_at DESC LIMIT 1",
                    symbol,
                )

        async def _q_liqs():
            async with self._pool.acquire() as c:
                return await c.fetch(
                    "SELECT side, price, avg_price, filled_qty "
                    "FROM liquidations "
                    "WHERE symbol = $1 "
                    "AND event_time >= now() - make_interval(hours => $2)",
                    symbol, hours,
                )

        ohlcv, mark_row, oi_row, liqs = await asyncio.gather(
            _q_ohlcv(), _q_mark(), _q_oi(), _q_liqs()
        )

        mark_price = float(mark_row["mark_price"]) if mark_row else 0.0
        oi_value = float(oi_row["oi_value"]) if oi_row else 0.0

        return ohlcv, mark_price, oi_value, liqs

    # ── Private — GPU/CPU computation ────────────────────────────

    def _compute_on_device(self, gpu_id: int, symbol: str,
                           ohlcv: list, mark_price: float,
                           oi_value: float, realized_liqs: list,
                           hours: int) -> HeatmapData:
        """Run the full computation pipeline on the selected device."""

        # Set GPU context if using CuPy
        if _use_gpu:
            import cupy as cp
            device = cp.cuda.Device(gpu_id)
            device.use()

        # ── Price axis ───────────────────────────────────────────
        if mark_price <= 0:
            # Fallback: use last close from OHLCV
            mark_price = float(ohlcv[-1]["close"]) if ohlcv else 1.0

        # ── Parse OHLCV into arrays (guard NULLs) ───────────────
        n_candles = len(ohlcv)
        if n_candles == 0:
            raise ValueError(f"No OHLCV data for {symbol}")

        highs = np.array([float(r["high"] or 0) for r in ohlcv])
        lows = np.array([float(r["low"] or 0) for r in ohlcv])
        volumes = np.array([float(r["volume_usdt"] or 0) for r in ohlcv])
        buy_vols = np.array([float(r["taker_buy_volume"] or 0) for r in ohlcv])
        sell_vols = np.array([float(r["taker_sell_volume"] or 0) for r in ohlcv])
        total_vols = buy_vols + sell_vols

        # Compute price range from actual data:
        # Moderately tight: candle range + ~80% padding so the
        # nearby liquidation bands (1 leverage tier away) are visible
        # but the chart is not drowned in empty space.
        data_high = float(highs.max())
        data_low = float(lows.min())
        data_range = max(data_high - data_low, data_high * 0.005)  # min 0.5%
        mid_price = (data_high + data_low) / 2.0

        # 80% padding on each side of the actual candle range
        pad = data_range * 0.80
        price_min = data_low - pad
        price_max = data_high + pad
        price_step = (price_max - price_min) / PRICE_BINS
        # Avoid division by zero
        safe_total = np.where(total_vols > 0, total_vols, 1.0)
        buy_ratio = buy_vols / safe_total
        sell_ratio = sell_vols / safe_total

        opens = np.array([float(r["open"] or 0) for r in ohlcv])
        closes = np.array([float(r["close"] or 0) for r in ohlcv])
        timestamps = np.array([r["bucket"].timestamp() for r in ohlcv])

        # Time range (guard single-candle case)
        time_start = timestamps[0]
        time_end = timestamps[-1]
        if time_end <= time_start:
            time_end = time_start + 60.0  # at least 1 minute span

        # ── Transfer to GPU ──────────────────────────────────────
        highs_g = xp.asarray(highs)
        lows_g = xp.asarray(lows)
        volumes_g = xp.asarray(volumes)
        buy_ratio_g = xp.asarray(buy_ratio)
        sell_ratio_g = xp.asarray(sell_ratio)

        # ── Build volume profile (n_candles × PRICE_BINS) ────────
        # Distribute each candle's volume as a Gaussian centered on
        # the candle midpoint.  This avoids the sparse 1-2 bin problem
        # that occurs with uniform [low, high] distribution on BTC
        # 1-min candles (range ≈ $100 vs bin width ≈ $100).
        price_edges = xp.linspace(price_min, price_max, PRICE_BINS + 1,
                                  dtype=xp.float32)
        bin_centers = (price_edges[:-1] + price_edges[1:]) / 2

        # Gaussian parameters — fully vectorised (no Python loop)
        mids = (highs_g + lows_g) / 2.0                          # (n,)
        ranges = xp.maximum(highs_g - lows_g, price_step * 5)    # min 5 bins
        sigmas = ranges / 2.0                                     # (n,)

        # (n,1) – (1,PRICE_BINS) → (n, PRICE_BINS)
        dists = bin_centers[None, :] - mids[:, None]
        weights = xp.exp(-0.5 * (dists / sigmas[:, None]) ** 2)
        w_sums = xp.sum(weights, axis=1, keepdims=True)
        w_sums = xp.maximum(w_sums, xp.float32(1e-10))
        weights = weights / w_sums

        heatmap_long  = weights * (volumes_g * buy_ratio_g)[:, None]
        heatmap_short = weights * (volumes_g * sell_ratio_g)[:, None]

        # ── Scale by OI ──────────────────────────────────────────
        total_volume = float(xp.sum(volumes_g))
        if total_volume > 0 and oi_value > 0:
            # OI represents current open positions; volume is a proxy
            # for position-building. Scale so total estimated ≈ OI.
            oi_scale = oi_value / total_volume
            heatmap_long *= oi_scale
            heatmap_short *= oi_scale

        # ── Estimate liquidation prices per leverage tier ────────
        # For each leverage, shift the bin index and accumulate.
        # Fully vectorised — no Python loop over time steps.

        liq_heatmap_long = xp.zeros((n_candles, PRICE_BINS), dtype=xp.float32)
        liq_heatmap_short = xp.zeros((n_candles, PRICE_BINS), dtype=xp.float32)

        for lev, weight in LEVERAGE_TIERS.items():
            # Long liquidation: price drops to liq level
            liq_factor_long = 1.0 - 1.0 / lev + MAINTENANCE_MARGIN
            # Short liquidation: price rises to liq level
            liq_factor_short = 1.0 + 1.0 / lev - MAINTENANCE_MARGIN

            liq_prices_long = bin_centers * liq_factor_long
            liq_prices_short = bin_centers * liq_factor_short

            # Map liquidation prices to bin indices — shape (PRICE_BINS,)
            liq_bins_long = xp.clip(
                ((liq_prices_long - price_min) / price_step).astype(xp.int32),
                0, PRICE_BINS - 1
            )
            liq_bins_short = xp.clip(
                ((liq_prices_short - price_min) / price_step).astype(xp.int32),
                0, PRICE_BINS - 1
            )

            # Vectorised gather-scatter:
            # For each time row, column j contributes weight*vol to
            # column liq_bins[j].  Equivalent to fancy-index assignment.
            # result[:, liq_bins[j]] += src[:, j]
            # We can do this row-free by building the destination index
            # and using advanced indexing.
            long_src = heatmap_long * weight   # (n_candles, PRICE_BINS)
            short_src = heatmap_short * weight

            # Build full (n_candles, PRICE_BINS) index arrays by broadcasting
            row_idx = xp.arange(n_candles, dtype=xp.int32)[:, None]  # (n,1)
            col_long = liq_bins_long[None, :]   # (1, PRICE_BINS) → broadcast
            col_short = liq_bins_short[None, :]

            if _use_gpu:
                import cupyx
                # Flatten for scatter_add: (n×P,) index, (n×P,) values
                flat_idx_long = (row_idx * PRICE_BINS + col_long).ravel()
                flat_idx_short = (row_idx * PRICE_BINS + col_short).ravel()
                cupyx.scatter_add(
                    liq_heatmap_long.ravel(), flat_idx_long, long_src.ravel()
                )
                cupyx.scatter_add(
                    liq_heatmap_short.ravel(), flat_idx_short, short_src.ravel()
                )
            else:
                flat_idx_long = (row_idx * PRICE_BINS + col_long).ravel()
                flat_idx_short = (row_idx * PRICE_BINS + col_short).ravel()
                np.add.at(
                    liq_heatmap_long.ravel(), _to_numpy(flat_idx_long),
                    _to_numpy(long_src.ravel())
                )
                np.add.at(
                    liq_heatmap_short.ravel(), _to_numpy(flat_idx_short),
                    _to_numpy(short_src.ravel())
                )

        # ── Cumulative accumulation (Coinglass-style) ────────────
        # Transfer per-candle liquidation volumes to CPU.
        liq_long_np = _to_numpy(liq_heatmap_long)    # (n_candles, PRICE_BINS)
        liq_short_np = _to_numpy(liq_heatmap_short)

        # Build cumulative heatmap: at each time step, the heat at a
        # price level is the total accumulated liquidation volume from
        # all previous candles, MINUS positions that got swept when
        # the candle price passed through those levels.
        # This creates the horizontal-streak band pattern of Coinglass.
        price_arr = np.linspace(price_min, price_max, PRICE_BINS)

        cum_long = np.zeros((n_candles, PRICE_BINS), dtype=np.float64)
        cum_short = np.zeros((n_candles, PRICE_BINS), dtype=np.float64)

        # Exponential decay: older accumulations fade, creating
        # brightness variation across time (matching Coinglass look).
        decay = 0.993   # per candle (per minute); 0.993^360 ≈ 0.08

        for t in range(n_candles):
            if t > 0:
                cum_long[t] = cum_long[t - 1] * decay
                cum_short[t] = cum_short[t - 1] * decay

            # Add new estimated liquidation volume from this candle
            cum_long[t] += liq_long_np[t]
            cum_short[t] += liq_short_np[t]

            # Remove triggered liquidations:
            # Long positions get liquidated when price drops TO the level
            # → sweep everything at or below the candle low
            triggered_long = price_arr <= lows[t]
            cum_long[t, triggered_long] *= 0.02   # ~liquidated

            # Short positions get liquidated when price rises TO the level
            # → sweep everything at or above the candle high
            triggered_short = price_arr >= highs[t]
            cum_short[t, triggered_short] *= 0.02  # ~liquidated

        # ── 1-D marginal densities (for /data endpoint) ──────────
        liq_density_long = cum_long[-1].copy()
        liq_density_short = cum_short[-1].copy()

        # Subtract any actually-observed liquidation events
        for liq in realized_liqs:
            side = liq.get("side") or liq["side"]
            raw_avg = liq.get("avg_price")
            raw_price = liq.get("price")
            liq_price = float(raw_avg or 0) or float(raw_price or 0)
            filled_qty = float(liq.get("filled_qty") or 0)
            if liq_price <= 0 or filled_qty <= 0:
                continue
            if liq_price < price_min or liq_price > price_max:
                continue
            bin_idx = int((liq_price - price_min) / price_step)
            bin_idx = max(0, min(bin_idx, PRICE_BINS - 1))
            qty_usd = filled_qty * liq_price
            if side == "SELL":
                liq_density_long[bin_idx] = max(0, liq_density_long[bin_idx] - qty_usd)
            else:
                liq_density_short[bin_idx] = max(0, liq_density_short[bin_idx] - qty_usd)

        # ── Return result ────────────────────────────────────────
        return HeatmapData(
            symbol=symbol,
            price_min=price_min,
            price_max=price_max,
            price_bins=PRICE_BINS,
            mark_price=mark_price,
            liq_long=liq_density_long,
            liq_short=liq_density_short,
            heatmap_long=cum_long,
            heatmap_short=cum_short,
            opens=opens,
            highs_arr=highs,
            lows_arr=lows,
            closes=closes,
            timestamps=timestamps,
            volumes=volumes,
            time_start=time_start,
            time_end=time_end,
            time_bins=n_candles,
            oi_value=oi_value,
            lookback_hours=hours,
            computed_at=time.time(),
            compute_time_ms=0,  # filled by caller
        )

    # ── Helpers ──────────────────────────────────────────────────

    def _pick_gpu(self) -> int:
        if not _use_gpu or not GPU_DEVICES:
            return 0
        gpu_id = GPU_DEVICES[self._gpu_counter % len(GPU_DEVICES)]
        self._gpu_counter += 1
        return gpu_id

    async def _safe_generate(self, symbol: str) -> Optional[HeatmapData]:
        """Generate without raising — returns None on error."""
        try:
            return await self.generate(symbol)
        except Exception as exc:
            log.debug("Skipping %s in summary: %s", symbol, exc)
            return None
