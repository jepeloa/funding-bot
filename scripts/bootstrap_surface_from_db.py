#!/usr/bin/env python3
"""
Bootstrap Shannon P(win) surface from existing base-variant trades.

Connects to TimescaleDB, fetches all closed base trades, reconstructs
MFE(t) paths from ohlcv_1m candles, and builds the P(win | t, MFE) grid.

Output: data/pwin_surface.json
"""

import asyncio
import json
import os
import sys

# Add parent to path so we can import config
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncpg

# ── DB connection ──
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "binance_futures"
DB_USER = "recorder"
DB_PASS = os.environ.get("DB_PASS", "K32CzfnWtWtLoj98n6R5QTqEx3jLYLv5")

# ── Surface grid ──
T_GRID = [0, 2, 5, 10, 20, 30, 60, 120, 240]       # minutes
M_GRID = [0, 0.2, 0.5, 1, 2, 3, 5, 8, 12, 20]      # MFE in pct (0-20)

# ── Prior (Laplace smoothing) ──
PRIOR_WIN = 0.566
PRIOR_WEIGHT = 2   # pseudo-count: 2 wins + 2*(1-prior) losses


def build_path_from_candles(entry_price: float, entry_ts: float,
                            exit_ts: float, candles: list) -> list:
    """Reconstruct [secs, pnl, mfe, mae] path from 1-min OHLCV candles."""
    if not candles:
        return []

    samples = []
    for c in candles:
        bucket_ts = c[0].timestamp() if hasattr(c[0], 'timestamp') else float(c[0])
        base_sec = int(bucket_ts - entry_ts)
        o, h, l, close = float(c[1]), float(c[2]), float(c[3]), float(c[4])
        samples.append((max(base_sec, 0), o))
        pnl_h = (entry_price - h) / entry_price
        pnl_l = (entry_price - l) / entry_price
        if pnl_l < pnl_h:
            samples.append((base_sec + 20, l))
            samples.append((base_sec + 40, h))
        else:
            samples.append((base_sec + 20, h))
            samples.append((base_sec + 40, l))
        samples.append((base_sec + 59, close))

    samples.sort(key=lambda x: x[0])

    path = []
    mfe = 0.0
    mae = 0.0
    duration = int(exit_ts - entry_ts)

    for sec, price in samples:
        if sec < 0 or sec > duration + 60:
            continue
        pnl = (entry_price - price) / entry_price
        if pnl > mfe:
            mfe = pnl
        if pnl < mae:
            mae = pnl
        path.append((sec, pnl, mfe, mae))

    return path


def build_surface(all_observations: list) -> list:
    """
    Build P(win | t, MFE) surface from observations.

    all_observations: list of (t_min, mfe_pct, is_winner)
    Returns: [[t_min, mfe_pct, p_win], ...]
    """
    # Count wins/total per cell
    cells = {}
    for t, m, t_hi, m_hi in _grid_cells():
        cells[(t, m)] = {"wins": PRIOR_WEIGHT * PRIOR_WIN,
                         "total": PRIOR_WEIGHT}

    for t_min, mfe_pct, is_winner in all_observations:
        # Find the cell this observation belongs to
        t_cell = _find_bin(t_min, T_GRID)
        m_cell = _find_bin(mfe_pct, M_GRID)
        if t_cell is not None and m_cell is not None:
            key = (t_cell, m_cell)
            if key in cells:
                cells[key]["total"] += 1
                if is_winner:
                    cells[key]["wins"] += 1

    surface = []
    for (t, m), counts in sorted(cells.items()):
        pw = counts["wins"] / counts["total"] if counts["total"] > 0 else PRIOR_WIN
        surface.append([t, m, round(pw, 4)])

    return surface


def _grid_cells():
    """Generate all grid cell boundaries."""
    for i, t in enumerate(T_GRID):
        t_hi = T_GRID[i + 1] if i + 1 < len(T_GRID) else float('inf')
        for j, m in enumerate(M_GRID):
            m_hi = M_GRID[j + 1] if j + 1 < len(M_GRID) else float('inf')
            yield t, m, t_hi, m_hi


def _find_bin(value: float, grid: list):
    """Find which grid bin a value falls into."""
    for i in range(len(grid) - 1):
        if grid[i] <= value < grid[i + 1]:
            return grid[i]
    if value >= grid[-1]:
        return grid[-1]
    if value < grid[0]:
        return grid[0]
    return None


def build_surface_from_trades(trades, all_candles: dict) -> list:
    """
    Build P(win) surface from pre-fetched trades and candles.

    Args:
        trades: list of DB rows with symbol, entry_price, entry_time, exit_time, pnl_pct
        all_candles: dict mapping trade id -> list of candle tuples

    Returns: surface as [[t_min, mfe_pct, p_win], ...]
    """
    all_observations = []
    for tr in trades:
        tid = tr['id']
        candles = all_candles.get(tid, [])
        if not candles:
            continue
        entry_price = float(tr['entry_price'])
        entry_ts = float(tr['entry_time'])
        exit_ts = float(tr['exit_time'])
        pnl_pct = float(tr['pnl_pct']) if tr['pnl_pct'] is not None else 0.0
        is_winner = pnl_pct > 0

        path = build_path_from_candles(entry_price, entry_ts, exit_ts, candles)
        if not path:
            continue

        for sec, pnl, mfe, mae in path:
            all_observations.append((sec / 60.0, mfe * 100, is_winner))

    return build_surface(all_observations)


async def main():
    print("Connecting to DB...")
    pool = await asyncpg.create_pool(
        host=DB_HOST, port=DB_PORT, database=DB_NAME,
        user=DB_USER, password=DB_PASS, min_size=1, max_size=2,
    )

    # Fetch all closed base trades
    async with pool.acquire() as conn:
        trades = await conn.fetch("""
            SELECT symbol, entry_price, entry_time, exit_time, pnl_pct
            FROM virtual_trades
            WHERE variant = 'base'
              AND status = 'closed'
              AND trading_mode = 'paper'
            ORDER BY entry_time
        """)
    print(f"Found {len(trades)} closed base trades")

    # ── Step 1: Concurrent OHLCV fetch ──
    sem = asyncio.Semaphore(8)

    async def _fetch_candles(tr):
        async with sem:
            async with pool.acquire() as c:
                rows = await c.fetch(
                    "SELECT bucket, open, high, low, close FROM ohlcv_1m "
                    "WHERE symbol = $1 "
                    "AND bucket >= to_timestamp($2) - interval '1 minute' "
                    "AND bucket <= to_timestamp($3) + interval '1 minute' "
                    "ORDER BY bucket",
                    tr['symbol'], float(tr['entry_time']), float(tr['exit_time']),
                )
            return tr, [(r[0], r[1], r[2], r[3], r[4]) for r in rows]

    print("Fetching OHLCV paths concurrently...")
    fetched = await asyncio.gather(*[_fetch_candles(tr) for tr in trades])
    print(f"All fetches done")

    # ── Step 2: Build paths + extract observations ──
    all_observations = []
    processed = 0
    skipped = 0

    for tr, candles in fetched:
        if not candles:
            skipped += 1
            continue
        entry_price = float(tr['entry_price'])
        entry_ts = float(tr['entry_time'])
        exit_ts = float(tr['exit_time'])
        pnl_pct = float(tr['pnl_pct']) if tr['pnl_pct'] is not None else 0.0
        is_winner = pnl_pct > 0

        path = build_path_from_candles(entry_price, entry_ts, exit_ts, candles)
        if not path:
            skipped += 1
            continue

        for sec, pnl, mfe, mae in path:
            all_observations.append((sec / 60.0, mfe * 100, is_winner))
        processed += 1

    print(f"Processed: {processed}, Skipped: {skipped}")
    print(f"Total observations: {len(all_observations)}")

    # ── Step 3: Build surface ──
    surface = build_surface(all_observations)
    print(f"Surface cells: {len(surface)}")

    print("\nSample cells:")
    for cell in surface[:15]:
        print(f"  t={cell[0]:>5}min  MFE={cell[1]:>5}%  P(win)={cell[2]:.4f}")

    output_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data", "pwin_surface.json"
    )
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(surface, f, indent=2)
    print(f"\nSurface written to: {output_path}")

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
