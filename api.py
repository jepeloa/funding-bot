"""
REST API para acceder a los datos grabados de Binance Futures (TimescaleDB).

Uso:
  uvicorn api:app --host 0.0.0.0 --port 8000
  # O directamente:
  python3 api.py

Docs interactivos: http://<host>:8000/docs
"""

import os
import secrets
import shutil
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

import aiohttp
import asyncpg
from fastapi import FastAPI, HTTPException, Query, Security, Depends
from fastapi.security import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, Response
from pydantic import BaseModel

from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

# ══════════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════════

API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8000"))

# API Keys — comma-separated list.  Auto-generate one if missing.
_raw_keys = os.getenv("API_KEYS", "")
if _raw_keys:
    API_KEYS = set(k.strip() for k in _raw_keys.split(",") if k.strip())
else:
    _auto = secrets.token_urlsafe(32)
    API_KEYS = {_auto}
    print(f"\n⚠  No API_KEYS env var set.  Auto-generated key:\n   {_auto}\n")

# Max rows per response to protect bandwidth / memory
MAX_LIMIT = 10_000
DEFAULT_LIMIT = 100

# Heatmap micro-service (separate process on port 8009)
HEATMAP_SERVICE_URL = os.getenv("HEATMAP_SERVICE_URL", "http://127.0.0.1:8009")
HEATMAP_PROXY_TIMEOUT = 30  # seconds


# ══════════════════════════════════════════════════════════════════
#  AUTH
# ══════════════════════════════════════════════════════════════════

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(key: str = Security(_api_key_header)):
    if not key or key not in API_KEYS:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
    return key


# ══════════════════════════════════════════════════════════════════
#  DB POOL (asyncpg)
# ══════════════════════════════════════════════════════════════════

pool: asyncpg.Pool | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    pool = await asyncpg.create_pool(
        host=DB_HOST, port=DB_PORT, database=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
        min_size=2, max_size=15,
        command_timeout=120,
    )
    yield
    await pool.close()


# ══════════════════════════════════════════════════════════════════
#  APP
# ══════════════════════════════════════════════════════════════════

app = FastAPI(
    title="Binance Futures Recorder API",
    version="1.0.0",
    description="Acceso REST a los datos de mercado grabados desde Binance Futures USDT-M (TimescaleDB).",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ══════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════

def _clamp_limit(limit: int) -> int:
    return max(1, min(limit, MAX_LIMIT))


def _parse_time(s: str | None) -> datetime | None:
    """Accept ISO-8601 or epoch seconds/millis."""
    if s is None:
        return None
    # Try epoch
    try:
        v = float(s)
        if v > 1e12:  # millis
            v /= 1000.0
        return datetime.fromtimestamp(v, tz=timezone.utc)
    except ValueError:
        pass
    # ISO
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except ValueError:
        raise HTTPException(400, f"Invalid time format: {s}")


class _TimeFilter:
    """Reusable time-range filtering."""

    def __init__(self, col: str, start: str | None, end: str | None):
        self.clauses: list[str] = []
        self.params: list = []
        self._idx = 1
        if start:
            dt = _parse_time(start)
            self.clauses.append(f"{col} >= ${self._next()}")
            self.params.append(dt)
        if end:
            dt = _parse_time(end)
            self.clauses.append(f"{col} <= ${self._next()}")
            self.params.append(dt)

    def _next(self) -> int:
        v = self._idx
        self._idx += 1
        return v

    def add_symbol(self, sym: str | None):
        if sym:
            self.clauses.append(f"symbol = ${self._next()}")
            self.params.append(sym.upper())

    def where(self) -> str:
        if not self.clauses:
            return ""
        return "WHERE " + " AND ".join(self.clauses)

    @property
    def next_idx(self) -> int:
        return self._idx


# ══════════════════════════════════════════════════════════════════
#  ENDPOINTS — Dashboard
# ══════════════════════════════════════════════════════════════════

_DASHBOARD_HTML = None

def _load_dashboard() -> str:
    """Load dashboard HTML (re-reads on each request for easy development)."""
    path = os.path.join(os.path.dirname(__file__), "dashboard.html")
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    """Trade dashboard — no API key required in URL, key is injected into the page."""
    html = _load_dashboard()
    # Inject the first API key so JS can call endpoints
    key = next(iter(API_KEYS))
    html = html.replace("{{API_KEY}}", key)
    return HTMLResponse(content=html)


@app.get("/chart", response_class=HTMLResponse)
async def chart_page():
    """Multi-series chart (OI + Funding + Price) — no API key in URL."""
    path = os.path.join(os.path.dirname(__file__), "chart_pixel.html")
    with open(path, "r", encoding="utf-8") as f:
        html = f.read()
    key = next(iter(API_KEYS))
    html = html.replace("{{API_KEY}}", key)
    return HTMLResponse(content=html)


@app.get("/report", response_class=HTMLResponse)
async def report_page():
    """v2 strategy report dashboard."""
    path = os.path.join(os.path.dirname(__file__), "report.html")
    with open(path, "r", encoding="utf-8") as f:
        html = f.read()
    key = next(iter(API_KEYS))
    html = html.replace("{{API_KEY}}", key)
    return HTMLResponse(content=html)


# ══════════════════════════════════════════════════════════════════
#  ENDPOINTS — Health / Stats
# ══════════════════════════════════════════════════════════════════

@app.get("/health")
async def health():
    """Health check (no auth required)."""
    try:
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(503, detail=str(e))


@app.get("/stats", dependencies=[Depends(verify_api_key)])
async def stats():
    """Resumen general: conteo de registros, rango temporal, tamaño DB."""
    tables = [
        ("depth_updates", "event_time"),
        ("agg_trades", "event_time"),
        ("book_tickers", "event_time"),
        ("mark_prices", "event_time"),
        ("open_interest", "polled_at"),
        ("funding_rates", "funding_time"),
        ("strategy_snapshots", "timestamp"),
        ("liquidations", "event_time"),
        ("long_short_ratio", "timestamp"),
        ("taker_buy_sell", "timestamp"),
    ]
    result = {}
    async with pool.acquire() as conn:
        for table, ts_col in tables:
            try:
                cnt = await conn.fetchval(
                    "SELECT approximate_row_count($1::regclass)", table
                )
                row = await conn.fetchrow(
                    f"SELECT MIN({ts_col}) as min_t, MAX({ts_col}) as max_t FROM {table}"
                )
                result[table] = {
                    "rows_approx": cnt,
                    "from": row["min_t"].isoformat() if row["min_t"] else None,
                    "to": row["max_t"].isoformat() if row["max_t"] else None,
                }
            except Exception as e:
                result[table] = {"error": str(e)}

        # Virtual trades
        open_cnt = await conn.fetchval(
            "SELECT COUNT(*) FROM virtual_trades WHERE status='open'"
        )
        closed_cnt = await conn.fetchval(
            "SELECT COUNT(*) FROM virtual_trades WHERE status='closed'"
        )
        result["virtual_trades"] = {"open": open_cnt, "closed": closed_cnt}

        # DB size
        size_bytes = await conn.fetchval(
            "SELECT pg_database_size($1)", DB_NAME
        )
        result["db_size_bytes"] = size_bytes
        result["db_size_gb"] = round(size_bytes / (1024**3), 2)

        # Symbols count
        sym_cnt = await conn.fetchval(
            "SELECT COUNT(DISTINCT symbol) FROM agg_trades"
        )
        result["symbols_count"] = sym_cnt

    return result


@app.get("/symbols", dependencies=[Depends(verify_api_key)])
async def symbols():
    """Lista de todos los símbolos grabados (usa mark_prices por velocidad)."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT DISTINCT symbol FROM mark_prices ORDER BY symbol"
        )
    return [r["symbol"] for r in rows]


@app.get("/dbsize", dependencies=[Depends(verify_api_key)])
async def dbsize():
    """Tamaño detallado por hypertable."""
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT
                hypertable_name,
                hypertable_size(format('%I', hypertable_name)::regclass) as size_bytes,
                pg_size_pretty(hypertable_size(format('%I', hypertable_name)::regclass)) as size_pretty
            FROM timescaledb_information.hypertables
            ORDER BY hypertable_size(format('%I', hypertable_name)::regclass) DESC
        """)
        total = await conn.fetchval("SELECT pg_database_size($1)", DB_NAME)
    return {
        "tables": [
            {
                "name": r["hypertable_name"],
                "size_bytes": r["size_bytes"],
                "size_pretty": r["size_pretty"],
            }
            for r in rows
        ],
        "total_bytes": total,
        "total_pretty": f"{total / (1024**3):.2f} GB",
    }


@app.get("/storage", dependencies=[Depends(verify_api_key)])
async def storage():
    """Monitoreo de almacenamiento: tamaño DB por tabla, espacio en disco, tasa de crecimiento y días restantes estimados."""
    # ── Disk usage ──
    DATA_PATH = os.getenv("DATA_PATH", "/media/mapplics-ia/recorder-data")
    try:
        usage = shutil.disk_usage(DATA_PATH)
        disk = {
            "path": DATA_PATH,
            "total_bytes": usage.total,
            "total_pretty": f"{usage.total / (1024**3):.1f} GB",
            "used_bytes": usage.used,
            "used_pretty": f"{usage.used / (1024**3):.1f} GB",
            "free_bytes": usage.free,
            "free_pretty": f"{usage.free / (1024**3):.1f} GB",
            "used_pct": round(usage.used / usage.total * 100, 1),
        }
    except OSError as e:
        disk = {"error": str(e)}

    async with pool.acquire() as conn:
        # ── Per-table sizes ──
        rows = await conn.fetch("""
            SELECT
                hypertable_name,
                hypertable_size(format('%I', hypertable_name)::regclass) as size_bytes,
                pg_size_pretty(hypertable_size(format('%I', hypertable_name)::regclass)) as size_pretty
            FROM timescaledb_information.hypertables
            ORDER BY hypertable_size(format('%I', hypertable_name)::regclass) DESC
        """)

        db_total = await conn.fetchval("SELECT pg_database_size($1)", DB_NAME)

        # ── Row counts + time ranges ──
        table_details = []
        for r in rows:
            name = r["hypertable_name"]
            ts_col = {
                "depth_updates": "event_time",
                "agg_trades": "event_time",
                "book_tickers": "event_time",
                "mark_prices": "event_time",
                "open_interest": "polled_at",
                "funding_rates": "funding_time",
                "strategy_snapshots": "timestamp",
                "liquidations": "event_time",
                "long_short_ratio": "timestamp",
                "taker_buy_sell": "timestamp",
            }.get(name)
            detail = {
                "name": name,
                "size_bytes": r["size_bytes"],
                "size_pretty": r["size_pretty"],
            }
            if ts_col:
                try:
                    cnt = await conn.fetchval(
                        "SELECT approximate_row_count($1::regclass)", name
                    )
                    rng = await conn.fetchrow(
                        f"SELECT MIN({ts_col}) as t0, MAX({ts_col}) as t1 FROM {name}"
                    )
                    detail["rows_approx"] = cnt
                    detail["from"] = rng["t0"].isoformat() if rng["t0"] else None
                    detail["to"] = rng["t1"].isoformat() if rng["t1"] else None
                except Exception:
                    pass
            table_details.append(detail)

        # ── Compression stats ──
        try:
            comp = await conn.fetch("""
                SELECT
                    hypertable_name,
                    before_compression_total_bytes,
                    after_compression_total_bytes,
                    CASE WHEN after_compression_total_bytes > 0
                         THEN round(before_compression_total_bytes::numeric
                              / after_compression_total_bytes, 1)
                         ELSE NULL END AS ratio
                FROM hypertable_compression_stats
                ORDER BY before_compression_total_bytes DESC
            """)
            compression = [
                {
                    "table": c["hypertable_name"],
                    "before_bytes": c["before_compression_total_bytes"],
                    "after_bytes": c["after_compression_total_bytes"],
                    "ratio": float(c["ratio"]) if c["ratio"] else None,
                }
                for c in comp
            ]
        except Exception:
            compression = []

        # ── Growth rate estimate ──
        # Use agg_trades time range + DB size to estimate daily growth
        growth = {}
        try:
            rng = await conn.fetchrow(
                "SELECT MIN(event_time) as t0, MAX(event_time) as t1 FROM agg_trades"
            )
            if rng["t0"] and rng["t1"]:
                span = rng["t1"] - rng["t0"]
                days = span.total_seconds() / 86400
                if days >= 1:
                    daily_gb = (db_total / (1024**3)) / days
                    free_gb = disk.get("free_bytes", 0) / (1024**3) if isinstance(disk, dict) and "free_bytes" in disk else 0
                    growth = {
                        "recording_days": round(days, 1),
                        "db_growth_gb_per_day": round(daily_gb, 2),
                        "est_days_until_full": round(free_gb / daily_gb, 0) if daily_gb > 0 else None,
                    }
        except Exception:
            pass

    return {
        "disk": disk,
        "database": {
            "total_bytes": db_total,
            "total_pretty": f"{db_total / (1024**3):.2f} GB",
            "tables": table_details,
        },
        "compression": compression,
        "growth": growth,
    }


# ══════════════════════════════════════════════════════════════════
#  ENDPOINTS — Market Data
# ══════════════════════════════════════════════════════════════════

@app.get("/trades/{symbol}", dependencies=[Depends(verify_api_key)])
async def trades(
    symbol: str,
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    start: Optional[str] = Query(None, description="Start time (ISO-8601 / epoch)"),
    end: Optional[str] = Query(None, description="End time (ISO-8601 / epoch)"),
):
    """Aggregated trades para un símbolo."""
    tf = _TimeFilter("event_time", start, end)
    tf.add_symbol(symbol)
    lim_idx = tf.next_idx
    sql = (
        f"SELECT event_time, agg_trade_id, price, quantity, is_buyer_maker "
        f"FROM agg_trades {tf.where()} "
        f"ORDER BY event_time DESC LIMIT ${lim_idx}"
    )
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *tf.params, _clamp_limit(limit))
    return [
        {
            "time": r["event_time"].isoformat(),
            "id": r["agg_trade_id"],
            "price": r["price"],
            "qty": r["quantity"],
            "is_buyer_maker": r["is_buyer_maker"],
        }
        for r in rows
    ]


@app.get("/depth/{symbol}", dependencies=[Depends(verify_api_key)])
async def depth(
    symbol: str,
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
):
    """Snapshots de order book (L20) para un símbolo."""
    tf = _TimeFilter("event_time", start, end)
    tf.add_symbol(symbol)
    lim_idx = tf.next_idx
    sql = (
        f"SELECT event_time, last_update_id, bid_prices, bid_qtys, ask_prices, ask_qtys "
        f"FROM depth_updates {tf.where()} "
        f"ORDER BY event_time DESC LIMIT ${lim_idx}"
    )
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *tf.params, _clamp_limit(limit))
    return [
        {
            "time": r["event_time"].isoformat(),
            "update_id": r["last_update_id"],
            "bids": list(zip(r["bid_prices"], r["bid_qtys"])),
            "asks": list(zip(r["ask_prices"], r["ask_qtys"])),
        }
        for r in rows
    ]


@app.get("/tickers/{symbol}", dependencies=[Depends(verify_api_key)])
async def tickers(
    symbol: str,
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
):
    """Book tickers (best bid/ask) para un símbolo."""
    tf = _TimeFilter("event_time", start, end)
    tf.add_symbol(symbol)
    lim_idx = tf.next_idx
    sql = (
        f"SELECT event_time, best_bid_price, best_bid_qty, "
        f"best_ask_price, best_ask_qty "
        f"FROM book_tickers {tf.where()} "
        f"ORDER BY event_time DESC LIMIT ${lim_idx}"
    )
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *tf.params, _clamp_limit(limit))
    return [
        {
            "time": r["event_time"].isoformat(),
            "bid_price": r["best_bid_price"],
            "bid_qty": r["best_bid_qty"],
            "ask_price": r["best_ask_price"],
            "ask_qty": r["best_ask_qty"],
            "spread": r["best_ask_price"] - r["best_bid_price"],
        }
        for r in rows
    ]


@app.get("/marks/{symbol}", dependencies=[Depends(verify_api_key)])
async def marks(
    symbol: str,
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
):
    """Mark price, index price y funding rate para un símbolo."""
    tf = _TimeFilter("event_time", start, end)
    tf.add_symbol(symbol)
    lim_idx = tf.next_idx
    sql = (
        f"SELECT event_time, mark_price, index_price, funding_rate, next_funding_ts "
        f"FROM mark_prices {tf.where()} "
        f"ORDER BY event_time DESC LIMIT ${lim_idx}"
    )
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *tf.params, _clamp_limit(limit))
    return [
        {
            "time": r["event_time"].isoformat(),
            "mark_price": r["mark_price"],
            "index_price": r["index_price"],
            "funding_rate": r["funding_rate"],
            "next_funding": r["next_funding_ts"].isoformat(),
        }
        for r in rows
    ]


@app.get("/oi/{symbol}", dependencies=[Depends(verify_api_key)])
async def oi(
    symbol: str,
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
):
    """Open interest para un símbolo."""
    tf = _TimeFilter("polled_at", start, end)
    tf.add_symbol(symbol)
    lim_idx = tf.next_idx
    sql = (
        f"SELECT polled_at, oi_contracts, oi_value "
        f"FROM open_interest {tf.where()} "
        f"ORDER BY polled_at DESC LIMIT ${lim_idx}"
    )
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *tf.params, _clamp_limit(limit))
    return [
        {
            "time": r["polled_at"].isoformat(),
            "contracts": r["oi_contracts"],
            "value": r["oi_value"],
        }
        for r in rows
    ]


@app.get("/funding/{symbol}", dependencies=[Depends(verify_api_key)])
async def funding(
    symbol: str,
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
):
    """Historial de funding rates para un símbolo."""
    tf = _TimeFilter("funding_time", start, end)
    tf.add_symbol(symbol)
    lim_idx = tf.next_idx
    sql = (
        f"SELECT funding_time, funding_rate "
        f"FROM funding_rates {tf.where()} "
        f"ORDER BY funding_time DESC LIMIT ${lim_idx}"
    )
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *tf.params, _clamp_limit(limit))
    return [
        {
            "time": r["funding_time"].isoformat(),
            "funding_rate": r["funding_rate"],
        }
        for r in rows
    ]


@app.get("/ohlcv/{symbol}", dependencies=[Depends(verify_api_key)])
async def ohlcv(
    symbol: str,
    interval: str = Query("1m", description="1m, 1h, or 1d"),
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
):
    """Velas OHLCV desde continuous aggregates (1m, 1h, 1d)."""
    view_map = {"1m": "ohlcv_1m", "1h": "ohlcv_1h", "1d": "ohlcv_1d"}
    view = view_map.get(interval.lower())
    if not view:
        raise HTTPException(400, f"interval must be one of: {', '.join(view_map)}")

    tf = _TimeFilter("bucket", start, end)
    tf.add_symbol(symbol)
    lim_idx = tf.next_idx
    sql = (
        f"SELECT bucket, open, high, low, close, volume, volume_usdt, "
        f"trade_count, taker_buy_volume, taker_sell_volume "
        f"FROM {view} {tf.where()} "
        f"ORDER BY bucket DESC LIMIT ${lim_idx}"
    )
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *tf.params, _clamp_limit(limit))
    return [
        {
            "time": r["bucket"].isoformat(),
            "open": r["open"],
            "high": r["high"],
            "low": r["low"],
            "close": r["close"],
            "volume": r["volume"],
            "volume_usdt": r["volume_usdt"],
            "trades": r["trade_count"],
            "taker_buy_vol": r["taker_buy_volume"],
            "taker_sell_vol": r["taker_sell_volume"],
        }
        for r in rows
    ]


@app.get("/liquidations", dependencies=[Depends(verify_api_key)])
async def liquidations(
    symbol: Optional[str] = Query(None),
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
):
    """Liquidaciones recientes (todas o por símbolo)."""
    tf = _TimeFilter("event_time", start, end)
    if symbol:
        tf.add_symbol(symbol)
    lim_idx = tf.next_idx
    sql = (
        f"SELECT event_time, symbol, side, original_qty, price, "
        f"avg_price, filled_qty, order_status "
        f"FROM liquidations {tf.where()} "
        f"ORDER BY event_time DESC LIMIT ${lim_idx}"
    )
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *tf.params, _clamp_limit(limit))
    return [
        {
            "time": r["event_time"].isoformat(),
            "symbol": r["symbol"],
            "side": r["side"],
            "qty": r["original_qty"],
            "bankruptcy_price": r["price"],
            "avg_price": r["avg_price"],
            "filled_qty": r["filled_qty"],
            "status": r["order_status"],
        }
        for r in rows
    ]


@app.get("/lsr/{symbol}", dependencies=[Depends(verify_api_key)])
async def lsr(
    symbol: str,
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
):
    """Long/Short Ratio (top account, top position, global)."""
    tf = _TimeFilter("timestamp", start, end)
    tf.add_symbol(symbol)
    lim_idx = tf.next_idx
    sql = (
        f"SELECT timestamp, ratio_type, long_short_ratio, "
        f"long_account_pct, short_account_pct "
        f"FROM long_short_ratio {tf.where()} "
        f"ORDER BY timestamp DESC LIMIT ${lim_idx}"
    )
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *tf.params, _clamp_limit(limit))
    return [
        {
            "time": r["timestamp"].isoformat(),
            "type": r["ratio_type"],
            "ratio": r["long_short_ratio"],
            "long_pct": r["long_account_pct"],
            "short_pct": r["short_account_pct"],
        }
        for r in rows
    ]


@app.get("/taker/{symbol}", dependencies=[Depends(verify_api_key)])
async def taker(
    symbol: str,
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
):
    """Taker Buy/Sell Volume ratio."""
    tf = _TimeFilter("timestamp", start, end)
    tf.add_symbol(symbol)
    lim_idx = tf.next_idx
    sql = (
        f"SELECT timestamp, buy_sell_ratio, buy_vol, sell_vol "
        f"FROM taker_buy_sell {tf.where()} "
        f"ORDER BY timestamp DESC LIMIT ${lim_idx}"
    )
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *tf.params, _clamp_limit(limit))
    return [
        {
            "time": r["timestamp"].isoformat(),
            "buy_sell_ratio": r["buy_sell_ratio"],
            "buy_vol": r["buy_vol"],
            "sell_vol": r["sell_vol"],
        }
        for r in rows
    ]


# ══════════════════════════════════════════════════════════════════
#  ENDPOINTS — Strategy
# ══════════════════════════════════════════════════════════════════

@app.get("/snapshots/{symbol}", dependencies=[Depends(verify_api_key)])
async def snapshots(
    symbol: str,
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
):
    """Snapshots de la estrategia para un símbolo."""
    tf = _TimeFilter("timestamp", start, end)
    tf.add_symbol(symbol)
    lim_idx = tf.next_idx
    sql = (
        f"SELECT timestamp, symbol, score_total, c_fund, c_oi, c_price, "
        f"c_taker, c_vol, energy_hours, exhaustion, mark_price, "
        f"funding_rate, oi_value, taker_buy_ratio, volume_ratio, "
        f"price_change_12h, price_change_24h, sma_24h, premium_velocity "
        f"FROM strategy_snapshots {tf.where()} "
        f"ORDER BY timestamp DESC LIMIT ${lim_idx}"
    )
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *tf.params, _clamp_limit(limit))
    return [dict(r) for r in rows]


@app.get("/vtrades", dependencies=[Depends(verify_api_key)])
async def vtrades(
    status: Optional[str] = Query(None, description="open or closed"),
    symbol: Optional[str] = Query(None),
    trading_mode: Optional[str] = Query(None, description="paper or live"),
    variant: Optional[str] = Query(None, description="conservative, base, aggressive, high_energy"),
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
):
    """Virtual trades de la estrategia. Filtrable por status, symbol, trading_mode y variant."""
    clauses = []
    params = []
    idx = 1
    if status:
        clauses.append(f"status = ${idx}")
        params.append(status)
        idx += 1
    if symbol:
        clauses.append(f"symbol = ${idx}")
        params.append(symbol.upper())
        idx += 1
    if trading_mode:
        clauses.append(f"trading_mode = ${idx}")
        params.append(trading_mode)
        idx += 1
    if variant:
        clauses.append(f"variant = ${idx}")
        params.append(variant)
        idx += 1
    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    sql = (
        f"SELECT * FROM virtual_trades {where} "
        f"ORDER BY entry_time DESC LIMIT ${idx}"
    )
    params.append(_clamp_limit(limit))
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *params)
        results = [dict(r) for r in rows]

        # Enrich open trades with live mark price & PnL
        open_symbols = list({r["symbol"] for r in results if r.get("status") == "open"})
        if open_symbols:
            placeholders = ", ".join(f"${i+1}" for i in range(len(open_symbols)))
            prices = await conn.fetch(
                f"SELECT DISTINCT ON (symbol) symbol, mark_price "
                f"FROM mark_prices WHERE symbol IN ({placeholders}) "
                f"ORDER BY symbol, event_time DESC",
                *open_symbols,
            )
            price_map = {r["symbol"]: float(r["mark_price"]) for r in prices}
            now_ts = time.time()
            for r in results:
                if r.get("status") != "open":
                    continue
                mp = price_map.get(r["symbol"])
                if mp and r.get("entry_price"):
                    ep = float(r["entry_price"])
                    lev = int(r.get("leverage") or 1)
                    ps = float(r.get("position_size") or 0)
                    pnl_pct = (ep - mp) / ep  # short
                    r["_mark_price"] = mp
                    r["pnl_pct"] = pnl_pct
                    r["pnl_leveraged"] = pnl_pct * lev
                    r["pnl_usd"] = pnl_pct * ps
                    r["hold_hours"] = (now_ts - float(r["entry_time"])) / 3600
                    r["mfe_pct"] = float(r["mfe_pct"] or 0)
                    r["mae_pct"] = float(r["mae_pct"] or 0)

    return results


@app.get("/trade/exit_signals", dependencies=[Depends(verify_api_key)])
async def trade_exit_signals():
    """
    Para cada trade abierto, calcula el estado de TODAS las condiciones
    de salida y muestra cuánto falta para que se dispare cada una.
    """
    from config import VARIANTS
    import time as _time

    now = _time.time()
    async with pool.acquire() as conn:
        open_trades = await conn.fetch(
            "SELECT * FROM virtual_trades WHERE status='open' ORDER BY entry_time"
        )
        if not open_trades:
            return {"trades": [], "message": "No hay trades abiertos"}

        results = []
        for t in open_trades:
            t = dict(t)
            symbol = t["symbol"]
            vname = t["variant"]
            vparams = VARIANTS.get(vname)
            if not vparams:
                continue

            entry_price = float(t["entry_price"])
            entry_time = float(t["entry_time"])
            snap = t.get("entry_snapshot") or {}
            if isinstance(snap, str):
                import json as _json
                try:
                    snap = _json.loads(snap)
                except Exception:
                    snap = {}
            entry_oi_raw = snap.get("oi_value", 0)
            if isinstance(entry_oi_raw, dict):
                entry_oi_raw = entry_oi_raw.get("oi_value", 0)
            entry_oi = float(entry_oi_raw or 0)
            mfe = float(t.get("mfe_pct") or 0)
            mae = float(t.get("mae_pct") or 0)
            leverage = int(t.get("leverage") or 1)

            # Live mark price
            mp_row = await conn.fetchrow(
                "SELECT mark_price, funding_rate FROM mark_prices "
                "WHERE symbol=$1 ORDER BY event_time DESC LIMIT 1", symbol
            )
            mark_price = float(mp_row["mark_price"]) if mp_row else entry_price
            funding_rate = float(mp_row["funding_rate"]) if mp_row else 0

            # Live OI
            oi_row = await conn.fetchrow(
                "SELECT oi_value FROM open_interest "
                "WHERE symbol=$1 ORDER BY polled_at DESC LIMIT 1", symbol
            )
            current_oi = float(oi_row["oi_value"]) if oi_row else 0

            # OI 1h ago
            oi_1h_row = await conn.fetchrow(
                "SELECT oi_value FROM open_interest "
                "WHERE symbol=$1 AND polled_at < now() - interval '55 min' "
                "ORDER BY polled_at DESC LIMIT 1", symbol
            )
            oi_1h_ago = float(oi_1h_row["oi_value"]) if oi_1h_row else current_oi

            # Taker buy ratio (last 5min)
            taker_row = await conn.fetchrow(
                "SELECT SUM(buy_vol) as buyvol, "
                "SUM(buy_vol + sell_vol) as totalvol "
                "FROM taker_buy_sell WHERE symbol=$1 AND timestamp > now() - interval '5 min'",
                symbol
            )
            taker_buy_ratio = 0.5
            if taker_row and taker_row["totalvol"] and float(taker_row["totalvol"]) > 0:
                taker_buy_ratio = float(taker_row["buyvol"]) / float(taker_row["totalvol"])

            # Price change 24h (approx from mark_prices)
            mp_24h_row = await conn.fetchrow(
                "SELECT mark_price FROM mark_prices "
                "WHERE symbol=$1 AND event_time < now() - interval '23 hours' "
                "ORDER BY event_time DESC LIMIT 1", symbol
            )
            price_24h_ago = float(mp_24h_row["mark_price"]) if mp_24h_row else mark_price
            price_change_24h = (mark_price - price_24h_ago) / price_24h_ago if price_24h_ago > 0 else 0

            # Calculations
            pnl_pct = (entry_price - mark_price) / entry_price
            hold_hours = (now - entry_time) / 3600.0
            oi_change = (current_oi - entry_oi) / entry_oi if entry_oi > 0 else 0
            oi_change_1h = (current_oi - oi_1h_ago) / oi_1h_ago if oi_1h_ago > 0 else 0

            # Try AEPS calibrated params first, fall back to static
            aeps_path = os.path.join(os.path.dirname(__file__), f"aeps_{vname}.json")
            aeps_params = None
            if os.path.exists(aeps_path):
                try:
                    import json as _json2
                    with open(aeps_path) as _af:
                        aeps_data = _json2.load(_af)
                    aeps_params = aeps_data.get("current_params", {})
                except Exception:
                    pass

            sl_pct = (aeps_params or {}).get("stop_loss_pct") or vparams["stop_loss_pct"]
            tp_pct = vparams["take_profit_pct"]
            max_hold = vparams["max_hold_hours"]
            min_hold = vparams["min_hold_hours"]
            oi_abort_pct = vparams["oi_abort_pct"]
            trail_act = (aeps_params or {}).get("trailing_activation_pct") or vparams.get("trailing_activation_pct", 0)
            breakeven_trig = (aeps_params or {}).get("breakeven_trigger_pct") or vparams.get("breakeven_trigger_pct", 0)
            trail_cb = (aeps_params or {}).get("trailing_callback_pct") or vparams.get("trailing_callback_pct", 0.5)
            ea_hours_aeps = (aeps_params or {}).get("early_abort_hours")
            ea_loss_aeps = (aeps_params or {}).get("early_abort_max_loss")
            ea_mfe_aeps = (aeps_params or {}).get("early_abort_max_mfe")
            pt_mfe_aeps = (aeps_params or {}).get("partial_tp_mfe_pct")
            pt_frac_aeps = (aeps_params or {}).get("partial_tp_fraction")
            p_lock_aeps = (aeps_params or {}).get("profit_lock_pct")

            # ── Build each exit signal status ──
            signals = []

            # 1. Stop Loss
            sl_distance = (-pnl_pct - sl_pct) / sl_pct  # negative = still safe
            sl_price = entry_price * (1 + sl_pct)
            signals.append({
                "name": "Stop Loss",
                "icon": "🛑",
                "threshold": f"-{sl_pct*100:.1f}%",
                "current": f"{pnl_pct*100:+.2f}%",
                "progress": min(max(-pnl_pct / sl_pct * 100, 0), 100),
                "triggered": pnl_pct <= -sl_pct,
                "detail": f"SL @ ${sl_price:.6g} (mark ${mark_price:.6g})",
            })

            # 2. Take Profit
            tp_price = entry_price * (1 - tp_pct)
            signals.append({
                "name": "Take Profit",
                "icon": "🎯",
                "threshold": f"+{tp_pct*100:.1f}%",
                "current": f"{pnl_pct*100:+.2f}%",
                "progress": min(max(pnl_pct / tp_pct * 100, 0), 100),
                "triggered": pnl_pct >= tp_pct,
                "detail": f"TP @ ${tp_price:.6g} (mark ${mark_price:.6g})",
            })

            # 3. Trailing continuo con floor dinámico
            trail_floor = None
            if breakeven_trig > 0 and mfe >= breakeven_trig:
                if trail_act > 0 and mfe >= trail_act:
                    trail_floor = mfe * (1.0 - trail_cb)
                else:
                    range_width = max(trail_act - breakeven_trig, 0.001)
                    progress_ramp = min((mfe - breakeven_trig) / range_width, 1.0)
                    max_floor = mfe * (1.0 - trail_cb)
                    trail_floor = progress_ramp * max_floor
            elif trail_act > 0 and mfe >= trail_act:
                trail_floor = mfe * (1.0 - trail_cb)

            trail_active = trail_floor is not None
            trail_progress = 0
            if breakeven_trig > 0:
                trail_progress = min(mfe / breakeven_trig * 100, 100)
            elif trail_act > 0:
                trail_progress = min(mfe / trail_act * 100, 100)

            signals.append({
                "name": "Trailing Stop",
                "icon": "📐",
                "threshold": f"BE≥{breakeven_trig*100:.1f}% → trail → {trail_act*100:.1f}%",
                "current": f"MFE={mfe*100:.2f}%",
                "progress": trail_progress,
                "triggered": trail_active and pnl_pct < trail_floor,
                "detail": f"Floor={trail_floor*100:.2f}%, PnL={pnl_pct*100:.2f}%" if trail_active else f"Inactivo (MFE < {breakeven_trig*100:.1f}%)",
            })

            # 5. Max Hold
            signals.append({
                "name": "Max Hold Time",
                "icon": "⏰",
                "threshold": f"{max_hold}h",
                "current": f"{hold_hours:.1f}h",
                "progress": min(hold_hours / max_hold * 100, 100),
                "triggered": hold_hours >= max_hold,
                "detail": f"Faltan {max(max_hold - hold_hours, 0):.1f}h",
            })

            # 6. OI Abort
            signals.append({
                "name": "OI Abort",
                "icon": "📊",
                "threshold": f"ΔOI>{oi_abort_pct*100:.0f}%",
                "current": f"ΔOI={oi_change*100:+.2f}%",
                "progress": min(max(oi_change / oi_abort_pct * 100, 0), 100) if oi_abort_pct > 0 else 0,
                "triggered": oi_change > oi_abort_pct,
                "detail": f"OI entry={entry_oi:,.0f} → now={current_oi:,.0f}",
            })

            # 7. Pump Capture
            half_pump = 0.5 * abs(price_change_24h) if abs(price_change_24h) > 0 else 0.01
            pc_progress = min(mfe / half_pump * 100, 100) if half_pump > 0 else 0
            signals.append({
                "name": "Pump Capture",
                "icon": "🚀",
                "threshold": f"MFE≥½|ΔP24h| ({half_pump*100:.2f}%)",
                "current": f"MFE={mfe*100:.2f}%, Ê≥2",
                "progress": pc_progress,
                "triggered": False,  # computed in strategy with live exhaustion
                "detail": f"ΔP24h={price_change_24h*100:+.1f}%",
            })

            # 8. Reversal Signal
            rev_conditions = [
                hold_hours >= min_hold,
                pnl_pct > 0.01,
                oi_change_1h > 0,
                taker_buy_ratio > 0.55,
            ]
            rev_met = sum(rev_conditions)
            signals.append({
                "name": "Reversal Signal",
                "icon": "🔄",
                "threshold": "hold≥min & PnL>1% & dOI₁ₕ>0 & η_buy>55%",
                "current": f"{rev_met}/4 condiciones",
                "progress": rev_met / 4 * 100,
                "triggered": all(rev_conditions),
                "detail": f"hold={'✓' if rev_conditions[0] else '✗'} PnL={'✓' if rev_conditions[1] else '✗'} dOI={'✓' if rev_conditions[2] else '✗'} η={'✓' if rev_conditions[3] else '✗'}",
            })

            # 9. Partial TP (MAX scheme)
            pt_mfe = pt_mfe_aeps if pt_mfe_aeps is not None else vparams.get("partial_tp_mfe_pct", 0)
            pt_frac = pt_frac_aeps if pt_frac_aeps is not None else vparams.get("partial_tp_fraction", 0)
            if pt_mfe > 0:
                pt_progress = min(mfe / pt_mfe * 100, 100)
                signals.append({
                    "name": "Partial TP",
                    "icon": "✂️",
                    "threshold": f"{pt_frac*100:.0f}% @ MFE≥{pt_mfe*100:.1f}%",
                    "current": f"MFE={mfe*100:.2f}%",
                    "progress": pt_progress,
                    "triggered": mfe >= pt_mfe,
                    "detail": f"Cierra {pt_frac*100:.0f}% cuando MFE alcance {pt_mfe*100:.1f}%",
                })

            # 10. Profit Lock (post partial TP)
            p_lock = p_lock_aeps if p_lock_aeps is not None else vparams.get("profit_lock_pct", 0)
            if p_lock > 0 and pt_mfe > 0:
                lock_active = mfe >= pt_mfe  # partial TP already triggered
                if lock_active:
                    lock_progress = min(max((p_lock - pnl_pct) / p_lock * 100, 0), 100) if p_lock > 0 else 0
                else:
                    lock_progress = 0
                signals.append({
                    "name": "Profit Lock",
                    "icon": "🔒",
                    "threshold": f"Floor +{p_lock*100:.1f}%",
                    "current": f"PnL={pnl_pct*100:.2f}%",
                    "progress": lock_progress,
                    "triggered": lock_active and pnl_pct <= p_lock,
                    "detail": f"{'Activo' if lock_active else 'Inactivo'} — cierra si PnL ≤ +{p_lock*100:.1f}%",
                })

            # 11. Early Abort (MAX scheme)
            ea_hours = ea_hours_aeps if ea_hours_aeps is not None else vparams.get("early_abort_hours", 0)
            ea_mfe = ea_mfe_aeps if ea_mfe_aeps is not None else vparams.get("early_abort_max_mfe", 0)
            ea_loss = ea_loss_aeps if ea_loss_aeps is not None else vparams.get("early_abort_max_loss", 0)
            if ea_hours > 0:
                ea_time_pct = min(hold_hours / ea_hours * 100, 100)
                ea_conds = [hold_hours > ea_hours, mfe < ea_mfe, pnl_pct < ea_loss]
                ea_met = sum(ea_conds)
                signals.append({
                    "name": "Early Abort",
                    "icon": "⏱️",
                    "threshold": f">{ea_hours:.0f}h & MFE<{ea_mfe*100:.1f}% & PnL<{ea_loss*100:.1f}%",
                    "current": f"{ea_met}/3 condiciones",
                    "progress": ea_met / 3 * 100,
                    "triggered": all(ea_conds),
                    "detail": f"hold={'✓' if ea_conds[0] else '✗'} MFE={'✓' if ea_conds[1] else '✗'} PnL={'✓' if ea_conds[2] else '✗'}",
                })

            results.append({
                "trade_id": t["id"],
                "symbol": symbol,
                "variant": vname,
                "trading_mode": t.get("trading_mode", "paper"),
                "entry_price": entry_price,
                "mark_price": mark_price,
                "pnl_pct": pnl_pct,
                "pnl_leveraged": pnl_pct * leverage,
                "hold_hours": hold_hours,
                "mfe_pct": mfe,
                "mae_pct": mae,
                "funding_rate": funding_rate,
                "leverage": leverage,
                "signals": signals,
            })

    return {"trades": results}


@app.get("/pnl", dependencies=[Depends(verify_api_key)])
async def pnl(
    trading_mode: Optional[str] = Query(None, description="paper or live — omit for combined"),
):
    """Resumen de PnL de la estrategia, separado por modo (paper/live)."""
    async with pool.acquire() as conn:
        if trading_mode:
            rows = await conn.fetch(
                "SELECT * FROM virtual_trades WHERE status='closed' AND trading_mode=$1 ORDER BY exit_time",
                trading_mode,
            )
            open_cnt = await conn.fetchval(
                "SELECT COUNT(*) FROM virtual_trades WHERE status='open' AND trading_mode=$1",
                trading_mode,
            )
        else:
            rows = await conn.fetch(
                "SELECT * FROM virtual_trades WHERE status='closed' ORDER BY exit_time"
            )
            open_cnt = await conn.fetchval(
                "SELECT COUNT(*) FROM virtual_trades WHERE status='open'"
            )

    if not rows:
        return {"trades": 0, "message": "No closed trades yet", "trading_mode": trading_mode or "all"}

    def _build_pnl_summary(trade_rows):
        total_pnl = sum(r["pnl_usd"] or 0 for r in trade_rows)
        wins = [r for r in trade_rows if (r["pnl_usd"] or 0) >= 0]
        losses = [r for r in trade_rows if (r["pnl_usd"] or 0) < 0]
        win_rate = len(wins) / len(trade_rows) * 100 if trade_rows else 0

        variants = {}
        for r in trade_rows:
            v = r["variant"] or "unknown"
            if v not in variants:
                variants[v] = {"count": 0, "wins": 0, "pnl": 0.0}
            variants[v]["count"] += 1
            variants[v]["pnl"] += r["pnl_usd"] or 0
            if (r["pnl_usd"] or 0) >= 0:
                variants[v]["wins"] += 1

        return {
            "total_trades": len(trade_rows),
            "winners": len(wins),
            "losers": len(losses),
            "win_rate_pct": round(win_rate, 1),
            "total_pnl_usd": round(total_pnl, 2),
            "best_trade_usd": round(max((r["pnl_usd"] or 0) for r in trade_rows), 2),
            "worst_trade_usd": round(min((r["pnl_usd"] or 0) for r in trade_rows), 2),
            "avg_hold_hours": round(
                sum(r["hold_hours"] or 0 for r in trade_rows) / len(trade_rows), 1
            ),
            "by_variant": {
                v: {
                    "count": d["count"],
                    "win_rate_pct": round(d["wins"] / d["count"] * 100, 1) if d["count"] else 0,
                    "pnl_usd": round(d["pnl"], 2),
                }
                for v, d in variants.items()
            },
        }

    result = _build_pnl_summary(rows)
    result["trading_mode"] = trading_mode or "all"
    result["open_trades"] = open_cnt

    # Si no se filtró por modo, incluir desglose paper vs live
    if not trading_mode:
        paper_rows = [r for r in rows if (r.get("trading_mode") or "paper") == "paper"]
        live_rows = [r for r in rows if (r.get("trading_mode") or "paper") == "live"]
        result["by_mode"] = {}
        if paper_rows:
            result["by_mode"]["paper"] = _build_pnl_summary(paper_rows)
        if live_rows:
            result["by_mode"]["live"] = _build_pnl_summary(live_rows)

    return result


# ══════════════════════════════════════════════════════════════════
#  ENDPOINTS — Spread analysis
# ══════════════════════════════════════════════════════════════════

@app.get("/spread/{symbol}", dependencies=[Depends(verify_api_key)])
async def spread(
    symbol: str,
    limit: int = Query(1000, ge=1, le=MAX_LIMIT),
):
    """Análisis de spread bid-ask."""
    tf = _TimeFilter("event_time", None, None)
    tf.add_symbol(symbol)
    lim_idx = tf.next_idx
    sql = (
        f"SELECT best_ask_price - best_bid_price as spread, best_bid_price as bid "
        f"FROM book_tickers {tf.where()} "
        f"ORDER BY event_time DESC LIMIT ${lim_idx}"
    )
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *tf.params, _clamp_limit(limit))

    if not rows:
        raise HTTPException(404, f"No data for {symbol}")

    spreads = [float(r["spread"]) for r in rows]
    avg = sum(spreads) / len(spreads)
    bid = float(rows[0]["bid"])
    return {
        "symbol": symbol.upper(),
        "samples": len(spreads),
        "avg_spread": avg,
        "min_spread": min(spreads),
        "max_spread": max(spreads),
        "spread_pct": (avg / bid * 100) if bid > 0 else None,
    }


# ══════════════════════════════════════════════════════════════════
#  ENDPOINTS — Liquidation Heatmaps (proxy to heatmap service)
# ══════════════════════════════════════════════════════════════════

async def _proxy_heatmap(path: str, params: dict | None = None) -> Response:
    """Forward request to the heatmap micro-service."""
    url = f"{HEATMAP_SERVICE_URL}{path}"
    timeout = aiohttp.ClientTimeout(total=HEATMAP_PROXY_TIMEOUT)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, params=params) as resp:
                body = await resp.read()
                if resp.status != 200:
                    raise HTTPException(
                        status_code=resp.status,
                        detail=body.decode(errors="replace"),
                    )
                return Response(
                    content=body,
                    media_type=resp.content_type,
                    headers={
                        k: v for k, v in resp.headers.items()
                        if k.lower() in ("cache-control", "x-heatmap-size")
                    },
                )
    except aiohttp.ClientError as e:
        raise HTTPException(503, detail=f"Heatmap service unavailable: {e}")


@app.get("/heatmap/{symbol}", dependencies=[Depends(verify_api_key)])
async def heatmap(
    symbol: str,
    hours: int = Query(6, ge=1, le=24),
    width: int = Query(1200, ge=400, le=3840),
    height: int = Query(800, ge=300, le=2160),
    format: str = Query("webp"),
    nocache: bool = Query(False),
):
    """Liquidation heatmap image for a symbol (proxied to GPU service)."""
    params = {
        "hours": hours, "width": width, "height": height,
        "format": format, "nocache": str(nocache).lower(),
    }
    return await _proxy_heatmap(f"/heatmap/{symbol.upper()}", params)


@app.get("/heatmap/{symbol}/data", dependencies=[Depends(verify_api_key)])
async def heatmap_data(
    symbol: str,
    hours: int = Query(6, ge=1, le=24),
):
    """Liquidation heatmap raw data as JSON."""
    return await _proxy_heatmap(
        f"/heatmap/{symbol.upper()}/data",
        {"hours": hours},
    )


@app.get("/heatmap/summary", dependencies=[Depends(verify_api_key)])
async def heatmap_summary(
    top_n: int = Query(20, ge=1, le=100),
):
    """Top symbols by estimated liquidation density."""
    return await _proxy_heatmap("/summary", {"top_n": top_n})


# ══════════════════════════════════════════════════════════════════
#  BINANCE ACCOUNT ENDPOINTS
# ══════════════════════════════════════════════════════════════════

@app.get("/binance/accounts", dependencies=[Depends(verify_api_key)])
async def binance_accounts():
    """Lista de cuentas configuradas y modo de operación actual."""
    from config import (
        load_trading_config, BINANCE_ACCOUNTS as accts
    )
    tc = load_trading_config()
    from config import VARIANTS
    return {
        "trading_mode": tc["trading_mode"],
        "active_account": tc["active_account"],
        "active_variant": tc.get("active_variant", "base"),
        "variants": list(VARIANTS.keys()),
        "accounts": [
            {
                "name": name,
                "label": cfg["label"],
                "key_masked": cfg["api_key"][:8] + "..." + cfg["api_key"][-4:],
            }
            for name, cfg in accts.items()
        ],
    }


@app.get("/binance/config", dependencies=[Depends(verify_api_key)])
async def get_binance_config():
    """Lee la configuración actual de trading (del archivo compartido)."""
    from config import load_trading_config
    return load_trading_config()


@app.post("/binance/config", dependencies=[Depends(verify_api_key)])
async def set_binance_config(
    trading_mode: str = Query(..., description="dry-run | live"),
    account: str = Query(..., description="principal | copytrading | copytrading_privado"),
    variant: str = Query("base", description="conservative | base | aggressive | high_energy"),
):
    """
    Cambia el modo de trading, cuenta activa y variante.
    El recorder lo detecta automáticamente en el próximo ciclo (~10s).
    """
    from config import save_trading_config, load_trading_config, BINANCE_ACCOUNTS, VARIANTS

    if trading_mode not in ("dry-run", "live"):
        raise HTTPException(400, "trading_mode must be 'dry-run' or 'live'")
    if account not in BINANCE_ACCOUNTS:
        raise HTTPException(400, f"account must be one of: {', '.join(BINANCE_ACCOUNTS)}")
    if variant not in VARIANTS:
        raise HTTPException(400, f"variant must be one of: {', '.join(VARIANTS)}")

    old = load_trading_config()
    save_trading_config(trading_mode, account, variant)

    import logging
    log = logging.getLogger("api")
    log.warning(
        f"⚙️ Trading config changed: "
        f"{old['trading_mode']}→{trading_mode}, "
        f"{old['active_account']}→{account}, "
        f"variant={variant}"
    )

    new_cfg = {"trading_mode": trading_mode, "active_account": account, "active_variant": variant}
    return {
        "status": "ok",
        "previous": old,
        "current": new_cfg,
        "message": f"Modo cambiado a {trading_mode} (cuenta: {account}, variante: {variant}). "
                   f"El recorder aplicará el cambio en ~10 segundos.",
    }


@app.get("/binance/balance", dependencies=[Depends(verify_api_key)])
async def binance_balance(
    account: str = Query("principal", description="principal | copytrading | copytrading_privado"),
):
    """Balance USDT de una cuenta Binance (consulta en vivo)."""
    from config import BINANCE_ACCOUNTS as accts
    from binance_trader import BinanceTrader

    acct = accts.get(account)
    if not acct:
        raise HTTPException(404, f"Cuenta '{account}' no encontrada")

    trader = BinanceTrader(
        api_key=acct["api_key"],
        api_secret=acct["api_secret"],
        account_name=account,
    )
    try:
        await trader.connect()
        balance = await trader.get_account_balance()
        positions = await trader.get_positions()
        return {
            "account": account,
            "label": acct["label"],
            "balance": balance,
            "positions": positions,
            "position_count": len(positions),
        }
    finally:
        await trader.close()


@app.get("/binance/positions", dependencies=[Depends(verify_api_key)])
async def binance_positions(
    account: str = Query("principal", description="principal | copytrading | copytrading_privado"),
    symbol: Optional[str] = Query(None),
):
    """Posiciones abiertas en Binance Futures."""
    from config import BINANCE_ACCOUNTS as accts
    from binance_trader import BinanceTrader

    acct = accts.get(account)
    if not acct:
        raise HTTPException(404, f"Cuenta '{account}' no encontrada")

    trader = BinanceTrader(
        api_key=acct["api_key"],
        api_secret=acct["api_secret"],
        account_name=account,
    )
    try:
        await trader.connect()
        positions = await trader.get_positions(symbol)
        return positions
    finally:
        await trader.close()


# ══════════════════════════════════════════════════════════════════
#  STRATEGY STATUS (file-based, written by recorder every 15s)
# ══════════════════════════════════════════════════════════════════

_STATUS_FILE = os.path.join(os.path.dirname(__file__), "strategy_status.json")

@app.get("/strategy/status", dependencies=[Depends(verify_api_key)])
async def strategy_status():
    """Estado actual de la estrategia: hot candidates, open trades, equities."""
    import json as _json2
    try:
        with open(_STATUS_FILE, "r") as f:
            return _json2.load(f)
    except FileNotFoundError:
        raise HTTPException(503, "Status file not available yet")


# ══════════════════════════════════════════════════════════════════
#  AEPS CALIBRATOR DATA
# ══════════════════════════════════════════════════════════════════

@app.get("/aeps", dependencies=[Depends(verify_api_key)])
async def aeps_data(
    variant: str = Query("base", description="Variant name"),
):
    """
    Returns full AEPS calibrator state for a variant, including
    param evolution history (replayed from trade history).
    """
    import json as _json3
    from adaptive_exit import AdaptiveExitCalibrator, TradeRecord
    from config import VARIANTS

    base_dir = os.path.dirname(__file__)
    aeps_path = os.path.join(base_dir, f"aeps_{variant}.json")
    if not os.path.exists(aeps_path):
        raise HTTPException(404, f"No AEPS file for variant '{variant}'")

    with open(aeps_path) as f:
        raw = _json3.load(f)

    vparams = VARIANTS.get(variant, {})
    history = raw.get("history", [])
    current = raw.get("current_params", {})
    cal_count = raw.get("calibration_count", 0)

    # Replay calibration to build param evolution
    cal = AdaptiveExitCalibrator(vparams)
    evolution = []
    static = cal._from_static(vparams)

    PARAM_KEYS = [
        "stop_loss_pct", "partial_tp_mfe_pct", "profit_lock_pct",
        "breakeven_trigger_pct", "trailing_activation_pct",
        "trailing_callback_pct", "early_abort_hours",
        "early_abort_max_mfe",
    ]

    # Record static baseline
    evolution.append({
        "trade_idx": 0,
        "label": "static",
        **{k: getattr(static, k) for k in PARAM_KEYS},
    })

    for i, t in enumerate(history):
        rec = TradeRecord(**t)
        cal.add_trade(rec)
        snap = {}
        for k in PARAM_KEYS:
            snap[k] = getattr(cal.current, k)
        snap["trade_idx"] = i + 1
        snap["label"] = t.get("exit_reason", "")
        snap["is_winner"] = t.get("is_winner", False)
        snap["pnl_pct"] = t.get("pnl_pct", 0)
        snap["mfe_pct"] = t.get("mfe_pct", 0)
        snap["mae_pct"] = t.get("mae_pct", 0)
        snap["etd_pct"] = t.get("etd_pct", 0)
        snap["hold_hours"] = t.get("hold_secs", 0) / 3600
        snap["calibrated"] = len(cal.history) >= cal.MIN_TRADES_TO_CALIBRATE
        evolution.append(snap)

    # Bounds for the UI
    bounds = {k: list(v) for k, v in AdaptiveExitCalibrator.BOUNDS.items()
              if k in PARAM_KEYS}

    # Win rate evolution
    wins = losses = 0
    wr_series = []
    for i, t in enumerate(history):
        if t.get("is_winner"):
            wins += 1
        else:
            losses += 1
        wr_series.append({"trade_idx": i + 1, "win_rate": wins / (wins + losses)})

    return {
        "variant": variant,
        "current_params": current,
        "calibration_count": cal_count,
        "history": history,
        "evolution": evolution,
        "bounds": bounds,
        "win_rate_series": wr_series,
        "min_trades": AdaptiveExitCalibrator.MIN_TRADES_TO_CALIBRATE,
        "window_size": AdaptiveExitCalibrator.WINDOW_SIZE,
    }


# ══════════════════════════════════════════════════════════════════
#  TRADE PATH RECONSTRUCTION (uses ohlcv_1m continuous aggregate)
# ══════════════════════════════════════════════════════════════════


def _build_path_from_candles(entry_price: float, entry_ts: float,
                              exit_ts: float, candles: list) -> list:
    """Build [secs, pnl, mfe, mae] path from 1-minute OHLCV candles.

    Each candle expands into up to 4 price points (O, H, L, C) to capture
    the intra-candle extremes for accurate MFE/MAE tracking.
    """
    if not candles:
        return []

    # Expand candles into (offset_secs, price) samples
    samples: list[tuple[int, float]] = []
    for c in candles:
        bucket_ts = c[0].timestamp() if hasattr(c[0], "timestamp") else float(c[0])
        base_sec = int(bucket_ts - entry_ts)
        o, h, l, close = float(c[1]), float(c[2]), float(c[3]), float(c[4])
        samples.append((max(base_sec, 0), o))
        # Order H/L by which is more adverse for short
        pnl_h = (entry_price - h) / entry_price
        pnl_l = (entry_price - l) / entry_price
        if pnl_l < pnl_h:  # L is more adverse for short
            samples.append((base_sec + 20, l))
            samples.append((base_sec + 40, h))
        else:
            samples.append((base_sec + 20, h))
            samples.append((base_sec + 40, l))
        samples.append((base_sec + 59, close))

    samples.sort(key=lambda x: x[0])

    # Build path with adaptive downsampling:
    # Keep every point for first 300s, then ~60s intervals after
    path = []
    mfe = 0.0
    mae = 0.0
    last_emit_sec = -999
    duration = int(exit_ts - entry_ts)

    for sec, price in samples:
        if sec < 0 or sec > duration + 60:
            continue
        pnl = (entry_price - price) / entry_price  # short
        if pnl > mfe:
            mfe = pnl
        if pnl < mae:
            mae = pnl
        # Adaptive: full detail first 300s, ~60s after
        if sec <= 300 or (sec - last_emit_sec) >= 60 or sec >= duration - 60:
            path.append([sec, round(pnl, 6), round(mfe, 6), round(mae, 6)])
            last_emit_sec = sec

    return path


@app.get("/vtrades/paths", dependencies=[Depends(verify_api_key)])
async def vtrades_paths(
    variant: Optional[str] = Query(None),
    symbol: Optional[str] = Query(None),
    version: Optional[str] = Query(None, description="Comma-separated strategy versions, e.g. v2,v3,v4"),
    limit: int = Query(100, ge=1, le=500),
):
    """Closed trades with reconstructed price path from ohlcv_1m aggregate."""
    clauses = ["status = 'closed'"]
    params = []
    idx = 1
    if variant:
        clauses.append(f"variant = ${idx}")
        params.append(variant)
        idx += 1
    if symbol:
        clauses.append(f"symbol = ${idx}")
        params.append(symbol.upper())
        idx += 1
    if version:
        versions = [v.strip() for v in version.split(",") if v.strip()]
        placeholders = ", ".join(f"${idx + i}" for i in range(len(versions)))
        clauses.append(f"strategy_version IN ({placeholders})")
        params.extend(versions)
        idx += len(versions)
    where = "WHERE " + " AND ".join(clauses)
    sql = (
        f"SELECT id, symbol, variant, entry_price, entry_time, exit_time, "
        f"exit_reason, pnl_pct, mfe_pct, mae_pct, hold_hours, etd_pct, leverage, "
        f"strategy_version "
        f"FROM virtual_trades {where} "
        f"ORDER BY exit_time DESC LIMIT ${idx}"
    )
    params.append(_clamp_limit(limit))

    import asyncio

    async with pool.acquire() as conn:
        trades = await conn.fetch(sql, *params)
        if not trades:
            return []

        trade_list = []
        for t in trades:
            trade_list.append((t, float(t["entry_time"]), float(t["exit_time"])))

    # ── Fetch candles per-trade with bounded concurrency ──
    sem = asyncio.Semaphore(8)

    async def _fetch_one(t, entry_ts, exit_ts):
        entry_price = float(t["entry_price"])
        async with sem:
            async with pool.acquire() as c:
                rows = await c.fetch(
                    "SELECT bucket, open, high, low, close FROM ohlcv_1m "
                    "WHERE symbol = $1 "
                    "AND bucket >= to_timestamp($2) - interval '1 minute' "
                    "AND bucket <= to_timestamp($3) + interval '1 minute' "
                    "ORDER BY bucket",
                    t["symbol"], entry_ts, exit_ts,
                )
        candles = [(r["bucket"], r["open"], r["high"], r["low"], r["close"])
                   for r in rows]
        path = _build_path_from_candles(entry_price, entry_ts, exit_ts, candles)
        return {
            "trade_id": t["id"],
            "symbol": t["symbol"],
            "variant": t["variant"],
            "strategy_version": t["strategy_version"],
            "entry_price": entry_price,
            "entry_time": entry_ts,
            "exit_time": exit_ts,
            "exit_reason": t["exit_reason"],
            "pnl_pct": float(t["pnl_pct"] or 0),
            "mfe_pct": float(t["mfe_pct"] or 0),
            "mae_pct": float(t["mae_pct"] or 0),
            "hold_hours": float(t["hold_hours"] or 0),
            "etd_pct": float(t["etd_pct"] or 0),
            "leverage": int(t["leverage"] or 1),
            "path": path,
        }

    results = await asyncio.gather(
        *[_fetch_one(t, ets, xts) for t, ets, xts in trade_list]
    )
    return list(results)


# ══════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "api:app",
        host=API_HOST,
        port=API_PORT,
        log_level="info",
        access_log=True,
    )
