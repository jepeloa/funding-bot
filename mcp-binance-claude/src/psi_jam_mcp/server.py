"""
Ψ-jam MCP Server
================
Binance Futures data, physics-based analysis, and trading execution.

Architecture:
  src/psi_jam_mcp/
    server.py            ← THIS FILE: tool definitions + handlers (MCP entry point)
    binance_client.py    ← Async Binance Futures REST client (public endpoints)
    analysis.py          ← Ψ-jam physics: Kramers-Moyal, Hurst, Lyapunov, RQA, VPIN, JAM regime
    technical_analysis.py← Standard TA indicators (RSI, MACD, Bollinger, etc.)
    asymmetry.py         ← Range asymmetry: cycle detection, R:R, win rate, composite score
    oi_chart.py          ← OIED: OI energy divergence analysis + interactive charts
    scanner.py           ← Altcoin scanner: sigma-filtered + JAM-scored short opportunities
    context.py           ← Contextualización: interpreta datos crudos → veredictos en español
    l2_store.py          ← SQLite storage + background collector for L2 snapshots
    futures_trader.py    ← Authenticated trading client (multi-account)

Tool categories (46 tools):
  ┌──────────────────────────────────────────────────────────────────────┐
  │ CATEGORY            │ PREFIX       │ PURPOSE                        │
  ├──────────────────────────────────────────────────────────────────────┤
  │ 📊 DATA (11)        │ get_*/list_  │ Raw Binance market data        │
  │ 🔍 SCANNER (2)      │ scan_        │ Discover short opportunities   │
  │ 🌐 GLOBAL MARKET(1) │ get_global_  │ Market-wide short conditions   │
  │ 📈 TECH ANALYSIS(1) │ get_tech*    │ Standard TA multi-timeframe    │
  │ 🔬 Ψ-JAM (8)       │ analyze_*    │ Physics-based Langevin analysis│
  │ ⚡ OI LEVEL (1)      │ oi_level     │ OI energy divergence (OIED)    │
  │ 💥 LIQUIDATIONS (1) │ get_liq*     │ Liquidation cluster analysis   │
  │ 🎯 ASYMMETRY (1)    │ analyze_*    │ R:R cycle-based scoring        │
  │ 📋 L2 HISTORY (6)   │ *_l2_*       │ Order book recording system    │
  │ 💰 TRADING (10)     │ futures_*    │ Position execution & mgmt      │
  │ 🛡️ GUARD (3)        │ *_guard/*_entry│ Entry eval + auto watchdog   │
  └──────────────────────────────────────────────────────────────────────┘

Recommended workflow for short trading:
  1. get_global_market           → Assess market conditions (FAVORABLE/NEUTRAL/DESFAVORABLE)
  2. scan_altcoins               → Find pumped alts with JAM confirmation
  3. get_technical_analysis      → Multi-TF TA on candidates
  4. full_jam_pipeline           → Ψ-jam Langevin regime classification
  5. oi_level                    → OI energy state (TRAP/DISSIPATION warning)
  6. get_liquidation_clusters    → Identify liquidation zones & cascade risks
  7. analyze_range_asymmetry     → Validate R:R before entry
  8. evaluate_short_entry        → Final checklist R0-R4 + sizing + GO/WAIT/NO_TRADE
  9. futures_open_position       → Execute trade (ask user about auto-management first)
 10. activate_guard              → Enable background watchdog (auto_close if user accepted)
 11. futures_set_tp_sl           → Set TP/SL protection (if not set at open)
 12. guard_status                → Monitor guard alerts & auto-closes

All responses include a 'context' field with short-trading interpretations in Spanish.

Usage:
    pip install -e .
    psi-jam-mcp                    # stdio transport (for Claude Desktop)
    python -m psi_jam_mcp.server   # same
"""

import asyncio
import json
import os
import time
import numpy as np
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

from .binance_client import BinanceClient
from . import analysis
from . import technical_analysis as ta
from . import context as ctx
from .l2_store import L2Store, L2Collector
from .scanner import AltcoinScanner
from .futures_trader import FuturesTrader
from . import oi_chart
from . import asymmetry
from .spot_client import SpotClient
from .basis_scanner import BasisScanner
from .basis_trader import BasisTrader
from . import basis_engine
from . import ghost_flow
from .strategy_guard import StrategyGuard
from .copytrading_client import CopyTradingClient
from .striker_strategy import StrikerStrategy
from .carry_detector import CarryDetector

# ─────────────────────────────────────────────
# SERVER SETUP
# ─────────────────────────────────────────────

server = Server("psi-jam-mcp")
client = BinanceClient()
l2_store = L2Store()
l2_collector = L2Collector(client, l2_store)
scanner = AltcoinScanner(client)

# ── SPOT CLIENT (public, no auth needed for scanning) ──
spot_client = SpotClient()

# ── BASIS SCANNER (uses both spot + futures public endpoints) ──
basis_scanner = BasisScanner(futures_client=client, spot_client=spot_client)

# ── CARRY DETECTOR (funding rate carry trade scanner) ──
carry_detector = CarryDetector(futures_client=client)

# ── MULTI-ACCOUNT TRADING ──
# Supported accounts and their env-var prefixes
ACCOUNT_CONFIG = {
    "principal": {
        "key_env": "BINANCE_API_KEY",
        "secret_env": "BINANCE_API_SECRET",
        "label": "Principal",
    },
    "copytrading": {
        "key_env": "BINANCE_COPYTRADING_API_KEY",
        "secret_env": "BINANCE_COPYTRADING_API_SECRET",
        "label": "Copytrading",
    },
}

_traders: dict[str, FuturesTrader] = {}
_basis_traders: dict[str, BasisTrader] = {}

# ── COPY-TRADING SCRAPER (Playwright-based) ──
copytrading_client = CopyTradingClient()

# ── STRATEGY GUARD (background watchdog) ──
# Initialized here, loop started in main() once event loop is running.
strategy_guard: StrategyGuard | None = None  # set after _get_trader is defined

# ── STRIKER × Ψ-JAM HYBRID STRATEGY ──
# Initialized after _get_trader is defined (needs lazy trader + copytrading + ghost_flow)
striker_strategy: StrikerStrategy | None = None


def _get_trader(account: str = "principal") -> FuturesTrader:
    """Lazy-init the authenticated trading client for a given account."""
    account = account.lower().strip()
    if account not in ACCOUNT_CONFIG:
        raise ValueError(
            f"Cuenta '{account}' no reconocida. "
            f"Cuentas disponibles: {', '.join(ACCOUNT_CONFIG.keys())}"
        )
    if account not in _traders:
        cfg = ACCOUNT_CONFIG[account]
        api_key = os.environ.get(cfg["key_env"], "")
        api_secret = os.environ.get(cfg["secret_env"], "")
        if not api_key or not api_secret:
            raise ValueError(
                f"API keys para cuenta '{cfg['label']}' no configuradas. "
                f"Agrega {cfg['key_env']} y {cfg['secret_env']} al archivo .env"
            )
        _traders[account] = FuturesTrader(api_key=api_key, api_secret=api_secret)
    return _traders[account]


def _get_basis_trader(account: str = "principal") -> BasisTrader:
    """Lazy-init the basis trader (needs authenticated spot + futures clients)."""
    account = account.lower().strip()
    if account not in ACCOUNT_CONFIG:
        raise ValueError(
            f"Cuenta '{account}' no reconocida. "
            f"Cuentas disponibles: {', '.join(ACCOUNT_CONFIG.keys())}"
        )
    if account not in _basis_traders:
        cfg = ACCOUNT_CONFIG[account]
        api_key = os.environ.get(cfg["key_env"], "")
        api_secret = os.environ.get(cfg["secret_env"], "")
        if not api_key or not api_secret:
            raise ValueError(
                f"API keys para cuenta '{cfg['label']}' no configuradas. "
                f"Agrega {cfg['key_env']} y {cfg['secret_env']} al archivo .env"
            )
        auth_spot = SpotClient(api_key=api_key, api_secret=api_secret)
        futures_trader = _get_trader(account)
        _basis_traders[account] = BasisTrader(
            spot_client=auth_spot,
            futures_trader=futures_trader,
        )
    return _basis_traders[account]


def _arg(args: dict, key: str, default):
    """Get argument with null-coalescing: returns default when key is missing OR value is None."""
    val = args.get(key)
    return val if val is not None else default


def _json_default(obj):
    """Handle numpy / non-standard types for JSON serialization."""
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return float(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def _json_response(data: dict) -> list[TextContent]:
    """Format response as JSON text content."""
    return [TextContent(type="text", text=json.dumps(data, indent=2, ensure_ascii=False, default=_json_default))]


# ─────────────────────────────────────────────
# TOOL DEFINITIONS
# ─────────────────────────────────────────────

@server.list_tools()
async def list_tools() -> list[Tool]:
    return [
        # ── DATA TOOLS ──
        Tool(
            name="get_klines",
            description=(
                "Fetch OHLCV klines (candlestick data) from Binance Futures. "
                "Returns open, high, low, close, volume, taker_buy_volume, trades count. "
                "Intervals: 1m,3m,5m,15m,30m,1h,2h,4h,6h,8h,12h,1d,3d,1w,1M. Max 1500 candles."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair, e.g. BTCUSDT, ETHUSDT, DOGEUSDT"},
                    "interval": {"type": "string", "default": "1h", "description": "Candle interval: 1m,5m,15m,1h,4h,1d, etc."},
                    "limit": {"type": "integer", "default": 200, "description": "Number of candles (max 1500)"},
                    "start_time": {"type": "integer", "description": "Start time in ms (optional)"},
                    "end_time": {"type": "integer", "description": "End time in ms (optional)"},
                },
                "required": ["symbol"],
            },
        ),
        Tool(
            name="get_multi_tf_klines",
            description=(
                "Fetch klines across multiple timeframes at once. "
                "Useful for multi-timeframe JAM analysis."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair"},
                    "intervals": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of intervals, e.g. ['5m','1h','4h']",
                    },
                    "limit": {"type": "integer", "default": 200},
                },
                "required": ["symbol", "intervals"],
            },
        ),
        Tool(
            name="get_orderbook",
            description=(
                "Fetch L2 order book depth from Binance Futures. "
                "Returns bids, asks, plus derived metrics: mid_price, spread (bps), "
                "depth ratio, top-10 imbalance, and wall detection (orders >3x median). "
                "Depths: 5,10,20,50,100,500,1000 levels."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair"},
                    "limit": {"type": "integer", "default": 100, "description": "Depth levels (5,10,20,50,100,500,1000)"},
                },
                "required": ["symbol"],
            },
        ),
        Tool(
            name="get_orderbook_light",
            description=(
                "Quick L2 snapshot: top 5 bids/asks + key metrics (spread, imbalance, walls). "
                "Lightweight version for rapid checks."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair, e.g. BTCUSDT"},
                    "depth": {"type": "integer", "default": 20, "description": "Order book depth to fetch (default 20)"},
                },
                "required": ["symbol"],
            },
        ),
        Tool(
            name="get_recent_trades",
            description="Fetch most recent trades (up to 1000). Shows price, qty, time, buyer/seller maker.",
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair, e.g. BTCUSDT"},
                    "limit": {"type": "integer", "default": 500, "description": "Number of trades (max 1000)"},
                },
                "required": ["symbol"],
            },
        ),
        Tool(
            name="get_agg_trades",
            description="Fetch aggregated trades with optional time range.",
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair, e.g. BTCUSDT"},
                    "limit": {"type": "integer", "default": 500, "description": "Number of trades (max 1000)"},
                    "start_time": {"type": "integer", "description": "Start timestamp in ms (optional)"},
                    "end_time": {"type": "integer", "description": "End timestamp in ms (optional)"},
                },
                "required": ["symbol"],
            },
        ),
        Tool(
            name="get_premium_index",
            description=(
                "Fetch premium index data from Binance: mark price, index price, estimated settle price, "
                "last funding rate, NEXT FUNDING TIME (countdown), and interest rate. "
                "If symbol is provided, returns data for that pair. If omitted, returns all symbols. "
                "Use this to know exactly when the next funding payment happens."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair, e.g. BTCUSDT. Omit for all symbols."},
                },
            },
        ),
        Tool(
            name="get_funding_rate",
            description="Fetch funding rate history. Shows funding rate and mark price per interval.",
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair, e.g. BTCUSDT"},
                    "limit": {"type": "integer", "default": 100, "description": "Number of funding entries"},
                    "start_time": {"type": "integer", "description": "Start timestamp in ms (optional)"},
                },
                "required": ["symbol"],
            },
        ),
        Tool(
            name="get_open_interest",
            description="Fetch current open interest, or historical OI with period parameter.",
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair, e.g. BTCUSDT"},
                    "historical": {"type": "boolean", "default": False, "description": "If true, fetch historical OI"},
                    "period": {"type": "string", "default": "1h", "description": "For historical: 5m,15m,30m,1h,2h,4h,6h,12h,1d"},
                    "limit": {"type": "integer", "default": 30, "description": "Number of data points"},
                },
                "required": ["symbol"],
            },
        ),
        Tool(
            name="get_long_short_ratio",
            description="Global long/short account ratio. Shows what % of accounts are long vs short.",
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair, e.g. BTCUSDT"},
                    "period": {"type": "string", "default": "1h", "description": "Period: 5m,15m,30m,1h,2h,4h,6h,12h,1d"},
                    "limit": {"type": "integer", "default": 30, "description": "Number of data points"},
                },
                "required": ["symbol"],
            },
        ),
        Tool(
            name="get_taker_volume",
            description="Taker buy/sell volume ratio. Measures aggressive buying vs selling pressure.",
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair, e.g. BTCUSDT"},
                    "period": {"type": "string", "default": "1h", "description": "Period: 5m,15m,30m,1h,2h,4h,6h,12h,1d"},
                    "limit": {"type": "integer", "default": 30, "description": "Number of data points"},
                },
                "required": ["symbol"],
            },
        ),
        Tool(
            name="get_ticker",
            description="24h ticker: price change, volume, high, low, trades count.",
            inputSchema={
                "type": "object",
                "properties": {"symbol": {"type": "string", "description": "Trading pair, e.g. BTCUSDT"}},
                "required": ["symbol"],
            },
        ),
        Tool(
            name="list_symbols",
            description="List available USDT-M perpetual futures symbols on Binance.",
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Optional: get info for specific symbol"},
                },
            },
        ),

        # ── L2 HISTORY TOOLS ──
        Tool(
            name="start_l2_recording",
            description=(
                "Start background recording of L2 order book snapshots for a symbol. "
                "Snapshots are saved to local SQLite DB at a configurable interval (default 30s). "
                "Recording continues in the background until explicitly stopped. "
                "Use this to build historical L2 depth data over time."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair, e.g. BTCUSDT"},
                    "interval_sec": {"type": "integer", "default": 30, "description": "Seconds between snapshots (min 5, default 30)"},
                    "depth": {"type": "integer", "default": 20, "description": "Order book depth levels to capture"},
                },
                "required": ["symbol"],
            },
        ),
        Tool(
            name="stop_l2_recording",
            description="Stop background L2 recording for a symbol.",
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair to stop recording"},
                },
                "required": ["symbol"],
            },
        ),
        Tool(
            name="get_l2_history",
            description=(
                "Query historical L2 order book snapshots from local storage. "
                "Returns time series of: mid_price, spread, depth_ratio, imbalance, walls. "
                "Each snapshot includes datetime (human-readable) and session_id. "
                "Filter by session_id to get data from a specific recording session. "
                "Use start_time/end_time (epoch ms) to filter range. "
                "Set metrics_only=true for compact output without individual price levels."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair"},
                    "session_id": {"type": "string", "description": "Filter by recording session (use list_l2_sessions to see available sessions)"},
                    "start_time": {"type": "integer", "description": "Start epoch ms (optional)"},
                    "end_time": {"type": "integer", "description": "End epoch ms (optional)"},
                    "limit": {"type": "integer", "default": 500, "description": "Max snapshots to return"},
                    "metrics_only": {"type": "boolean", "default": False, "description": "Only return metrics, omit bid/ask levels"},
                },
                "required": ["symbol"],
            },
        ),
        Tool(
            name="get_l2_recording_status",
            description=(
                "Check status of all L2 recordings: active tasks, stored symbols, "
                "snapshot counts, DB size, and time range covered."
            ),
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
        Tool(
            name="list_l2_sessions",
            description=(
                "List all L2 recording sessions (current and past). "
                "Shows session_id, symbol, start/stop dates (human-readable), "
                "duration, snapshot count, and whether still active. "
                "Use the session_id with get_l2_history to retrieve data from a specific session."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Filter sessions by symbol (optional)"},
                },
            },
        ),
        Tool(
            name="purge_l2_history",
            description=(
                "Delete old L2 snapshots to manage storage. "
                "Can filter by symbol and/or age (older_than_hours)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Symbol to purge (optional, all if omitted)"},
                    "older_than_hours": {"type": "number", "description": "Delete snapshots older than N hours"},
                },
            },
        ),

        # ── SCANNER TOOLS ──
        Tool(
            name="scan_altcoins",
            description=(
                "Scan altcoin USDT perpetuals for SHORT opportunities. "
                "Filters: 24h excursion > 2σ (statistical outlier) AND JAM pump_score ≥ 60%. "
                "Returns ranked list with: sigma, Langevin params, regime, short_analysis. "
                "IMPORTANT: Each candidate includes a SCORECARD in context.scorecards "
                "with 8 conditions from the historical elite trade profile, showing for each: "
                "status (✅ met, ⚠️ close, ❌ missing), actual value, target, and distance to ideal. "
                "Conditions: volume_spike (≥10x), pump_magnitude (≥30%), upper_wicks (rejection), "
                "taker_fading (buyers exhausted), regime_b (JAM confirmed), short_conviction (TA), "
                "reversal_started (off highs), multi_asset (diversification). "
                "Each candidate gets an elite_score (N/8, X%) and action_summary. "
                "ALWAYS present the scorecard to the user showing how close/far each candidate "
                "is from ideal trading conditions. Takes 15-30 seconds."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "top_n": {"type": "integer", "default": 20, "description": "Max number of opportunities to return (default 20)"},
                    "sigma_threshold": {"type": "number", "default": 2.0, "description": "Sigma threshold for 24h excursion filter (default 2.0 = only excursions > mean + 2σ)"},
                    "min_jam_score": {"type": "number", "default": 0.60, "description": "Minimum JAM pump_score to qualify as opportunity (default 0.60 = 60%)"},
                    "min_change_pct": {"type": "number", "default": 2.0, "description": "Minimum absolute 24h price change % (floor, default 2%)"},
                    "min_quote_volume": {"type": "number", "default": 5000000, "description": "Minimum 24h quote volume in USDT (default 5M)"},
                    "include_btc": {"type": "boolean", "default": False, "description": "Include BTCUSDT in scan results"},
                    "max_candidates": {"type": "integer", "default": 80, "description": "Max symbols to run JAM on (controls API calls)"},
                    "kline_interval": {"type": "string", "default": "1h", "description": "Timeframe for JAM klines (1h recommended)"},
                    "kline_limit": {"type": "integer", "default": 100, "description": "Candles to fetch per symbol for JAM"},
                },
            },
        ),
        Tool(
            name="scan_altcoins_quick",
            description=(
                "Quick altcoin scan — Phase 1 only (no JAM analysis). "
                "Returns all altcoins sorted by 24h pump intensity in ~1 second. "
                "Useful for a fast overview before running the full scan. "
                "Context flags mega-pumps (>30%) matching elite SINGLE-SHOT profile "
                "and multi-asset pump events for diversified shorting. "
                "No Langevin/JAM data, just ticker-based ranking."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "top_n": {"type": "integer", "default": 40, "description": "Number of results"},
                    "min_change_pct": {"type": "number", "default": 1.0, "description": "Minimum absolute 24h change %"},
                    "min_quote_volume": {"type": "number", "default": 1000000, "description": "Minimum 24h quote volume USDT"},
                    "include_btc": {"type": "boolean", "default": False, "description": "Include BTCUSDT in results"},
                },
            },
        ),

        # ── GLOBAL MARKET ──
        Tool(
            name="get_global_market",
            description=(
                "Global market snapshot oriented to SHORT TRADING. "
                "Aggregates: BTC status, market breadth, funding rates, long/short ratios, "
                "taker flow, OI, top pumps & dumps. "
                "Returns verdict: FAVORABLE / NEUTRAL / DESFAVORABLE for shorts. "
                "Use as FIRST call to assess market conditions before any trade."
            ),
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),

        # ── TECHNICAL ANALYSIS ──
        Tool(
            name="get_technical_analysis",
            description=(
                "COMPLETE multi-timeframe technical analysis. "
                "Computes ALL standard TA indicators across multiple timeframes simultaneously. "
                "Indicators per timeframe: "
                "TREND: SMA(7,20,50,100,200), EMA(7,20,50,100,200), MACD, ADX/DI+/DI-, Ichimoku Cloud, Supertrend. "
                "MOMENTUM: RSI, Stochastic, CCI, Williams %R, MFI, ROC. "
                "VOLATILITY: Bollinger Bands, ATR, Keltner Channel. "
                "VOLUME: OBV, VWAP, CMF, Volume Profile. "
                "S/R: Pivot Points, Fibonacci (retracement + extension). "
                "CANDLE PATTERNS: Doji, Hammer, Engulfing, Morning/Evening Star, etc. "
                "Each timeframe includes a BIAS SUMMARY aggregating all signals into a score (-1 to +1). "
                "Default timeframes: 5m, 15m, 1h, 4h, 1d."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair, e.g. BTCUSDT"},
                    "intervals": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Timeframes to analyze. Default: ['5m','15m','1h','4h','1d']",
                    },
                    "limit": {"type": "integer", "default": 200, "description": "Candles per timeframe (200 default, max 1500)"},
                },
                "required": ["symbol"],
            },
        ),

        # ── Ψ-JAM ANALYSIS ──
        Tool(
            name="analyze_kramers_moyal",
            description=(
                "Extract Kramers-Moyal coefficients D1 (drift), D2 (diffusion), D4 from price series. "
                "Reconstructs the effective Langevin potential V(x) and detects potential wells. "
                "Includes Pawula theorem check for Langevin validity. "
                "Fetches data automatically from Binance if symbol is provided."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair (fetches data automatically)"},
                    "interval": {"type": "string", "default": "1h", "description": "Candle timeframe"},
                    "limit": {"type": "integer", "default": 500, "description": "Number of candles"},
                    "bins": {"type": "integer", "default": 50, "description": "State space bins for KM estimation"},
                },
                "required": ["symbol"],
            },
        ),
        Tool(
            name="analyze_hurst",
            description=(
                "Compute Hurst exponent via R/S analysis. "
                "H<0.5: anti-persistent (mean-reverting), H=0.5: random walk, H>0.5: persistent (trending). "
                "Critical for detecting regime type in JAM framework."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair, e.g. BTCUSDT"},
                    "interval": {"type": "string", "default": "1h", "description": "Candle timeframe"},
                    "limit": {"type": "integer", "default": 500, "description": "Number of candles"},
                },
                "required": ["symbol"],
            },
        ),
        Tool(
            name="analyze_lyapunov",
            description=(
                "Estimate maximum Lyapunov exponent (Rosenstein method). "
                "λ>0: chaotic dynamics, λ≈0: marginally stable, λ<0: stable. "
                "Detects sensitivity to initial conditions — key for crash prediction."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair, e.g. BTCUSDT"},
                    "interval": {"type": "string", "default": "1h", "description": "Candle timeframe"},
                    "limit": {"type": "integer", "default": 500, "description": "Number of candles"},
                    "embedding_dim": {"type": "integer", "default": 5, "description": "Phase space embedding dimension"},
                    "tau": {"type": "integer", "default": 1, "description": "Time delay for embedding"},
                },
                "required": ["symbol"],
            },
        ),
        Tool(
            name="analyze_rqa",
            description=(
                "Recurrence Quantification Analysis. Key measures: "
                "LAM (laminarity) — most sensitive pre-crash indicator, "
                "DET (determinism), ENTR (entropy), TT (trapping time). "
                "High LAM = system approaching frozen/jammed state."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair, e.g. BTCUSDT"},
                    "interval": {"type": "string", "default": "1h", "description": "Candle timeframe"},
                    "limit": {"type": "integer", "default": 500, "description": "Number of candles"},
                    "embedding_dim": {"type": "integer", "default": 3, "description": "Embedding dimension for delay vectors"},
                    "threshold_pct": {"type": "number", "default": 10.0, "description": "Recurrence threshold as % of std"},
                },
                "required": ["symbol"],
            },
        ),
        Tool(
            name="analyze_vpin",
            description=(
                "Compute VPIN (Volume-synchronized Probability of Informed Trading) "
                "using real aggTrades with isBuyerMaker for accurate buy/sell classification. "
                "Measures order flow toxicity — probability of adverse selection. "
                "High VPIN = informed traders active, potential for sharp moves."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair, e.g. BTCUSDT"},
                    "n_trades": {"type": "integer", "default": 5000, "description": "Number of aggTrades to fetch (more = deeper VPIN history)"},
                    "n_buckets": {"type": "integer", "default": 50, "description": "Rolling window size (number of volume buckets averaged per VPIN value)"},
                },
                "required": ["symbol"],
            },
        ),
        Tool(
            name="analyze_kyles_lambda",
            description=(
                "Estimate Kyle's lambda (price impact coefficient). "
                "ΔP = λ × SignedVolume + ε. Higher λ = less liquid, more adverse selection."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair, e.g. BTCUSDT"},
                    "interval": {"type": "string", "default": "5m", "description": "Candle timeframe (5m recommended)"},
                    "limit": {"type": "integer", "default": 500, "description": "Number of candles"},
                },
                "required": ["symbol"],
            },
        ),
        Tool(
            name="analyze_jam_regime",
            description=(
                "Full JAM regime classification using Langevin physics. "
                "Classifies current state as: "
                "Régimen A (impulso sostenido: F_ext domina, γ bajo), "
                "Régimen B (pump fallido: energía disipada, κ restaura), "
                "Neutral (sin impulso significativo). "
                "Uses volume ratio, delta, retention, absorption thresholds."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair, e.g. BTCUSDT"},
                    "interval": {"type": "string", "default": "1h", "description": "Candle timeframe"},
                    "limit": {"type": "integer", "default": 200, "description": "Number of candles"},
                    "window": {"type": "integer", "default": 20, "description": "Rolling window for metrics"},
                },
                "required": ["symbol"],
            },
        ),
        Tool(
            name="full_jam_pipeline",
            description=(
                "🔬 COMPLETE Ψ-JAM ANALYSIS PIPELINE. "
                "Runs ALL indicators: Kramers-Moyal, Hurst, Lyapunov, RQA, VPIN, JAM regime. "
                "Returns composite risk score and full Langevin system characterization. "
                "This is the equivalent of 'analizame X' — the full 8+1 section analysis."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair to analyze"},
                    "interval": {"type": "string", "default": "1h", "description": "Primary timeframe"},
                    "limit": {"type": "integer", "default": 500, "description": "Number of candles"},
                },
                "required": ["symbol"],
            },
        ),

        # ── OI LEVEL (OIED) ──
        Tool(
            name="oi_level",
            description=(
                "OI Level — OIED (Open Interest Energy Divergence). "
                "Classifies OI state into 5 Langevin energy regimes: "
                "ENERGY_TRAP (⚠ longs trapped, crash precursor ~36h), "
                "ENERGY_DISSIPATION (⚠ energy draining), "
                "ENERGY_DELEVERAGING (crash/liquidation active), "
                "ENERGY_INFLOW (capital entering, bullish), "
                "ENERGY_RECOVERY (price rising, positions closing). "
                "Also detects ROC divergences and swing divergences (OI vs price). "
                "Generates interactive HTML chart. "
                "Use AFTER confirming a candidate to check OI energy traps before entry."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {
                        "type": "string",
                        "description": "Par de futuros, e.g. BTCUSDT, ETHUSDT",
                    },
                    "period": {
                        "type": "string", "default": "1h",
                        "description": (
                            "Periodo del OI histórico. Valores: 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d. "
                            "Periodos más cortos dan más resolución pero menos historia."
                        ),
                    },
                    "limit": {
                        "type": "integer", "default": 100,
                        "description": "Cantidad de velas/puntos OI a analizar (max ~500)",
                    },
                    "divergence_window": {
                        "type": "integer", "default": 6,
                        "description": (
                            "Ventana (en periodos) para calcular ROC y clasificar estados energéticos. "
                            "6 periodos en 1h = 6 horas de lookback."
                        ),
                    },
                    "show_volume": {
                        "type": "boolean", "default": True,
                        "description": "Incluir panel de volumen coloreado por Taker Buy Ratio",
                    },
                    "show_funding": {
                        "type": "boolean", "default": True,
                        "description": "Incluir panel de funding rate",
                    },
                    "annotate": {
                        "type": "boolean", "default": True,
                        "description": "Anotar swing divergences directamente en el gráfico",
                    },
                    "include_series": {
                        "type": "boolean", "default": False,
                        "description": (
                            "Incluir arrays crudos de series temporales en la respuesta. "
                            "Solo activar si se necesita para rendering externo (payload grande)."
                        ),
                    },
                },
                "required": ["symbol"],
            },
        ),

        # ── LIQUIDATION CLUSTERS ──
        Tool(
            name="get_liquidation_clusters",
            description=(
                "Calculate liquidation price clusters based on historical positions. "
                "Estimates where liquidations are concentrated by analyzing: "
                "1. Price zones with high trading volume (likely entry points) "
                "2. Common leverage levels (2x, 3x, 5x, 10x, 20x, 25x, 50x, 75x, 100x) "
                "3. Current OI distribution "
                "Returns: LONG liquidation levels (below current price), "
                "SHORT liquidation levels (above current price), "
                "CASCADE RISK zones (where multiple leverages converge → potential cascade). "
                "USE CASE: Before shorting, check where long liquidations cluster. "
                "If price approaching a cascade zone, expect accelerated moves. "
                "⚠️ These are ESTIMATES based on volume analysis, not actual position data."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair, e.g. BTCUSDT, ETHUSDT"},
                    "interval": {
                        "type": "string", "default": "1h",
                        "description": "Kline interval for volume analysis (1h recommended for reliable zones)",
                    },
                    "limit": {
                        "type": "integer", "default": 168,
                        "description": "Number of candles to analyze (168 = 1 week of 1h candles)",
                    },
                    "leverage_levels": {
                        "type": "array",
                        "items": {"type": "integer"},
                        "description": "Custom leverage levels to analyze (default: [2,3,5,10,20,25,50,75,100])",
                    },
                },
                "required": ["symbol"],
            },
        ),

        # ── RANGE ASYMMETRY ──
        Tool(
            name="analyze_range_asymmetry",
            description=(
                "Evaluate R:R asymmetry for a SHORT (or LONG) entry using historical cycles. "
                "Answers: 'At $X, is the downside potential > upside risk?' "
                "8 sections: cycle detection, retrace stats, R:R ratio, win rate, "
                "funding carry, velocity/timing, volume structure, composite score (0-100). "
                "Verdict: ELITE SHORT (90+) / STRONG / MODERATE / WEAK / POOR / AVOID. "
                "Use BEFORE opening a position to validate asymmetry."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair, e.g. VVVUSDT, ETHUSDT"},
                    "entry_price": {"type": "number", "description": "Proposed entry price. Uses current price if omitted."},
                    "side": {
                        "type": "string", "enum": ["SHORT", "LONG"], "default": "SHORT",
                        "description": "Trade direction: SHORT (default) or LONG",
                    },
                    "timeframe": {
                        "type": "string", "default": "1d",
                        "description": "Timeframe for cycle detection: '1d' for swing (default), '1h' for intraday",
                    },
                    "pump_threshold": {
                        "type": "number", "default": 5.0,
                        "description": "Min % move to consider as pump/dump in zigzag detection (default 5%)",
                    },
                },
                "required": ["symbol"],
            },
        ),

        # ── TRADING TOOLS (require API keys, support multi-account: principal/copytrading) ──
        Tool(
            name="futures_list_accounts",
            description=(
                "List available Binance Futures trading accounts. "
                "Shows account names that can be used with the 'account' parameter "
                "in all trading tools. Currently configured: principal, copytrading."
            ),
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
        Tool(
            name="futures_get_balance",
            description=(
                "Get Binance Futures account balance. "
                "Returns USDT total balance, available balance, and unrealized PnL. "
                "Requires API keys in .env file. "
                "IMPORTANT: specify 'account' to choose which account (principal or copytrading)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "account": {"type": "string", "enum": ["principal", "copytrading"], "description": "Which account to use: 'principal' or 'copytrading'. ALWAYS ask the user which account before calling."},
                },
                "required": ["account"],
            },
        ),
        Tool(
            name="futures_get_positions",
            description=(
                "Get open Binance Futures positions with full details: "
                "side (LONG/SHORT), size, entry price, mark price, notional USDT, "
                "leverage, margin type, unrealized PnL, ROE%, "
                "liquidation price, and distance to liquidation %. "
                "Optionally filter by symbol. "
                "IMPORTANT: specify 'account' to choose which account (principal or copytrading)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Filter by trading pair (optional, shows all if omitted)"},
                    "account": {"type": "string", "enum": ["principal", "copytrading"], "description": "Which account to use: 'principal' or 'copytrading'. ALWAYS ask the user which account before calling."},
                },
                "required": ["account"],
            },
        ),
        Tool(
            name="futures_get_performance",
            description=(
                "Get trading performance summary for a Binance Futures account. "
                "Returns: net PnL, realized PnL, funding costs, commissions, ROI%, "
                "win/loss stats (win rate, profit factor, expectancy, largest win/loss), "
                "max drawdown, best/worst day, breakdown by symbol and by day. "
                "IMPORTANT: specify 'account' to choose which account (principal or copytrading)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "days": {"type": "integer", "description": "Lookback period in days (default 30, max 90)"},
                    "symbol": {"type": "string", "description": "Filter by trading pair (optional, shows all if omitted)"},
                    "account": {"type": "string", "enum": ["principal", "copytrading"], "description": "Which account to use: 'principal' or 'copytrading'. ALWAYS ask the user which account before calling."},
                },
                "required": ["account"],
            },
        ),
        Tool(
            name="futures_get_income_history",
            description=(
                "Get individual income records from Binance Futures account. "
                "Returns each funding fee payment, realized PnL event, commission, etc. "
                "with exact timestamp, amount, and symbol. "
                "Use income_type filter: FUNDING_FEE, REALIZED_PNL, COMMISSION, TRANSFER. "
                "IMPORTANT: specify 'account' to choose which account (principal or copytrading)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "income_type": {"type": "string", "enum": ["FUNDING_FEE", "REALIZED_PNL", "COMMISSION", "TRANSFER"], "description": "Filter by income type (optional, shows all if omitted)"},
                    "symbol": {"type": "string", "description": "Filter by trading pair (optional, shows all if omitted)"},
                    "days": {"type": "integer", "description": "Lookback period in days (default 7, max 90)"},
                    "account": {"type": "string", "enum": ["principal", "copytrading"], "description": "Which account to use: 'principal' or 'copytrading'. ALWAYS ask the user which account before calling."},
                },
                "required": ["account"],
            },
        ),
        Tool(
            name="futures_evaluate_exit",
            description=(
                "Evaluate whether to EXIT open SHORT positions based on elite exit rules. "
                "Analyses each open short: holding time, funding efficiency, volume collapse, "
                "green candle signals, taker buy flip, pump capture %, consolidation. "
                "Returns verdict (EXIT_NOW / EXIT_SOON / MONITOR / HOLD) with decision tree. "
                "Based on proven patterns: 8h window, 40-60% pump capture, funding <15% PnL. "
                "Use this WHENEVER asking about exits, closing positions, or checking position health. "
                "IMPORTANT: specify 'account' to choose which account (principal or copytrading)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Evaluate specific symbol only (optional, evaluates all open shorts if omitted)"},
                    "account": {"type": "string", "enum": ["principal", "copytrading"], "description": "Which account to use: 'principal' or 'copytrading'. ALWAYS ask the user which account before calling."},
                },
                "required": ["account"],
            },
        ),
        Tool(
            name="futures_set_leverage",
            description=(
                "Set leverage for a symbol on Binance Futures (1-125x). "
                "Must be set BEFORE opening a position. "
                "Returns confirmed leverage and max notional value. "
                "IMPORTANT: specify 'account' to choose which account (principal or copytrading)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair, e.g. BTCUSDT"},
                    "leverage": {"type": "integer", "description": "Leverage multiplier (1-125)"},
                    "account": {"type": "string", "enum": ["principal", "copytrading"], "description": "Which account to use: 'principal' or 'copytrading'. ALWAYS ask the user which account before calling."},
                },
                "required": ["symbol", "leverage", "account"],
            },
        ),
        Tool(
            name="futures_open_position",
            description=(
                "Open a LONG or SHORT position on Binance Futures. "
                "Supports MARKET and LIMIT order types. "
                "For shorts: use side='SHORT'. For longs: side='LONG'. "
                "If leverage is specified, it will be set before opening. "
                "NOW SUPPORTS setting take_profit, stop_loss, and trailing_stop at order creation time. "
                "For MARKET orders: TP/SL/trailing are placed immediately after fill. "
                "For LIMIT orders: TP/SL are pre-placed as conditional orders. "
                "TRAILING STOP: callbackRate (0.1–5.0%) determines how far price must retrace to trigger. "
                "IMPORTANT: specify 'account' to choose which account (principal or copytrading). "
                "IMPORTANT: double-check symbol, side, quantity, leverage AND account before calling."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair, e.g. ETHUSDT"},
                    "side": {"type": "string", "enum": ["LONG", "SHORT"], "description": "Position direction"},
                    "quantity": {"type": "number", "description": "Amount in base asset (e.g. 0.01 ETH)"},
                    "order_type": {"type": "string", "enum": ["MARKET", "LIMIT"], "default": "MARKET", "description": "Order type"},
                    "price": {"type": "number", "description": "Limit price (required for LIMIT orders)"},
                    "leverage": {"type": "integer", "description": "Set leverage before opening (optional, 1-125)"},
                    "take_profit": {"type": "number", "description": "Take-profit trigger price (optional). LONG: must be above entry. SHORT: must be below entry."},
                    "stop_loss": {"type": "number", "description": "Stop-loss trigger price (optional). LONG: must be below entry. SHORT: must be above entry."},
                    "trailing_stop_callback": {"type": "number", "description": "Trailing stop callback rate in % (0.1–5.0). Price must retrace this % from peak to trigger."},
                    "trailing_stop_activation": {"type": "number", "description": "Activation price for trailing stop (optional). If omitted, activates immediately."},
                    "account": {"type": "string", "enum": ["principal", "copytrading"], "description": "Which account to use: 'principal' or 'copytrading'. ALWAYS ask the user which account before calling."},
                },
                "required": ["symbol", "side", "quantity", "account"],
            },
        ),
        Tool(
            name="futures_open_stop_limit",
            description=(
                "Open a STOP-LIMIT entry order on Binance Futures. "
                "The order activates when mark price reaches the stop (trigger) price, "
                "then places a limit order at the specified limit price. "
                "Use for conditional entries: e.g., buy LONG only if price breaks above X (stop), "
                "with limit at Y. Or open SHORT only if price drops below X. "
                "Supports optional take_profit and stop_loss pre-placement. "
                "IMPORTANT: specify 'account' to choose which account (principal or copytrading). "
                "IMPORTANT: double-check all prices, side, quantity, leverage AND account before calling."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair, e.g. ETHUSDT"},
                    "side": {"type": "string", "enum": ["LONG", "SHORT"], "description": "Position direction"},
                    "quantity": {"type": "number", "description": "Amount in base asset (e.g. 0.01 ETH)"},
                    "stop_price": {"type": "number", "description": "Trigger price: when mark price reaches this level, the limit order activates"},
                    "price": {"type": "number", "description": "Limit price: the price of the limit order placed after stop triggers"},
                    "leverage": {"type": "integer", "description": "Set leverage before opening (optional, 1-125)"},
                    "take_profit": {"type": "number", "description": "Take-profit trigger price (optional)"},
                    "stop_loss": {"type": "number", "description": "Stop-loss trigger price (optional)"},
                    "account": {"type": "string", "enum": ["principal", "copytrading"], "description": "Which account to use: 'principal' or 'copytrading'. ALWAYS ask the user which account before calling."},
                },
                "required": ["symbol", "side", "quantity", "stop_price", "price", "account"],
            },
        ),
        Tool(
            name="futures_close_position",
            description=(
                "Close an open Binance Futures position (full or partial). "
                "If quantity is omitted, closes the FULL position. "
                "Supports MARKET and LIMIT closes. "
                "Automatically detects position side and sends the opposite order. "
                "IMPORTANT: specify 'account' to choose which account (principal or copytrading)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair"},
                    "quantity": {"type": "number", "description": "Amount to close (omit for full close)"},
                    "order_type": {"type": "string", "enum": ["MARKET", "LIMIT"], "default": "MARKET", "description": "Order type: MARKET (default) or LIMIT"},
                    "price": {"type": "number", "description": "Limit price (required for LIMIT closes)"},
                    "account": {"type": "string", "enum": ["principal", "copytrading"], "description": "Which account to use: 'principal' or 'copytrading'. ALWAYS ask the user which account before calling."},
                },
                "required": ["symbol", "account"],
            },
        ),
        Tool(
            name="futures_set_tp_sl",
            description=(
                "Set take-profit, stop-loss, and/or trailing stop for an open Binance Futures position. "
                "Uses TAKE_PROFIT_MARKET, STOP_MARKET, and TRAILING_STOP_MARKET orders (triggered by mark price). "
                "Automatically uses the Algo Order API when the standard endpoint is not "
                "supported for a symbol (error -4120). "
                "All are reduceOnly orders. "
                "SUPPORTS MULTIPLE TP LEVELS via 'take_profits' array for scaled exits. "
                "Example: take_profits=[{price: 2000, quantity_pct: 50}, {price: 2100, quantity_pct: 30}] "
                "closes 50% at 2000 and 30% at 2100. Use 'take_profit' for a single TP or 'take_profits' for multiple. "
                "TRAILING STOP: callbackRate (0.1–5.0%) determines how far price must retrace to trigger. "
                "IMPORTANT: specify 'account' to choose which account (principal or copytrading)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair"},
                    "take_profit": {"type": "number", "description": "Single take-profit trigger price (use take_profits for multiple levels)"},
                    "take_profits": {
                        "type": "array",
                        "description": "Multiple TP levels for scaled exits. Each entry has 'price' and 'quantity_pct' (% of position to close). Sum of quantity_pct must be ≤100.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "price": {"type": "number", "description": "TP trigger price"},
                                "quantity_pct": {"type": "number", "description": "% of position to close at this level (1-100)"},
                            },
                            "required": ["price", "quantity_pct"],
                        },
                    },
                    "stop_loss": {"type": "number", "description": "Stop-loss trigger price"},
                    "tp_quantity": {"type": "number", "description": "TP quantity for single take_profit (default: full position)"},
                    "sl_quantity": {"type": "number", "description": "SL quantity (default: full position)"},
                    "trailing_stop_callback": {"type": "number", "description": "Trailing stop callback rate in % (0.1–5.0). E.g. 1.0 means price must retrace 1% from peak to trigger."},
                    "trailing_stop_activation": {"type": "number", "description": "Activation price for trailing stop (optional). If omitted, trailing activates immediately at current price."},
                    "trailing_stop_quantity": {"type": "number", "description": "Trailing stop quantity (default: full position)"},
                    "account": {"type": "string", "enum": ["principal", "copytrading"], "description": "Which account to use: 'principal' or 'copytrading'. ALWAYS ask the user which account before calling."},
                },
                "required": ["symbol", "account"],
            },
        ),
        Tool(
            name="futures_get_open_orders",
            description=(
                "Get all open orders on Binance Futures (standard + algo/conditional). "
                "Shows pending limit orders, TP/SL orders (including algo conditional orders). "
                "Optionally filter by symbol. "
                "IMPORTANT: specify 'account' to choose which account (principal or copytrading)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Filter by symbol (optional)"},
                    "account": {"type": "string", "enum": ["principal", "copytrading"], "description": "Which account to use: 'principal' or 'copytrading'. ALWAYS ask the user which account before calling."},
                },
                "required": ["account"],
            },
        ),
        Tool(
            name="futures_cancel_order",
            description="Cancel a specific open order by order_id (supports both standard and algo orders). Specify 'account' to choose which account.",
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair"},
                    "order_id": {"type": "integer", "description": "Order ID to cancel"},
                    "account": {"type": "string", "enum": ["principal", "copytrading"], "description": "Which account to use: 'principal' or 'copytrading'. ALWAYS ask the user which account before calling."},
                },
                "required": ["symbol", "order_id", "account"],
            },
        ),
        Tool(
            name="futures_cancel_all_orders",
            description="Cancel ALL open orders for a symbol (standard + algo conditional). Use with caution. Specify 'account' to choose which account.",
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair"},
                    "account": {"type": "string", "enum": ["principal", "copytrading"], "description": "Which account to use: 'principal' or 'copytrading'. ALWAYS ask the user which account before calling."},
                },
                "required": ["symbol", "account"],
            },
        ),

        # ── BASIS / ARBITRAGE TOOLS ──
        Tool(
            name="scan_basis",
            description=(
                "Full scan of spot-futures basis across 50+ USDT pairs. "
                "Returns ranked opportunities with score (0-100), basis %, annualized return, "
                "funding carry, fee-adjusted profit, and strategy recommendation. "
                "Phase 1: fast ticker screen. Phase 2: deep analysis with funding rates. "
                "Takes ~10-15 seconds."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "min_volume_usd": {"type": "number", "description": "Minimum 24h volume in USD (default: 1000000)"},
                    "min_basis_pct": {"type": "number", "description": "Minimum absolute basis % to include (default: 0.05)"},
                    "top_n": {"type": "integer", "description": "Number of top results to return (default: 20)"},
                },
            },
        ),
        Tool(
            name="scan_basis_quick",
            description=(
                "Quick scan of spot-futures basis — Phase 1 only (2-3 API calls). "
                "Returns raw basis % for all USDT pairs without funding or scoring. "
                "Good for a fast overview. Takes ~2 seconds."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "min_volume_usd": {"type": "number", "description": "Minimum 24h volume in USD (default: 1000000)"},
                    "top_n": {"type": "integer", "description": "Number of results to return (default: 30)"},
                },
            },
        ),
        Tool(
            name="get_basis",
            description=(
                "Get current spot-futures basis for a single symbol with full analysis: "
                "basis %, annualized, funding rate, fee-adjusted profit per strategy, "
                "liquidity score, and recommendation."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair, e.g. BTCUSDT"},
                },
                "required": ["symbol"],
            },
        ),
        Tool(
            name="get_basis_history",
            description=(
                "Get historical basis (spot vs futures) over a time range. "
                "Useful for understanding how the basis evolves, mean-reversion patterns, "
                "and identifying entry/exit timing for basis trades."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair, e.g. BTCUSDT"},
                    "interval": {"type": "string", "description": "Kline interval (default: 1h). Options: 1m,5m,15m,1h,4h,1d"},
                    "limit": {"type": "integer", "description": "Number of candles (default: 168 = 7 days at 1h)"},
                },
                "required": ["symbol"],
            },
        ),
        Tool(
            name="prepare_basis_trade",
            description=(
                "Prepare a basis/arbitrage trade proposal for review. "
                "Creates a hedged position plan (buy spot + short futures, or vice versa) "
                "with exact sizing, fee estimates, and profit scenarios. "
                "Returns a proposal_id to pass to execute_basis_trade after user review. "
                "ALWAYS show the proposal to the user before executing."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair, e.g. BTCUSDT"},
                    "capital_usd": {"type": "number", "description": "Total capital to allocate in USD"},
                    "strategy": {"type": "string", "enum": ["cash_and_carry", "reverse_cash_and_carry", "funding_arb"], "description": "Strategy type"},
                    "account": {"type": "string", "enum": ["principal", "copytrading"], "description": "Which account to use. ALWAYS ask the user."},
                },
                "required": ["symbol", "capital_usd", "strategy", "account"],
            },
        ),
        Tool(
            name="execute_basis_trade",
            description=(
                "Execute a previously prepared basis trade proposal. "
                "Places both legs (spot + futures) simultaneously. "
                "The proposal_id comes from prepare_basis_trade. "
                "ONLY call this after the user has reviewed and approved the proposal."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "proposal_id": {"type": "string", "description": "The proposal ID from prepare_basis_trade"},
                    "account": {"type": "string", "enum": ["principal", "copytrading"], "description": "Which account (must match the proposal)"},
                },
                "required": ["proposal_id", "account"],
            },
        ),
        Tool(
            name="close_basis_trade",
            description=(
                "Close an open basis/arbitrage position (both legs). "
                "Unwinds the hedged position by selling spot and closing the futures position."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "position_id": {"type": "string", "description": "The position ID from list_basis_positions"},
                    "account": {"type": "string", "enum": ["principal", "copytrading"], "description": "Which account"},
                },
                "required": ["position_id", "account"],
            },
        ),
        Tool(
            name="list_basis_positions",
            description=(
                "List all open basis/arbitrage positions with live PnL, "
                "current basis vs entry basis, funding accumulated, and status."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "account": {"type": "string", "enum": ["principal", "copytrading"], "description": "Which account"},
                },
                "required": ["account"],
            },
        ),
        Tool(
            name="get_basis_dashboard",
            description=(
                "Full dashboard of the basis trading system: open positions, "
                "pending proposals, total PnL, capital usage, and market overview."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "account": {"type": "string", "enum": ["principal", "copytrading"], "description": "Which account"},
                },
                "required": ["account"],
            },
        ),

        # ── CARRY DETECTOR (Funding Rate Carry Trades) ──
        Tool(
            name="scan_carry",
            description=(
                "Full scan for funding rate carry opportunities. "
                "Detects tokens with persistent negative funding (<-0.5%) where shorts are "
                "paying longs. Confirms price is consolidating (not in freefall) via ATR analysis. "
                "Scores opportunities by persistence, consolidation, yield, and cascade risk. "
                "Phase 1: fast ticker screen + premiumIndex. Phase 2: deep analysis with "
                "funding history + klines + OI. Takes ~10-20 seconds."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "min_volume_usd": {"type": "number", "description": "Minimum 24h volume in USD (default: 5000000)"},
                    "min_funding_pct": {"type": "number", "description": "Maximum funding rate % to consider, e.g. -0.5 (default: -0.5)"},
                    "min_persistence": {"type": "integer", "description": "Minimum consecutive negative funding intervals (default: 3)"},
                    "top_n": {"type": "integer", "description": "Number of top results to return (default: 15)"},
                },
            },
        ),
        Tool(
            name="scan_carry_quick",
            description=(
                "Quick snapshot of tokens with extreme negative funding — Phase 1 only. "
                "No persistence check or scoring. Shows current funding rate for all liquid pairs. "
                "Good for a fast overview before running full scan. Takes ~2 seconds."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "min_volume_usd": {"type": "number", "description": "Minimum 24h volume in USD (default: 5000000)"},
                    "min_funding_pct": {"type": "number", "description": "Maximum funding rate % to consider (default: -0.5)"},
                    "top_n": {"type": "integer", "description": "Number of results to return (default: 30)"},
                },
            },
        ),
        Tool(
            name="analyze_carry",
            description=(
                "Deep carry analysis for a single symbol. Returns full breakdown: "
                "funding persistence (consecutive intervals, acceleration, trend), "
                "price consolidation (ATR, volatility, drawdown), "
                "cascade risk assessment (OI crowding, volume trend), "
                "carry profitability (daily/weekly/annualized yield, net after price drift, SL recommendation), "
                "and composite score with verdict (CARRY_EXCELENTE / CARRY_VIABLE / CARRY_MARGINAL / NO_CARRY). "
                "Works even if the symbol doesn't meet scanner thresholds."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair, e.g. KITEUSDT"},
                },
                "required": ["symbol"],
            },
        ),
        Tool(
            name="monitor_carry_exit",
            description=(
                "Monitor an open carry trade for exit signals. "
                "Checks if funding is normalizing (becoming less negative) which means "
                "the carry is ending. Returns action: MANTENER / PREPARAR_SALIDA / CERRAR_POSICION. "
                "Key signals: FUNDING_NORMALIZADO, FUNDING_REDUCIDO, FUNDING_REVIRTIENDO, CARRY_ACTIVO."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "The pair being carried, e.g. KITEUSDT"},
                    "entry_funding_pct": {"type": "number", "description": "The avg funding % when the trade was entered (e.g. -1.2)"},
                },
                "required": ["symbol", "entry_funding_pct"],
            },
        ),

        # ── GHOST FLOW (Pseudo-L3) ──
        Tool(
            name="analyze_ghost_flow",
            description=(
                "Ψ-JAM Section 7d: Pseudo-L3 Ghost Flow. "
                "Estimates hidden directional intent by cross-referencing two L2 snapshots "
                "with aggregated trades in the window between them. "
                "Classifies each price level as GHOST (cancelled), FILLED (executed), "
                "ICEBERG (replenished), or MIXED. "
                "Outputs: GAS (Ghost Asymmetry Score), DLI (Directional Liquidity Imbalance), "
                "iceberg detection with hidden volume estimation, real support/resistance levels, "
                "and a directional verdict (UP/DOWN/NEUTRAL) with confidence. "
                "Makes 4 API calls with a configurable delay between snapshots (default 15s). "
                "Use to detect spoofing, ghost liquidity, and hidden iceberg orders."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading pair, e.g. BTCUSDT, HUSDT"},
                    "delay_seconds": {"type": "number", "default": 15, "description": "Seconds between snapshots (10-30). 10 for high vol, 15 default, 20-30 for low activity."},
                    "depth": {"type": "integer", "default": 20, "description": "Order book depth levels per side (5,10,20,50,100)"},
                    "trades_limit": {"type": "integer", "default": 1000, "description": "Max trades to fetch per API call (max 1000)"},
                    "ghost_threshold": {"type": "number", "default": 0.7, "description": "Fill rate below which a level is classified GHOST (0-1)"},
                    "iceberg_threshold": {"type": "number", "default": 1.5, "description": "Replenishment ratio above which a level is ICEBERG"},
                },
                "required": ["symbol"],
            },
        ),
        # ── STRATEGY GUARD (entry evaluation + background watchdog) ──
        Tool(
            name="evaluate_short_entry",
            description=(
                "Evalúa las condiciones de entrada para un short (R0-R4). "
                "Devuelve 🟢 GO / 🟡 WAIT / 🔴 NO_TRADE con checklist detallada y sizing. "
                "Chequea: R0 mercado global, R1 JAM régimen, R2 OI capitulación, "
                "R3 funding rate, R4 sizing (1% riesgo). "
                "NUNCA abre la posición — solo evalúa. "
                "Cuando el resultado es GO, ANTES de abrir preguntar al usuario: "
                "'¿Querés que el trade se autogestione (cierre automático por funding/time stop)?'. "
                "Después de abrir con futures_open_position, llamar activate_guard. "
                "IMPORTANT: specify 'account' to choose which account (principal or copytrading)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Par de trading, ej: ORCAUSDT"},
                    "account": {"type": "string", "enum": ["principal", "copytrading"], "description": "Which account to use. ALWAYS ask the user which account before calling."},
                    "sl_percent": {"type": "number", "default": 5, "description": "Stop loss en porcentaje (default 5%)"},
                },
                "required": ["symbol", "account"],
            },
        ),
        Tool(
            name="activate_guard",
            description=(
                "Activa el watchdog automático para una posición SHORT abierta. "
                "Monitorea en background cada 5 minutos, independiente del chat. "
                "Reglas monitoreadas: R5 SL colocado, R6 funding adverso, R7 time stop, R8 no-DCA. "
                "Con auto_close=true, el MCP cierra automáticamente la posición si: "
                "funding adverso > 0.10%%/h (R6) o time stop excedido (R7). "
                "Llamar DESPUÉS de abrir la posición con futures_open_position. "
                "IMPORTANT: preguntar al usuario si quiere auto_close=true ANTES de activar. "
                "IMPORTANT: specify 'account' to choose which account (principal or copytrading)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Par de trading"},
                    "account": {"type": "string", "enum": ["principal", "copytrading"], "description": "Which account to use. ALWAYS ask the user which account before calling."},
                    "auto_close": {"type": "boolean", "default": False, "description": "Si true, cierra automáticamente por R6/R7. Si false, solo alerta."},
                    "entry_price": {"type": "number", "description": "Precio de entrada (auto-detecta si se omite)"},
                    "sl_price": {"type": "number", "description": "Precio de stop loss"},
                    "max_hold_hours": {"type": "number", "description": "Override del time stop (default 12h, 24h si en profit)"},
                },
                "required": ["symbol", "account"],
            },
        ),
        Tool(
            name="guard_status",
            description=(
                "Muestra el estado del watchdog: posiciones monitoreadas, alertas activas, "
                "cierres ejecutados, y log reciente del guard. "
                "Usar para revisar qué está haciendo el monitoreo automático."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "account": {"type": "string", "enum": ["principal", "copytrading"], "description": "Filtrar por cuenta (opcional, muestra todas si se omite)"},
                },
            },
        ),

        # ── COPY-TRADING ANALYSIS ──
        Tool(
            name="search_top_copytraders",
            description=(
                "Busca los mejores copy-traders de Binance Futures. "
                "Devuelve ranking paginado con ROI, PnL, win rate, AUM, MDD, Sharpe ratio, "
                "copiers actuales y slots disponibles. "
                "Permite ordenar por ROI/PNL/WIN_RATE/MDD/AUM, filtrar por nombre, "
                "y elegir período (7D/30D/90D). "
                "En la primera página también incluye 'daily_picks' (selección diaria de Binance). "
                "Usa Playwright (headless browser) — la primera llamada tarda más por el warmup."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "page_number": {"type": "integer", "default": 1, "description": "Página (1-indexed)"},
                    "page_size": {"type": "integer", "default": 18, "description": "Resultados por página (max 30, hard cap de Binance)"},
                    "time_range": {"type": "string", "enum": ["7D", "30D", "90D"], "default": "30D", "description": "Período de análisis"},
                    "sort_by": {"type": "string", "enum": ["ROI", "PNL", "WIN_RATE", "MDD", "AUM", "COPIER_PNL"], "default": "ROI", "description": "Campo de ordenamiento"},
                    "order": {"type": "string", "enum": ["DESC", "ASC"], "default": "DESC", "description": "Dirección del orden"},
                    "nickname": {"type": "string", "description": "Buscar por nombre del trader (match parcial)"},
                    "hide_full": {"type": "boolean", "default": False, "description": "Ocultar traders sin slots disponibles"},
                    "portfolio_type": {"type": "string", "enum": ["ALL", "PUBLIC"], "default": "ALL", "description": "Tipo de portfolio a mostrar"},
                },
            },
        ),
        Tool(
            name="get_copytrader_detail",
            description=(
                "Obtiene el detalle completo de un copy-trader específico de Binance. "
                "Requiere el portfolio_id (obtenido del search o de la URL de Binance). "
                "Devuelve: perfil, stats (balance, AUM, copier PnL, win rate, MDD, Sharpe), "
                "rendimiento por período, gráfico de ROI, distribución por moneda, "
                "y posiciones abiertas actuales con entry price, mark price, PnL y leverage. "
                "Usa Playwright (headless browser) — la primera llamada tarda más por el warmup."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "portfolio_id": {"type": "string", "description": "ID del portfolio del líder. Se obtiene del search_top_copytraders o de la URL de Binance (el número largo en la URL)"},
                    "time_range": {"type": "string", "enum": ["7D", "30D", "90D"], "default": "7D", "description": "Período para gráfico ROI y distribución de monedas"},
                },
                "required": ["portfolio_id"],
            },
        ),
        Tool(
            name="scan_copytrader_positions",
            description=(
                "Escanea los top N copy-traders de Binance y agrega sus posiciones abiertas "
                "para un símbolo específico. Muestra cuántos traders tienen LONG vs SHORT, "
                "el volumen nocional total por lado, el AUM respaldando cada dirección, "
                "y el detalle individual de cada trader con esa posición. "
                "Útil para medir el sentimiento y la concentración del smart money en una moneda. "
                "Escanea en batch paralelo (~2s por cada 20 traders). "
                "Con top_n=100 tarda unos 10-15 segundos."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Par de trading (ej: ETHUSDT, BTCUSDT)"},
                    "top_n": {"type": "integer", "default": 100, "description": "Cantidad de top traders a escanear (max 200)"},
                    "sort_by": {"type": "string", "enum": ["AUM", "ROI", "PNL", "WIN_RATE"], "default": "AUM", "description": "Criterio de ranking para seleccionar los top traders. AUM recomendado para estimar volumen real."},
                    "time_range": {"type": "string", "enum": ["7D", "30D", "90D"], "default": "30D", "description": "Período del ranking"},
                },
                "required": ["symbol"],
            },
        ),
        Tool(
            name="get_copytrader_history",
            description=(
                "Obtiene el historial completo de trades cerrados de un copy-trader y genera "
                "un análisis de estrategia. Para cada trade devuelve: símbolo, dirección (LONG/SHORT), "
                "fecha apertura/cierre, duración, precio entrada/salida, PnL, y volumen. "
                "El análisis incluye: win rate, profit factor, expectancy, R:R ratio, rachas, "
                "duración promedio, breakdown por dirección y por símbolo (top 15). "
                "Requiere el portfolio_id y la cantidad de días hacia atrás a analizar. "
                "IMPORTANTE: primero preguntar al usuario cuántos días quiere analizar."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "portfolio_id": {"type": "string", "description": "ID del portfolio del líder. Se obtiene del search_top_copytraders, get_copytrader_detail, o de la URL de Binance."},
                    "days": {"type": "integer", "default": 7, "description": "Cantidad de días hacia atrás a analizar. Preguntar al usuario antes de llamar."},
                },
                "required": ["portfolio_id", "days"],
            },
        ),

        # ── STRIKER × Ψ-JAM HYBRID ──
        Tool(
            name="striker_scan_pairs",
            description=(
                "STRIKER HYBRID: Escanea watchlist primarios (HUMA, SIGN) + secundarios + "
                "detección dinámica de pumps. Retorna candidatos con modo sugerido (A=Wick Catch, "
                "B=Range Mean-Reversion, C=Pump Fade), dirección, y métricas clave. "
                "Primer paso del workflow Striker × Ψ-JAM."
            ),
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
        Tool(
            name="striker_check_filters",
            description=(
                "STRIKER HYBRID: Ejecuta los 4 kill switches Ψ-JAM antes de entrar: "
                "(1) Funding Kill Switch (-0.10% para SHORT), "
                "(2) Ghost Flow DLI (>+0.15 = demanda institucional), "
                "(3) Wick Requerido (solo Modo A, rejection >3%), "
                "(4) Volume Anomaly (>10x y creciendo). "
                "Retorna GO ✅ o NO_ENTRY ❌ con motivos. "
                "IMPORTANTE: requiere ~15 segundos por el delay de Ghost Flow."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Par de trading (ej: SIGNUSDT)"},
                    "mode": {"type": "string", "enum": ["A", "B", "C"], "default": "A", "description": "Modo de entrada: A=Wick Catch, B=Range, C=Pump Fade"},
                    "direction": {"type": "string", "enum": ["SHORT", "LONG"], "default": "SHORT", "description": "Dirección del trade"},
                },
                "required": ["symbol"],
            },
        ),
        Tool(
            name="striker_monitor_striker",
            description=(
                "STRIKER HYBRID: Consulta posiciones abiertas actuales de Striker突击手 "
                "(portfolio 4944611239358115329) y sus stats. "
                "Útil para ver qué está operando Striker en tiempo real y validar correlación."
            ),
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
        Tool(
            name="striker_open_scout",
            description=(
                "STRIKER HYBRID: Abre un micro-lot de $5 como sensor de dirección. "
                "Patrón descubierto en Striker: abre $5-10 que pierden centavos pero informan "
                "la dirección del conviction trade. Esperar 5 min y evaluar con striker_evaluate_scout."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Par de trading"},
                    "direction": {"type": "string", "enum": ["SHORT", "LONG"], "description": "Dirección del scout"},
                    "account": {"type": "string", "default": "principal", "description": "Cuenta de trading"},
                },
                "required": ["symbol", "direction"],
            },
        ),
        Tool(
            name="striker_evaluate_scout",
            description=(
                "STRIKER HYBRID: Evalúa resultado del scout abierto. "
                "CONFIRM (+0.1%) → abrir conviction. REJECT (-0.3%) → no escalar. "
                "WAIT → seguir esperando (max 15 min). Si 3 scouts pierden en misma dirección, "
                "sugiere FLIP (abrir conviction en dirección opuesta)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Par de trading"},
                    "account": {"type": "string", "default": "principal", "description": "Cuenta de trading"},
                },
                "required": ["symbol"],
            },
        ),
        Tool(
            name="striker_open_conviction",
            description=(
                "STRIKER HYBRID: Abre trade de convicción con sizing por modo. "
                "Modo A: 10% balance (Wick Catch). Modo B: 5% (Range). Modo C: 3% (Pump Fade). "
                "Leverage 20x. Coloca TP1 y SL automáticamente según fórmulas del modo. "
                "Máximo $100 notional por posición."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Par de trading"},
                    "mode": {"type": "string", "enum": ["A", "B", "C"], "description": "Modo de entrada"},
                    "direction": {"type": "string", "enum": ["SHORT", "LONG"], "default": "SHORT", "description": "Dirección"},
                    "account": {"type": "string", "default": "principal", "description": "Cuenta de trading"},
                    "skip_scout": {"type": "boolean", "default": False, "description": "True para saltar scout (solo si Modo A con todo verde o pump >30%)"},
                },
                "required": ["symbol", "mode"],
            },
        ),
        Tool(
            name="striker_set_tp_sl",
            description=(
                "STRIKER HYBRID: Calcula y setea TP/SL según reglas del modo para una posición existente. "
                "Modo A: TP1 -1.5%, TP2 -3.0%, SL +2.5%. "
                "Modo B: TP1 -0.8%, TP2 -1.5%, SL +1.0%. "
                "Modo C: TP1 -0.5%, SL +1.0%. "
                "Los precios se calculan automáticamente desde el entry_price."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Par de trading"},
                    "mode": {"type": "string", "enum": ["A", "B", "C"], "description": "Modo del trade"},
                    "entry_price": {"type": "number", "description": "Precio de entrada"},
                    "direction": {"type": "string", "enum": ["SHORT", "LONG"], "default": "SHORT", "description": "Dirección"},
                    "account": {"type": "string", "default": "principal", "description": "Cuenta de trading"},
                },
                "required": ["symbol", "mode", "entry_price"],
            },
        ),
        Tool(
            name="striker_check_exit",
            description=(
                "STRIKER HYBRID: Evalúa si cerrar una posición abierta. Chequea: "
                "(1) Funding kill switch (-1.0%), "
                "(2) Volume dead (<30% del entry con PnL >+0.3%), "
                "(3) Volume anomaly (10x spike), "
                "(4) Max hold 12h, "
                "(5) Opportunity cost (4h con PnL <+0.5%). "
                "Retorna EXIT ❌ o HOLD ✅."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Par de trading"},
                    "account": {"type": "string", "default": "principal", "description": "Cuenta de trading"},
                },
                "required": ["symbol"],
            },
        ),
        Tool(
            name="striker_partial_close",
            description=(
                "STRIKER HYBRID: Cierre parcial de posición para TP escalonado. "
                "Ejemplo Modo A: cerrar 50% en TP1, 30% en TP2, trailing 20% restante. "
                "Binance no soporta TP parcial nativo, esto ejecuta close por cantidad."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Par de trading"},
                    "pct": {"type": "number", "description": "Porcentaje a cerrar (0-100)"},
                    "account": {"type": "string", "default": "principal", "description": "Cuenta de trading"},
                },
                "required": ["symbol", "pct"],
            },
        ),
        Tool(
            name="striker_check_risk",
            description=(
                "STRIKER HYBRID: Verifica límites de riesgo antes de operar. "
                "Max posiciones: 3. Max notional total: $200. Max trades/día: 15. "
                "Circuit breakers: 3 losses → pausa 1h. Daily loss >10% → stop 24h. "
                "Retorna estado completo del risk manager."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "account": {"type": "string", "default": "principal", "description": "Cuenta de trading"},
                },
            },
        ),
        Tool(
            name="striker_daily_report",
            description=(
                "STRIKER HYBRID: Reporte de performance del día comparando nuestros trades "
                "vs Striker突击手. Incluye PnL, trades, win rate, profit factor nuestro "
                "y posiciones/stats actuales de Striker."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "account": {"type": "string", "default": "principal", "description": "Cuenta de trading"},
                },
            },
        ),
    ]


# ─────────────────────────────────────────────
# TOOL HANDLERS
# ─────────────────────────────────────────────

async def _fetch_klines_data(args: dict) -> tuple:
    """Helper: fetch klines and extract arrays."""
    klines = await client.get_klines(
        symbol=args["symbol"],
        interval=args.get("interval", "1h"),
        limit=args.get("limit", 500),
    )
    closes = np.array([k["close"] for k in klines])
    volumes = np.array([k["volume"] for k in klines])
    taker_buy_vols = np.array([k["taker_buy_volume"] for k in klines])
    highs = np.array([k["high"] for k in klines])
    lows = np.array([k["low"] for k in klines])
    return klines, closes, volumes, taker_buy_vols, highs, lows


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    try:
        # ── DATA TOOLS ──
        if name == "get_klines":
            data = await client.get_klines(
                symbol=arguments["symbol"],
                interval=arguments.get("interval", "1h"),
                limit=arguments.get("limit", 200),
                start_time=arguments.get("start_time"),
                end_time=arguments.get("end_time"),
            )
            result = {"symbol": arguments["symbol"], "count": len(data), "klines": data}
            result["context"] = ctx.contextualize_klines(result)
            return _json_response(result)

        elif name == "get_multi_tf_klines":
            data = await client.get_multi_tf_klines(
                symbol=arguments["symbol"],
                intervals=arguments["intervals"],
                limit=arguments.get("limit", 200),
            )
            summary = {tf: len(klines) for tf, klines in data.items()}
            result = {"symbol": arguments["symbol"], "summary": summary, "data": data}
            result["context"] = ctx.contextualize_klines(result)
            return _json_response(result)

        elif name == "get_orderbook":
            data = await client.get_orderbook(arguments["symbol"], arguments.get("limit", 100))
            data["context"] = ctx.contextualize_orderbook(data)
            return _json_response(data)

        elif name == "get_orderbook_light":
            data = await client.get_orderbook_snapshot(arguments["symbol"], arguments.get("depth", 20))
            data["context"] = ctx.contextualize_orderbook(data)
            return _json_response(data)

        elif name == "get_recent_trades":
            data = await client.get_recent_trades(arguments["symbol"], arguments.get("limit", 500))
            result = {"symbol": arguments["symbol"], "count": len(data), "trades": data}
            result["context"] = ctx.contextualize_trades(result)
            return _json_response(result)

        elif name == "get_agg_trades":
            data = await client.get_agg_trades(
                arguments["symbol"],
                arguments.get("limit", 500),
                arguments.get("start_time"),
                arguments.get("end_time"),
            )
            result = {"symbol": arguments["symbol"], "count": len(data), "trades": data}
            result["context"] = ctx.contextualize_trades(result)
            return _json_response(result)

        elif name == "get_premium_index":
            sym = arguments.get("symbol")
            data = await client.get_premium_index(symbol=sym)
            if isinstance(data, dict):
                result = data
            else:
                result = {"count": len(data), "symbols": data}
            result["context"] = ctx.contextualize_premium_index(result)
            return _json_response(result)

        elif name == "get_funding_rate":
            data = await client.get_funding_rate(
                arguments["symbol"],
                arguments.get("limit", 100),
                arguments.get("start_time"),
            )
            result = {"symbol": arguments["symbol"], "count": len(data), "funding_rates": data}
            result["context"] = ctx.contextualize_funding(result)
            return _json_response(result)

        elif name == "get_open_interest":
            if arguments.get("historical", False):
                data = await client.get_open_interest_hist(
                    arguments["symbol"],
                    arguments.get("period", "1h"),
                    arguments.get("limit", 30),
                )
                result = {"symbol": arguments["symbol"], "count": len(data), "history": data}
                result["context"] = ctx.contextualize_open_interest(result)
                return _json_response(result)
            else:
                data = await client.get_open_interest(arguments["symbol"])
                data["context"] = ctx.contextualize_open_interest(data)
                return _json_response(data)

        elif name == "get_long_short_ratio":
            data = await client.get_long_short_ratio(
                arguments["symbol"],
                arguments.get("period", "1h"),
                arguments.get("limit", 30),
            )
            result = {"symbol": arguments["symbol"], "count": len(data), "ratios": data}
            result["context"] = ctx.contextualize_long_short_ratio(result)
            return _json_response(result)

        elif name == "get_taker_volume":
            data = await client.get_taker_buy_sell_ratio(
                arguments["symbol"],
                arguments.get("period", "1h"),
                arguments.get("limit", 30),
            )
            result = {"symbol": arguments["symbol"], "count": len(data), "ratios": data}
            result["context"] = ctx.contextualize_taker_volume(result)
            return _json_response(result)

        elif name == "get_ticker":
            data = await client.get_ticker_24h(arguments["symbol"])
            data["context"] = ctx.contextualize_ticker(data)
            return _json_response(data)

        elif name == "list_symbols":
            data = await client.get_exchange_info(arguments.get("symbol"))
            return _json_response(data)

        # ── L2 HISTORY TOOLS ──
        elif name == "start_l2_recording":
            interval_sec = max(5, arguments.get("interval_sec", 30))
            depth = arguments.get("depth", 20)
            result = await l2_collector.start(
                symbol=arguments["symbol"],
                interval_sec=interval_sec,
                depth=depth,
            )
            return _json_response(result)

        elif name == "stop_l2_recording":
            result = await l2_collector.stop(arguments["symbol"])
            return _json_response(result)

        elif name == "get_l2_history":
            snapshots = l2_store.get_history(
                symbol=arguments["symbol"],
                session_id=arguments.get("session_id"),
                start_time=arguments.get("start_time"),
                end_time=arguments.get("end_time"),
                limit=arguments.get("limit", 500),
                metrics_only=arguments.get("metrics_only", False),
            )
            return _json_response({
                "symbol": arguments["symbol"].upper(),
                "count": len(snapshots),
                "snapshots": snapshots,
            })

        elif name == "get_l2_recording_status":
            result = l2_collector.status()
            return _json_response(result)

        elif name == "list_l2_sessions":
            sessions = l2_store.get_sessions(symbol=arguments.get("symbol"))
            return _json_response({
                "count": len(sessions),
                "sessions": sessions,
            })

        elif name == "purge_l2_history":
            older_than_ms = None
            if "older_than_hours" in arguments:
                older_than_ms = int(time.time() * 1000) - int(arguments["older_than_hours"] * 3600 * 1000)
            deleted = l2_store.purge(
                symbol=arguments.get("symbol"),
                older_than_ms=older_than_ms,
            )
            return _json_response({
                "deleted_snapshots": deleted,
                "stats": l2_store.get_stats(),
            })

        # ── SCANNER TOOLS ──
        elif name == "scan_altcoins":
            result = await scanner.scan(
                top_n=_arg(arguments, "top_n", 20),
                min_change_pct=_arg(arguments, "min_change_pct", 2.0),
                min_quote_volume=_arg(arguments, "min_quote_volume", 5_000_000),
                include_btc=_arg(arguments, "include_btc", False),
                max_candidates=_arg(arguments, "max_candidates", 80),
                kline_interval=_arg(arguments, "kline_interval", "1h"),
                kline_limit=_arg(arguments, "kline_limit", 100),
                sigma_threshold=_arg(arguments, "sigma_threshold", 2.0),
                min_jam_score=_arg(arguments, "min_jam_score", 0.60),
            )
            result["context"] = ctx.contextualize_scan(result)
            return _json_response(result)

        elif name == "scan_altcoins_quick":
            top_n = _arg(arguments, "top_n", 40)
            phase1 = await scanner._phase1_ticker_screen(
                min_change_pct=_arg(arguments, "min_change_pct", 1.0),
                min_quote_volume=_arg(arguments, "min_quote_volume", 1_000_000),
                include_btc=_arg(arguments, "include_btc", False),
            )
            candidates = phase1["candidates"][:top_n]
            for i, c in enumerate(candidates, 1):
                c["rank"] = i
            result = {
                "scan_timestamp": int(time.time() * 1000),
                "mode": "quick (ticker only, no JAM)",
                "total_screened": phase1["total_screened"],
                "count": len(candidates),
                "top_altcoins": candidates,
            }
            result["context"] = ctx.contextualize_scan_quick(result)
            return _json_response(result)

        # ── GLOBAL MARKET ──
        elif name == "get_global_market":
            result = await client.get_global_market_analysis()
            result["context"] = ctx.contextualize_global_market(result)
            return _json_response(result)

        # ── TECHNICAL ANALYSIS ──
        elif name == "get_technical_analysis":
            symbol = arguments["symbol"]
            intervals = arguments.get("intervals", ["5m", "15m", "1h", "4h", "1d"])
            limit = arguments.get("limit", 200)

            # Fetch all timeframes
            multi_data = await client.get_multi_tf_klines(
                symbol=symbol, intervals=intervals, limit=limit,
            )

            results = {}
            for tf, klines in multi_data.items():
                if not klines or len(klines) < 10:
                    results[tf] = {"error": "insufficient_data", "candles": len(klines) if klines else 0}
                    continue

                opens   = np.array([k["open"]   for k in klines])
                highs   = np.array([k["high"]   for k in klines])
                lows    = np.array([k["low"]    for k in klines])
                closes  = np.array([k["close"]  for k in klines])
                volumes = np.array([k["volume"] for k in klines])

                tf_result = ta.full_technical_analysis(opens, highs, lows, closes, volumes)
                tf_result["candles_analyzed"] = len(klines)
                results[tf] = tf_result

            # Cross-timeframe alignment
            biases = {}
            for tf, r in results.items():
                bs = r.get("bias_summary", {})
                if "bias" in bs:
                    biases[tf] = {"bias": bs["bias"], "score": bs["score"]}

            alignment = "MIXED"
            if biases:
                scores = [b["score"] for b in biases.values() if b["score"] is not None]
                if scores:
                    avg = sum(scores) / len(scores)
                    if all(s > 0.05 for s in scores):
                        alignment = "ALL_BULLISH"
                    elif all(s < -0.05 for s in scores):
                        alignment = "ALL_BEARISH"
                    elif avg > 0.1:
                        alignment = "MOSTLY_BULLISH"
                    elif avg < -0.1:
                        alignment = "MOSTLY_BEARISH"

            result = {
                "symbol": symbol,
                "timeframes": intervals,
                "multi_tf_alignment": alignment,
                "tf_biases": biases,
                "analysis": results,
            }
            result["context"] = ctx.contextualize_technical_analysis(result)
            return _json_response(result)

        # ── Ψ-JAM ANALYSIS ──
        elif name == "analyze_kramers_moyal":
            _, closes, *_ = await _fetch_klines_data(arguments)
            log_returns = np.diff(np.log(closes + 1e-12))
            result = analysis.kramers_moyal_coefficients(
                log_returns, bins=arguments.get("bins", 50)
            )
            result_dict = {"symbol": arguments["symbol"], "interval": arguments.get("interval", "1h"), **result}
            result_dict["context"] = ctx.contextualize_kramers_moyal(result_dict)
            return _json_response(result_dict)

        elif name == "analyze_hurst":
            _, closes, *_ = await _fetch_klines_data(arguments)
            result = analysis.hurst_exponent(closes)
            result_dict = {"symbol": arguments["symbol"], "interval": arguments.get("interval", "1h"), **result}
            result_dict["context"] = ctx.contextualize_hurst(result_dict)
            return _json_response(result_dict)

        elif name == "analyze_lyapunov":
            _, closes, *_ = await _fetch_klines_data(arguments)
            log_returns = np.diff(np.log(closes + 1e-12))
            result = analysis.lyapunov_exponent(
                log_returns,
                embedding_dim=arguments.get("embedding_dim", 5),
                tau=arguments.get("tau", 1),
            )
            result_dict = {"symbol": arguments["symbol"], "interval": arguments.get("interval", "1h"), **result}
            result_dict["context"] = ctx.contextualize_lyapunov(result_dict)
            return _json_response(result_dict)

        elif name == "analyze_rqa":
            _, closes, *_ = await _fetch_klines_data(arguments)
            log_returns = np.diff(np.log(closes + 1e-12))
            rqa_kwargs = {
                "embedding_dim": arguments.get("embedding_dim", 3),
                "tau": arguments.get("tau", 1),
                "target_rr": arguments.get("target_rr", 0.01),
            }
            # Legacy support: if threshold_pct is explicitly passed, use it
            if "threshold_pct" in arguments:
                rqa_kwargs["threshold_pct"] = arguments["threshold_pct"]
            result = analysis.rqa_analysis(log_returns, **rqa_kwargs)
            result_dict = {"symbol": arguments["symbol"], "interval": arguments.get("interval", "1h"), **result}
            result_dict["context"] = ctx.contextualize_rqa(result_dict)
            return _json_response(result_dict)

        elif name == "analyze_vpin":
            trades = await client.get_agg_trades_paginated(
                symbol=arguments["symbol"],
                total=arguments.get("n_trades", 5000),
            )
            result = analysis.compute_vpin(
                trades,
                n_buckets=arguments.get("n_buckets", 50),
            )
            result_dict = {"symbol": arguments["symbol"], **result}
            result_dict["context"] = ctx.contextualize_vpin(result_dict)
            return _json_response(result_dict)

        elif name == "analyze_kyles_lambda":
            klines, closes, volumes, taker_buy_vols, *_ = await _fetch_klines_data(arguments)
            price_changes = np.diff(closes)
            signed_volumes = np.array([
                k["taker_buy_volume"] - (k["volume"] - k["taker_buy_volume"])
                for k in klines[1:]
            ])
            result = analysis.kyles_lambda(price_changes, signed_volumes)
            result_dict = {"symbol": arguments["symbol"], "interval": arguments.get("interval", "5m"), **result}
            result_dict["context"] = ctx.contextualize_kyles_lambda(result_dict)
            return _json_response(result_dict)

        elif name == "analyze_jam_regime":
            _, closes, volumes, taker_buy_vols, highs, lows = await _fetch_klines_data(arguments)
            result = analysis.jam_regime_analysis(
                closes, volumes, taker_buy_vols, highs, lows,
                window=arguments.get("window", 20),
            )
            result_dict = {"symbol": arguments["symbol"], "interval": arguments.get("interval", "1h"), **result}
            result_dict["context"] = ctx.contextualize_jam_regime(result_dict)
            return _json_response(result_dict)

        elif name == "full_jam_pipeline":
            _, closes, volumes, taker_buy_vols, highs, lows = await _fetch_klines_data(arguments)
            trades = await client.get_agg_trades_paginated(
                symbol=arguments["symbol"], total=5000,
            )
            result = analysis.full_psi_jam_analysis(
                closes, volumes, taker_buy_vols, highs, lows, trades=trades
            )
            result_dict = {
                "symbol": arguments["symbol"],
                "interval": arguments.get("interval", "1h"),
                "candles": arguments.get("limit", 500),
                "pipeline": "Ψ-jam v2 full analysis",
                **result,
            }
            result_dict["context"] = ctx.contextualize_full_pipeline(result_dict)
            return _json_response(result_dict)

        # ── OI LEVEL (OIED) ──
        elif name == "oi_level":
            result = await oi_chart.oi_level(
                binance_client=client,
                symbol=arguments["symbol"],
                period=arguments.get("period", "1h"),
                limit=arguments.get("limit", 100),
                divergence_window=arguments.get("divergence_window", 6),
                show_volume=arguments.get("show_volume", True),
                show_funding=arguments.get("show_funding", True),
                annotate=arguments.get("annotate", True),
                include_series=arguments.get("include_series", False),
            )
            return _json_response(result)

        # ── LIQUIDATION CLUSTERS ──
        elif name == "get_liquidation_clusters":
            symbol = arguments["symbol"]
            interval = arguments.get("interval", "1h")
            limit = arguments.get("limit", 168)
            leverage_levels = arguments.get("leverage_levels")
            
            # Fetch required data in parallel
            klines, oi_current, funding_rates = await asyncio.gather(
                client.get_klines(symbol=symbol, interval=interval, limit=limit),
                client.get_open_interest(symbol=symbol),
                client.get_funding_rate(symbol=symbol, limit=1),
            )
            
            current_price = klines[-1]["close"] if klines else 0
            funding_rate = funding_rates[0]["funding_rate"] if funding_rates else 0
            
            result = analysis.calculate_liquidation_clusters(
                current_price=current_price,
                klines=klines,
                oi_data=oi_current,
                oi_history=None,
                funding_rate=funding_rate,
                leverage_levels=leverage_levels,
            )
            
            result["symbol"] = symbol.upper()
            result["interval"] = interval
            result["context"] = ctx.contextualize_liquidation_clusters(result)
            return _json_response(result)

        # ── RANGE ASYMMETRY ──
        elif name == "analyze_range_asymmetry":
            result = await asymmetry.analyze_range_asymmetry(
                binance_client=client,
                symbol=arguments["symbol"],
                entry_price=arguments.get("entry_price"),
                side=arguments.get("side", "SHORT"),
                timeframe=arguments.get("timeframe", "1d"),
                pump_threshold=arguments.get("pump_threshold", 5.0),
            )
            result["context"] = ctx.contextualize_asymmetry(result)
            return _json_response(result)

        # ── TRADING TOOLS ──
        elif name == "futures_list_accounts":
            accounts = []
            for acct_name, cfg in ACCOUNT_CONFIG.items():
                has_keys = bool(
                    os.environ.get(cfg["key_env"]) and os.environ.get(cfg["secret_env"])
                )
                accounts.append({
                    "account": acct_name,
                    "label": cfg["label"],
                    "configured": has_keys,
                })
            return _json_response({
                "accounts": accounts,
                "note": "Use the 'account' parameter in any trading tool to select which account to operate on.",
            })

        elif name == "futures_get_balance":
            trader = _get_trader(arguments.get("account", "principal"))
            result = await trader.get_account_balance()
            result["account"] = arguments.get("account", "principal")
            result["context"] = ctx.contextualize_balance(result)
            return _json_response(result)

        elif name == "futures_get_positions":
            trader = _get_trader(arguments.get("account", "principal"))
            result = await trader.get_positions(symbol=arguments.get("symbol"))
            if isinstance(result, dict):
                result["account"] = arguments.get("account", "principal")
                result["context"] = ctx.contextualize_positions(result)
            return _json_response(result)

        elif name == "futures_get_performance":
            trader = _get_trader(arguments.get("account", "principal"))
            days = _arg(arguments, "days", 30)
            days = min(int(days), 90)
            result = await trader.get_performance(
                days=days,
                symbol=arguments.get("symbol"),
            )
            result["account"] = arguments.get("account", "principal")
            result["context"] = ctx.contextualize_performance(result)
            return _json_response(result)

        elif name == "futures_get_income_history":
            trader = _get_trader(arguments.get("account", "principal"))
            days = min(int(_arg(arguments, "days", 7)), 90)
            records = await trader.get_income_history(
                income_type=arguments.get("income_type"),
                symbol=arguments.get("symbol"),
                days=days,
            )
            # Format for readability
            from datetime import datetime, timezone
            formatted = []
            for r in records:
                ts = int(r.get("time", 0))
                formatted.append({
                    "datetime": datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                    "symbol": r.get("symbol", ""),
                    "type": r.get("incomeType", ""),
                    "income": float(r.get("income", 0)),
                    "asset": r.get("asset", "USDT"),
                })
            total = sum(r["income"] for r in formatted)
            result = {
                "account": arguments.get("account", "principal"),
                "period_days": days,
                "filter_type": arguments.get("income_type"),
                "filter_symbol": arguments.get("symbol"),
                "total_records": len(formatted),
                "total_income": round(total, 4),
                "records": formatted,
            }
            return _json_response(result)

        elif name == "futures_evaluate_exit":
            import time as _time
            account = arguments.get("account", "principal")
            trader = _get_trader(account)
            filter_symbol = arguments.get("symbol")

            # 1. Get open positions
            pos_data = await trader.get_positions(symbol=filter_symbol)
            positions = pos_data.get("positions", [])
            shorts = [p for p in positions if p["side"] == "SHORT"]

            if not shorts:
                return _json_response({
                    "account": account,
                    "message": "No hay posiciones SHORT abiertas" + (f" para {filter_symbol}" if filter_symbol else "") + ".",
                    "evaluations": [],
                })

            evaluations = []
            for pos in shorts:
                sym = pos["symbol"]

                # 2. Fetch market data in parallel-ish fashion
                try:
                    klines_1h = await client.get_klines(sym, interval="1h", limit=24)
                except Exception:
                    klines_1h = []

                try:
                    taker = await client.get_taker_buy_sell_ratio(sym, period="1h", limit=5)
                except Exception:
                    taker = []

                try:
                    funding_rates = await client.get_funding_rate(sym, limit=1)
                    current_funding_rate = funding_rates[0]["funding_rate"] if funding_rates else 0
                except Exception:
                    current_funding_rate = 0

                try:
                    ticker = await client.get_ticker_24h(sym)
                except Exception:
                    ticker = {}

                # 3. Get funding paid on this symbol
                try:
                    funding_records = await trader.get_income_history(
                        income_type="FUNDING_FEE", symbol=sym, days=3, limit=100
                    )
                    funding_paid = sum(float(r.get("income", 0)) for r in funding_records)
                except Exception:
                    funding_paid = 0

                # 4. Estimate holding time from funding records + income
                # Use the earliest income record for this symbol as proxy for entry time
                try:
                    all_income = await trader.get_income_history(symbol=sym, days=3, limit=100)
                    if all_income:
                        earliest = min(int(r.get("time", 0)) for r in all_income)
                        holding_hours = (_time.time() * 1000 - earliest) / 3_600_000
                    else:
                        holding_hours = 0
                except Exception:
                    holding_hours = 0

                # 5. Build position dict with holding info
                pos_with_holding = dict(pos)
                pos_with_holding["holding_hours"] = holding_hours

                # 6. Build market data dict
                market_data = {
                    "klines_1h": klines_1h,
                    "taker_ratio": taker,
                    "funding_paid": funding_paid,
                    "funding_rate_current": current_funding_rate,
                    "ticker_24h": ticker,
                }

                # 7. Evaluate
                evaluation = ctx.evaluate_exit(pos_with_holding, market_data)
                evaluations.append(evaluation)

            # Sort by urgency (most urgent first)
            evaluations.sort(key=lambda x: x.get("urgency", 0), reverse=True)

            return _json_response({
                "account": account,
                "total_shorts": len(shorts),
                "evaluations": evaluations,
            })

        elif name == "futures_set_leverage":
            trader = _get_trader(arguments.get("account", "principal"))
            result = await trader.set_leverage(
                symbol=arguments["symbol"],
                leverage=arguments["leverage"],
            )
            result["account"] = arguments.get("account", "principal")
            return _json_response(result)

        elif name == "futures_open_position":
            trader = _get_trader(arguments.get("account", "principal"))
            result = await trader.open_position(
                symbol=arguments["symbol"],
                side=arguments["side"],
                quantity=arguments["quantity"],
                order_type=arguments.get("order_type", "MARKET"),
                price=arguments.get("price"),
                leverage=arguments.get("leverage"),
                take_profit=arguments.get("take_profit"),
                stop_loss=arguments.get("stop_loss"),
                trailing_stop_callback=arguments.get("trailing_stop_callback"),
                trailing_stop_activation=arguments.get("trailing_stop_activation"),
            )
            result["account"] = arguments.get("account", "principal")
            # Contextualize TP/SL if they were placed
            if "tp_sl" in result and isinstance(result["tp_sl"], dict):
                result["tp_sl"]["context"] = ctx.contextualize_tp_sl(result["tp_sl"])
            return _json_response(result)

        elif name == "futures_open_stop_limit":
            trader = _get_trader(arguments.get("account", "principal"))
            result = await trader.open_stop_limit_position(
                symbol=arguments["symbol"],
                side=arguments["side"],
                quantity=arguments["quantity"],
                stop_price=arguments["stop_price"],
                price=arguments["price"],
                leverage=arguments.get("leverage"),
                take_profit=arguments.get("take_profit"),
                stop_loss=arguments.get("stop_loss"),
            )
            result["account"] = arguments.get("account", "principal")
            if "tp_sl" in result and isinstance(result["tp_sl"], dict):
                result["tp_sl"]["context"] = ctx.contextualize_tp_sl(result["tp_sl"])
            return _json_response(result)

        elif name == "futures_close_position":
            trader = _get_trader(arguments.get("account", "principal"))
            result = await trader.close_position(
                symbol=arguments["symbol"],
                quantity=arguments.get("quantity"),
                order_type=arguments.get("order_type", "MARKET"),
                price=arguments.get("price"),
            )
            result["account"] = arguments.get("account", "principal")
            return _json_response(result)

        elif name == "futures_set_tp_sl":
            trader = _get_trader(arguments.get("account", "principal"))
            result = await trader.set_tp_sl(
                symbol=arguments["symbol"],
                take_profit=arguments.get("take_profit"),
                stop_loss=arguments.get("stop_loss"),
                tp_quantity=arguments.get("tp_quantity"),
                sl_quantity=arguments.get("sl_quantity"),
                trailing_stop_callback=arguments.get("trailing_stop_callback"),
                trailing_stop_activation=arguments.get("trailing_stop_activation"),
                trailing_stop_quantity=arguments.get("trailing_stop_quantity"),
                take_profits=arguments.get("take_profits"),
            )
            result["account"] = arguments.get("account", "principal")
            result["context"] = ctx.contextualize_tp_sl(result)
            return _json_response(result)

        elif name == "futures_get_open_orders":
            trader = _get_trader(arguments.get("account", "principal"))
            result = await trader.get_open_orders(symbol=arguments.get("symbol"))
            if isinstance(result, dict):
                result["account"] = arguments.get("account", "principal")
                result["context"] = ctx.contextualize_open_orders(result)
            return _json_response(result)

        elif name == "futures_cancel_order":
            trader = _get_trader(arguments.get("account", "principal"))
            result = await trader.cancel_order(
                symbol=arguments["symbol"],
                order_id=arguments["order_id"],
            )
            result["account"] = arguments.get("account", "principal")
            return _json_response(result)

        elif name == "futures_cancel_all_orders":
            trader = _get_trader(arguments.get("account", "principal"))
            result = await trader.cancel_all_orders(symbol=arguments["symbol"])
            result["account"] = arguments.get("account", "principal")
            return _json_response(result)

        # ── BASIS / ARBITRAGE TOOL HANDLERS ──

        elif name == "scan_basis":
            min_vol = _arg(arguments, "min_volume_usd", 1_000_000)
            min_basis = _arg(arguments, "min_basis_pct", 0.05)
            top_n = _arg(arguments, "top_n", 20)
            result = await basis_scanner.scan(
                min_volume=min_vol,
                min_basis_pct=min_basis,
                top_n=top_n,
            )
            result["context"] = ctx.contextualize_basis_scan(result)
            return _json_response(result)

        elif name == "scan_basis_quick":
            min_vol = _arg(arguments, "min_volume_usd", 1_000_000)
            top_n = _arg(arguments, "top_n", 30)
            result = await basis_scanner.quick_scan(
                min_volume=min_vol,
                top_n=top_n,
            )
            result["context"] = ctx.contextualize_basis_scan(result)
            return _json_response(result)

        elif name == "get_basis":
            result = await basis_scanner.get_basis(symbol=arguments["symbol"])
            result["context"] = ctx.contextualize_basis_single(result)
            return _json_response(result)

        elif name == "get_basis_history":
            interval = _arg(arguments, "interval", "1h")
            limit = _arg(arguments, "limit", 168)
            result = await basis_scanner.get_basis_history(
                symbol=arguments["symbol"],
                interval=interval,
                limit=limit,
            )
            result["context"] = ctx.contextualize_basis_history(result)
            return _json_response(result)

        elif name == "prepare_basis_trade":
            bt = _get_basis_trader(arguments["account"])
            result = await bt.prepare_trade(
                symbol=arguments["symbol"],
                capital_usdt=arguments["capital_usd"],
                strategy=arguments["strategy"],
            )
            result["context"] = ctx.contextualize_basis_trade_proposal(result)
            return _json_response(result)

        elif name == "execute_basis_trade":
            bt = _get_basis_trader(arguments["account"])
            result = await bt.execute_trade(
                proposal_id=arguments["proposal_id"],
            )
            return _json_response(result)

        elif name == "close_basis_trade":
            bt = _get_basis_trader(arguments["account"])
            result = await bt.close_trade(
                position_id=arguments["position_id"],
            )
            return _json_response(result)

        elif name == "list_basis_positions":
            bt = _get_basis_trader(arguments["account"])
            result = await bt.list_positions()
            result["context"] = ctx.contextualize_basis_positions(result)
            return _json_response(result)

        elif name == "get_basis_dashboard":
            bt = _get_basis_trader(arguments["account"])
            result = await bt.get_dashboard()
            result["context"] = ctx.contextualize_basis_dashboard(result)
            return _json_response(result)

        # ── CARRY DETECTOR ──
        elif name == "scan_carry":
            result = await carry_detector.scan(
                min_volume=_arg(arguments, "min_volume_usd", 5_000_000),
                min_funding_pct=_arg(arguments, "min_funding_pct", -0.5),
                min_persistence=_arg(arguments, "min_persistence", 3),
                top_n=_arg(arguments, "top_n", 15),
            )
            result["context"] = ctx.contextualize_carry_scan(result)
            return _json_response(result)

        elif name == "scan_carry_quick":
            result = await carry_detector.quick_scan(
                min_volume=_arg(arguments, "min_volume_usd", 5_000_000),
                min_funding_pct=_arg(arguments, "min_funding_pct", -0.5),
                top_n=_arg(arguments, "top_n", 30),
            )
            result["context"] = ctx.contextualize_carry_quick(result)
            return _json_response(result)

        elif name == "analyze_carry":
            result = await carry_detector.analyze_symbol(
                symbol=arguments["symbol"],
            )
            result["context"] = ctx.contextualize_carry_single(result)
            return _json_response(result)

        elif name == "monitor_carry_exit":
            result = await carry_detector.monitor_exit(
                symbol=arguments["symbol"],
                entry_funding_pct=arguments["entry_funding_pct"],
            )
            result["context"] = ctx.contextualize_carry_exit(result)
            return _json_response(result)

        # ── STRATEGY GUARD ──
        elif name == "evaluate_short_entry":
            result = await strategy_guard.evaluate_short_entry(
                symbol=arguments["symbol"],
                account=arguments["account"],
                sl_percent=_arg(arguments, "sl_percent", 5),
            )
            return _json_response(result)

        elif name == "activate_guard":
            result = await strategy_guard.activate_guard(
                symbol=arguments["symbol"],
                account=arguments["account"],
                auto_close=_arg(arguments, "auto_close", False),
                entry_price=arguments.get("entry_price"),
                sl_price=arguments.get("sl_price"),
                max_hold_hours=arguments.get("max_hold_hours"),
            )
            return _json_response(result)

        elif name == "guard_status":
            result = await strategy_guard.guard_status(
                account=arguments.get("account"),
            )
            return _json_response(result)

        # ── GHOST FLOW (Pseudo-L3) ──
        elif name == "analyze_ghost_flow":
            result = await ghost_flow.analyze_ghost_flow(
                client=client,
                symbol=arguments["symbol"],
                delay_seconds=_arg(arguments, "delay_seconds", 15),
                depth=_arg(arguments, "depth", 20),
                trades_limit=_arg(arguments, "trades_limit", 1000),
                ghost_threshold=_arg(arguments, "ghost_threshold", 0.7),
                iceberg_threshold=_arg(arguments, "iceberg_threshold", 1.5),
            )
            return _json_response(result)

        # ── COPY-TRADING ANALYSIS ──
        elif name == "search_top_copytraders":
            result = await copytrading_client.search_top_traders(
                page_number=_arg(arguments, "page_number", 1),
                page_size=_arg(arguments, "page_size", 18),
                time_range=_arg(arguments, "time_range", "30D"),
                sort_by=_arg(arguments, "sort_by", "ROI"),
                order=_arg(arguments, "order", "DESC"),
                nickname=_arg(arguments, "nickname", ""),
                hide_full=_arg(arguments, "hide_full", False),
                portfolio_type=_arg(arguments, "portfolio_type", "ALL"),
            )
            return _json_response(result)

        elif name == "get_copytrader_detail":
            result = await copytrading_client.get_trader_detail(
                portfolio_id=arguments["portfolio_id"],
                time_range=_arg(arguments, "time_range", "7D"),
            )
            return _json_response(result)

        elif name == "scan_copytrader_positions":
            result = await copytrading_client.scan_symbol_positions(
                symbol=arguments["symbol"],
                top_n=_arg(arguments, "top_n", 100),
                sort_by=_arg(arguments, "sort_by", "AUM"),
                time_range=_arg(arguments, "time_range", "30D"),
            )
            return _json_response(result)

        elif name == "get_copytrader_history":
            result = await copytrading_client.get_trader_history(
                portfolio_id=arguments["portfolio_id"],
                days=_arg(arguments, "days", 7),
            )
            return _json_response(result)

        # ── STRIKER × Ψ-JAM HYBRID ──
        elif name == "striker_scan_pairs":
            result = await striker_strategy.scan_pairs()
            return _json_response(result)

        elif name == "striker_check_filters":
            result = await striker_strategy.check_filters(
                symbol=arguments["symbol"],
                mode=_arg(arguments, "mode", "A"),
                direction=_arg(arguments, "direction", "SHORT"),
            )
            return _json_response(result)

        elif name == "striker_monitor_striker":
            result = await striker_strategy.monitor_striker()
            return _json_response(result)

        elif name == "striker_open_scout":
            result = await striker_strategy.open_scout(
                symbol=arguments["symbol"],
                direction=arguments["direction"],
                account=_arg(arguments, "account", "principal"),
            )
            return _json_response(result)

        elif name == "striker_evaluate_scout":
            result = await striker_strategy.evaluate_scout(
                symbol=arguments["symbol"],
                account=_arg(arguments, "account", "principal"),
            )
            return _json_response(result)

        elif name == "striker_open_conviction":
            result = await striker_strategy.open_conviction(
                symbol=arguments["symbol"],
                mode=arguments["mode"],
                direction=_arg(arguments, "direction", "SHORT"),
                account=_arg(arguments, "account", "principal"),
                skip_scout=_arg(arguments, "skip_scout", False),
            )
            return _json_response(result)

        elif name == "striker_set_tp_sl":
            result = await striker_strategy.set_tp_sl(
                symbol=arguments["symbol"],
                mode=arguments["mode"],
                entry_price=arguments["entry_price"],
                direction=_arg(arguments, "direction", "SHORT"),
                account=_arg(arguments, "account", "principal"),
            )
            return _json_response(result)

        elif name == "striker_check_exit":
            result = await striker_strategy.check_exit(
                symbol=arguments["symbol"],
                account=_arg(arguments, "account", "principal"),
            )
            return _json_response(result)

        elif name == "striker_partial_close":
            result = await striker_strategy.partial_close(
                symbol=arguments["symbol"],
                pct=arguments["pct"],
                account=_arg(arguments, "account", "principal"),
            )
            return _json_response(result)

        elif name == "striker_check_risk":
            result = await striker_strategy.check_risk(
                account=_arg(arguments, "account", "principal"),
            )
            return _json_response(result)

        elif name == "striker_daily_report":
            result = await striker_strategy.daily_report(
                account=_arg(arguments, "account", "principal"),
            )
            return _json_response(result)

        else:
            return _json_response({"error": f"Unknown tool: {name}"})

    except Exception as e:
        return _json_response({"error": str(e), "tool": name, "arguments": arguments})


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────

def main():
    """Run the MCP server with stdio transport."""
    async def run():
        global strategy_guard, striker_strategy
        strategy_guard = StrategyGuard(client, _get_trader, analysis)
        striker_strategy = StrikerStrategy(
            binance_client=client,
            get_trader_fn=_get_trader,
            copytrading_client=copytrading_client,
            ghost_flow_module=ghost_flow,
            scanner=scanner,
        )
        await strategy_guard.start_guard_loop()
        try:
            async with stdio_server() as (read_stream, write_stream):
                await server.run(read_stream, write_stream, server.create_initialization_options())
        finally:
            await copytrading_client.close()

    asyncio.run(run())


if __name__ == "__main__":
    main()
