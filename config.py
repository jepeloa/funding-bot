"""
Configuración del recorder de Binance Futures USDT-M.
Graba TODOS los pares + ejecuta estrategia α_f Bifurcation Short (4 variantes).
"""
import json
import os

# ══════════════════════════════════════════════════════════════════
#  SÍMBOLOS — se descubren automáticamente de la API REST
# ══════════════════════════════════════════════════════════════════
SYMBOLS: list[str] = []

# ══════════════════════════════════════════════════════════════════
#  ENDPOINTS DE BINANCE FUTURES USDT-M
# ══════════════════════════════════════════════════════════════════
BINANCE_FUTURES_WS_BASE = "wss://fstream.binance.com"
BINANCE_FUTURES_REST = "https://fapi.binance.com"

# ══════════════════════════════════════════════════════════════════
#  STREAMS
# ══════════════════════════════════════════════════════════════════
DEPTH_LEVELS = 20
DEPTH_SPEED = "100ms"
MAX_STREAMS_PER_WS = 190

# ══════════════════════════════════════════════════════════════════
#  BASE DE DATOS — TimescaleDB (Docker) en disco dedicado 4TB
# ══════════════════════════════════════════════════════════════════
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "binance_futures")
DB_USER = os.getenv("DB_USER", "recorder")
DB_PASSWORD = os.getenv("DB_PASSWORD", "recorder")
DB_POOL_MIN = 5
DB_POOL_MAX = 20
BATCH_SIZE = 1000

# Directorio para archivos auxiliares (PID file, etc.)
DATA_DIR = "/media/mapplics-ia/recorder-data"

# ══════════════════════════════════════════════════════════════════
#  RECONEXIÓN
# ══════════════════════════════════════════════════════════════════
RECONNECT_DELAY_SECS = 3
MAX_RECONNECT_ATTEMPTS = 0

# ══════════════════════════════════════════════════════════════════
#  TELEGRAM
# ══════════════════════════════════════════════════════════════════
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8004368637:AAGqon7LK-z_VG8V5Meg2D2UQg3zFsCiUhg")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "6834861066")
TELEGRAM_STATUS_INTERVAL = int(os.getenv("TELEGRAM_STATUS_INTERVAL", "3600"))  # secs

# ══════════════════════════════════════════════════════════════════
#  POLLING REST (OI, Funding)
# ══════════════════════════════════════════════════════════════════
OI_POLL_INTERVAL_SECS = 15
OI_PAUSE_BETWEEN_SYMBOLS = 0.05
FUNDING_POLL_INTERVAL_SECS = 300

# ── Sentiment polling (L/S ratio + Taker volume) ──
# Polleamos solo top N símbolos (rate limit: 2400 weight/min)
LSR_POLL_INTERVAL_SECS = 300          # cada 5 min
TAKER_VOL_POLL_INTERVAL_SECS = 300    # cada 5 min
SENTIMENT_PAUSE_BETWEEN = 0.1         # pausa entre calls REST
SENTIMENT_TOP_N = 50                  # top 50 símbolos por volumen
#  Los top symbols se actualizan automáticamente al arrancar

# ── Funding Info polling (intervalo dinámico por activo) ──
FUNDING_INFO_POLL_INTERVAL_SECS = 3600  # cada 1h consulta /fapi/v1/fundingInfo

# ══════════════════════════════════════════════════════════════════
#  LOGGING
# ══════════════════════════════════════════════════════════════════
LOG_LEVEL = "INFO"

# ══════════════════════════════════════════════════════════════════
#  CAPITAL Y FEES (compartido por todas las variantes)
# ══════════════════════════════════════════════════════════════════
INITIAL_CAPITAL = 10_000.0
TAKER_FEE = 0.0004
STRATEGY_VERSION = "v2"   # v2 = score4 + funding hard + tighter SL + concurrency throttle + adaptive sizing

# ══════════════════════════════════════════════════════════════════
#  SCORE Ŝ_αf — Umbrales universales (Paper Table 1)
#
#  Compartidos por TODAS las variantes: el score mide si el spring
#  está invertido, independiente de la agresividad del trade.
# ══════════════════════════════════════════════════════════════════
STRATEGY = {
    # ── Score components (Paper Table 1) ──
    "funding_rate_half": 0.0001,    # r ≥ 0.01% → c_fund = 0.5
    "funding_rate_full": 0.0005,    # r ≥ 0.05% → c_fund = 1.0
    "oi_growth_24h_half": 0.025,    # ΔOI ≥ 2.5% → c_oi = 0.5
    "oi_growth_24h_full": 0.05,     # ΔOI ≥ 5%   → c_oi = 1.0
    "price_pump_12h_half": 0.015,   # ΔP ≥ 1.5%  → c_price = 0.5
    "price_pump_12h_full": 0.03,    # ΔP ≥ 3%    → c_price = 1.0
    "taker_buy_ratio_half": 0.52,   # η ≥ 52%    → c_taker = 0.5
    "taker_buy_ratio_full": 0.55,   # η ≥ 55%    → c_taker = 1.0
    "volume_spike_half": 1.4,       # V/V̄ ≥ 1.4× → c_vol = 0.5
    "volume_spike_full": 2.0,       # V/V̄ ≥ 2×   → c_vol = 1.0
    # ── Energy accumulation (Paper Eq. 10) ──
    "score_threshold": 3.0,         # Ŝ ≥ 3 → spring inverted → acumula
    # ── Filtro Régimen A (Paper Eq. 18 — compartido) ──
    "regime_a_oi_growth": 0.0,      # ΔOI > 0
    "regime_a_taker_buy": 0.60,     # η > 60%
    "regime_a_vol_spike": 3.0,      # V/V̄ > 3×
    # ── Daily loss halt (Paper §8.5 — compartido) ──
    "max_daily_loss_pct": 0.05,     # 5% equity
    # ── Concurrent entry throttle (v2) ──
    "concurrent_entry_max": 2,       # max open trades across all symbols per variant within 2h window
    "concurrent_entry_window_h": 2,  # window to count concurrent entries (hours)
}

# ══════════════════════════════════════════════════════════════════
#  VARIANTES DE LA ESTRATEGIA (Paper Table 5)
#
#  Cada variante define umbrales de entrada, salida y sizing propios.
#  El score/energy/exhaustion son compartidos (calculados una sola vez
#  con los umbrales de Table 1).
# ══════════════════════════════════════════════════════════════════
VARIANTS = {
    "conservative": {
        # ── Entrada (Paper Table 5, umbrales adicionales al score) ──
        "entry_funding_min": 0.00015,   # r ≥ 0.015%
        "entry_oi_growth_min": 0.08,    # ΔOI ≥ 8%
        "entry_price_pump_min": 0.05,   # ΔP ≥ 5%
        "entry_vol_ratio_min": 2.0,     # V/V̄ ≥ 2×
        "energy_min_hours": 6.0,        # E ≥ 6h
        "entry_score_min": 2.5,         # Ŝ ≥ 2.5
        "entry_exhaustion_min": 3,      # Ê ≥ 3 (was 2 — too many premature entries)
        # ── Sizing ──
        "capital_fraction": 0.05,
        "leverage": 3,
        # ── Salida ──
        "stop_loss_pct": 0.03,          # 3%
        "take_profit_pct": 0.10,        # 10%
        "max_hold_hours": 36,           # 36h
        "min_hold_hours": 4,            # 4h (was 1h — avoid premature reversal exit)
        "oi_abort_pct": 0.15,           # 15% (was 10% — too sensitive on altcoins)
        "cooldown_hours": 36,           # 36h
        # ── Trailing stop (new) ──
        "breakeven_trigger_pct": 0.02,  # move stop to breakeven at MFE ≥ 2%
        "trailing_activation_pct": 0.05, # activate trailing at MFE ≥ 5%
        "trailing_callback_pct": 0.50,  # trail = MFE × (1 - 0.50) → keep 50% of gains
    },
    "base": {
        "entry_funding_min": 0.0001,    # r ≥ 0.01%
        "entry_oi_growth_min": 0.05,    # ΔOI ≥ 5%
        "entry_price_pump_min": 0.03,   # ΔP ≥ 3%
        "entry_vol_ratio_min": 2.0,     # V/V̄ ≥ 2×
        "energy_min_hours": 6.0,
        "entry_score_min": 2.5,
        "entry_exhaustion_min": 3,      # was 2
        "capital_fraction": 0.05,
        "leverage": 5,
        "stop_loss_pct": 0.05,          # 5%
        "take_profit_pct": 0.15,        # 15%
        "max_hold_hours": 48,           # 48h
        "min_hold_hours": 4,            # was 1h
        "oi_abort_pct": 0.12,           # 12% (was 8% — too sensitive)
        "cooldown_hours": 24,           # 24h
        "breakeven_trigger_pct": 0.02,
        "trailing_activation_pct": 0.05,
        "trailing_callback_pct": 0.50,
    },
    "aggressive": {
        "entry_funding_min": 0.00008,   # r ≥ 0.008%
        "entry_oi_growth_min": 0.03,    # ΔOI ≥ 3%
        "entry_price_pump_min": 0.02,   # ΔP ≥ 2%
        "entry_vol_ratio_min": 2.0,     # V/V̄ ≥ 2×
        "energy_min_hours": 6.0,
        "entry_score_min": 4.0,         # v2: was 2.5 — score<4 trades are net negative
        "entry_exhaustion_min": 3,      # was 2
        "entry_funding_score_min": 0.5, # v2: c_fund ≥ 0.5 (hard filter — no funding = no spring)
        "capital_fraction": 0.05,       # base fraction (scaled by score in v2)
        "capital_fraction_high": 0.075, # v2: 7.5% for score ≥ 4.5
        "leverage": 7,
        "stop_loss_pct": 0.05,          # v2: was 7% — losers avg MAE=-6.2%, winners -1.9%
        "take_profit_pct": 0.20,        # 20%
        "max_hold_hours": 72,           # 72h
        "min_hold_hours": 4,            # was 1h
        "oi_abort_pct": 0.10,           # 10% (was 6% — way too sensitive)
        "cooldown_hours": 12,           # 12h
        "breakeven_trigger_pct": 0.02,
        "trailing_activation_pct": 0.05,
        "trailing_callback_pct": 0.50,
    },
    "high_energy": {
        "entry_funding_min": 0.0001,    # r ≥ 0.01%
        "entry_oi_growth_min": 0.10,    # ΔOI ≥ 10%
        "entry_price_pump_min": 0.05,   # ΔP ≥ 5%
        "entry_vol_ratio_min": 3.0,     # V/V̄ ≥ 3×
        "energy_min_hours": 6.0,
        "entry_score_min": 2.5,
        "entry_exhaustion_min": 3,      # was 2
        "capital_fraction": 0.05,
        "leverage": 5,                  # was 10 — too much leverage, losses amplified
        "stop_loss_pct": 0.04,          # 4%
        "take_profit_pct": 0.25,        # 25%
        "max_hold_hours": 24,           # 24h
        "min_hold_hours": 4,            # was 1h
        "oi_abort_pct": 0.15,           # 15% (was 12%)
        "cooldown_hours": 48,           # 48h
        "breakeven_trigger_pct": 0.02,
        "trailing_activation_pct": 0.05,
        "trailing_callback_pct": 0.50,
    },
}

RECORDING = {
    "pre_record_score": 2.0,
    "post_close_tail_secs": 1800,
}

# ══════════════════════════════════════════════════════════════════
#  MODO DE OPERACIÓN
#
#  "dry-run"  → trades virtuales (paper), solo base de datos local
#  "live"     → trades reales en Binance Futures vía API
#
#  Se persiste en trading_config.json para hot-reload entre procesos.
# ══════════════════════════════════════════════════════════════════
TRADING_CONFIG_FILE = os.path.join(os.path.dirname(__file__), "trading_config.json")

def load_trading_config() -> dict:
    """Lee trading_config.json. Retorna defaults si no existe."""
    try:
        with open(TRADING_CONFIG_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"trading_mode": "dry-run", "active_account": "principal", "active_variant": "base"}

def save_trading_config(mode: str, account: str, variant: str = "base"):
    """Escribe trading_config.json (atómico vía rename)."""
    data = {"trading_mode": mode, "active_account": account, "active_variant": variant}
    tmp = TRADING_CONFIG_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f)
    os.replace(tmp, TRADING_CONFIG_FILE)

_tc = load_trading_config()
TRADING_MODE = _tc.get("trading_mode", "dry-run")
ACTIVE_ACCOUNT = _tc.get("active_account", "principal")
ACTIVE_VARIANT = _tc.get("active_variant", "base")

# ══════════════════════════════════════════════════════════════════
#  CUENTAS DE BINANCE FUTURES
# ══════════════════════════════════════════════════════════════════
BINANCE_ACCOUNTS = {
    "principal": {
        "label": "Cuenta Principal",
        "api_key": os.getenv(
            "BINANCE_API_KEY",
            "lSPFfpYRgc8t5fcZo8sMz7tUu6Lxe9Eqjf9sWrZsGjxS2lqVzuNljhWUm2UwK0Ac",
        ),
        "api_secret": os.getenv(
            "BINANCE_API_SECRET",
            "DHTrUkKXe48UV4DQmW6zvSr9HBc1xRccGXcm6Itzk8wTgm02qVHgjlLxc7jA9jUo",
        ),
    },
    "copytrading": {
        "label": "Cuenta CopyTrading",
        "api_key": os.getenv(
            "BINANCE_CT_API_KEY",
            "S9GHdLJOMwhkmwRTmairz4ZlgKm7lEEB2sSlYF7NqxTvGgadahXGXQmALVytqYTv",
        ),
        "api_secret": os.getenv(
            "BINANCE_CT_API_SECRET",
            "c69BGTXj2MdVN7ITJQ48geIl79bIVk4Gh0ikMSjZG7GQIPAGUcQhQWMcrmCXVjgS",
        ),
    },
    "copytrading_privado": {
        "label": "CopyTrading Privado",
        "api_key": os.getenv(
            "BINANCE_CTP_API_KEY",
            "pUbQMizPd79wksXwhMEfjKzsgi7ZH21G1CI1q2SieEaG4fpSRv7jBwKgJuQDC9T8",
        ),
        "api_secret": os.getenv(
            "BINANCE_CTP_API_SECRET",
            "kkQMcbVSRaYlSIXgtU8qXdIleSAUip0Fd9641rXhkqCQxkTBluHx09QQpe3nmlKA",
        ),
    },
}
