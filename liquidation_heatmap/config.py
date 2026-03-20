"""
Configuration for the Liquidation Heatmap Engine.
"""
import os

# ══════════════════════════════════════════════════════════════════
#  DATABASE (same as main recorder)
# ══════════════════════════════════════════════════════════════════
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "binance_futures")
DB_USER = os.getenv("DB_USER", "recorder")
DB_PASSWORD = os.getenv("DB_PASSWORD", "recorder")
DB_POOL_MIN = 2
DB_POOL_MAX = 10

# ══════════════════════════════════════════════════════════════════
#  HEATMAP PARAMETERS
# ══════════════════════════════════════════════════════════════════

# Lookback window
LOOKBACK_HOURS = 6

# Cache
CACHE_TTL_SECS = 300          # 5 minutes
CACHE_SWEEP_INTERVAL = 60     # cleanup stale entries every 60s

# Price axis
PRICE_RANGE_PCT = 0.15        # ±15% of mark price
PRICE_BINS = 400              # vertical resolution (higher = finer bands)

# Time axis
TIME_BINS = 360               # 1 bin per minute for 6h

# Leverage distribution — weights must sum to 1.0
# Empirical distribution based on market studies.
# Keys = leverage multiplier, values = fraction of OI at that leverage.
LEVERAGE_TIERS: dict[int, float] = {
    5:   0.30,
    10:  0.25,
    25:  0.20,
    50:  0.15,
    100: 0.10,
}

# Maintenance margin rate (Binance default for most symbols)
MAINTENANCE_MARGIN = 0.004    # 0.4%

# ══════════════════════════════════════════════════════════════════
#  RENDERING
# ══════════════════════════════════════════════════════════════════
IMG_WIDTH = 1400
IMG_HEIGHT = 900
DEFAULT_FORMAT = "webp"       # "webp" or "png"
WEBP_QUALITY = 85

# ══════════════════════════════════════════════════════════════════
#  GPU
# ══════════════════════════════════════════════════════════════════
GPU_DEVICES: list[int] = [int(d) for d in os.getenv(
    "CUDA_VISIBLE_DEVICES", "0,1"
).split(",") if d.strip()]

# Falls back to NumPy if no GPU is available
FORCE_CPU = os.getenv("HEATMAP_FORCE_CPU", "0") == "1"

# ══════════════════════════════════════════════════════════════════
#  SERVER
# ══════════════════════════════════════════════════════════════════
LISTEN_HOST = os.getenv("HEATMAP_HOST", "127.0.0.1")
LISTEN_PORT = int(os.getenv("HEATMAP_PORT", "8009"))
