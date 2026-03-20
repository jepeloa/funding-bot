"""
Liquidation Heatmap micro-service — FastAPI HTTP server.

Runs as a standalone process (port 8009 by default).
The main API (port 8008) proxies /heatmap/* requests here.
"""

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import Response

from .cache import HeatmapCache, CacheKey
from .config import (
    LISTEN_HOST, LISTEN_PORT,
    LOOKBACK_HOURS, IMG_WIDTH, IMG_HEIGHT, DEFAULT_FORMAT,
    CACHE_TTL_SECS,
)
from .engine import LiquidationHeatmapEngine, HeatmapData
from .renderer import render_heatmap

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("heatmap.server")

# ── Globals ──────────────────────────────────────────────────────

engine: LiquidationHeatmapEngine
cache: HeatmapCache


# ── Lifespan ─────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global engine, cache
    engine = LiquidationHeatmapEngine()
    cache = HeatmapCache(ttl=CACHE_TTL_SECS)
    await engine.start()
    cache.start_sweep()
    log.info("Heatmap engine started")

    # GPU warmup: run one generation to trigger CuPy JIT compilation
    # so the first real request doesn't hit a 30s+ cold start.
    asyncio.create_task(_gpu_warmup())

    yield
    cache.stop_sweep()
    await engine.stop()
    log.info("Heatmap engine stopped")


async def _gpu_warmup():
    """Pre-compile CuPy CUDA kernels with a real symbol."""
    try:
        syms = await engine.get_symbols()
        if syms:
            warmup_sym = "BTCUSDT" if "BTCUSDT" in syms else syms[0]
            log.info("GPU warmup: generating %s ...", warmup_sym)
            data = await engine.generate(warmup_sym, hours=1)
            # Also render to warm up matplotlib
            from .renderer import render_heatmap
            render_heatmap(data, width=400, height=300, fmt="png")
            log.info("GPU warmup complete (%.0fms)", data.compute_time_ms)
    except Exception as e:
        log.warning("GPU warmup failed (non-fatal): %s", e)


# ── App ──────────────────────────────────────────────────────────

app = FastAPI(
    title="Liquidation Heatmap Engine",
    version="1.0.0",
    description="GPU-accelerated liquidation heatmap generation",
    lifespan=lifespan,
)


# ══════════════════════════════════════════════════════════════════
#  ENDPOINTS
# ══════════════════════════════════════════════════════════════════

@app.get("/health")
async def health():
    """Health check."""
    from .engine import _use_gpu, xp
    gpu_info = "none"
    if _use_gpu:
        try:
            import cupy as cp
            gpu_info = f"{cp.cuda.runtime.getDeviceCount()} GPU(s)"
        except Exception:
            gpu_info = "error"
    return {
        "status": "ok",
        "gpu": gpu_info,
        "cache_entries": cache.size,
        "cache_memory_kb": round(cache.memory_bytes / 1024, 1),
    }


@app.get("/heatmap/{symbol}")
async def heatmap(
    symbol: str,
    hours: int = Query(LOOKBACK_HOURS, ge=1, le=24,
                       description="Lookback window in hours"),
    width: int = Query(IMG_WIDTH, ge=400, le=3840,
                       description="Image width in pixels"),
    height: int = Query(IMG_HEIGHT, ge=300, le=2160,
                        description="Image height in pixels"),
    format: str = Query(DEFAULT_FORMAT, pattern="^(webp|png)$",
                        description="Image format"),
    nocache: bool = Query(False, description="Force regeneration"),
):
    """Generate a liquidation heatmap for a symbol."""
    symbol = symbol.upper()
    key: CacheKey = (symbol, hours, width, height, format)

    # Check cache
    if not nocache:
        cached = cache.get(key)
        if cached is not None:
            log.debug("Cache hit: %s", symbol)
            return _image_response(cached, format)

    # Generate (with retry on timeout)
    last_err = None
    for attempt in range(2):
        try:
            data: HeatmapData = await engine.generate(symbol, hours=hours)
            break
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except (TimeoutError, asyncio.TimeoutError) as e:
            last_err = e
            log.warning("Timeout generating %s (attempt %d/2)",
                        symbol, attempt + 1)
            await asyncio.sleep(0.5)
            continue
        except Exception as e:
            log.exception("Failed to generate heatmap for %s", symbol)
            raise HTTPException(status_code=500,
                                detail=f"Generation failed: {e}")
    else:
        log.error("All retries exhausted for %s", symbol)
        raise HTTPException(status_code=504,
                            detail=f"Generation timed out after 2 attempts: {last_err}")

    # Render
    try:
        img_bytes = render_heatmap(data, width=width, height=height, fmt=format)
    except Exception as e:
        log.exception("Failed to render heatmap for %s", symbol)
        raise HTTPException(status_code=500,
                            detail=f"Rendering failed: {e}")

    # Cache
    cache.put(key, img_bytes)

    return _image_response(img_bytes, format)


@app.get("/heatmap/{symbol}/data")
async def heatmap_data(
    symbol: str,
    hours: int = Query(LOOKBACK_HOURS, ge=1, le=24),
):
    """Return raw heatmap data as JSON (for advanced consumers)."""
    symbol = symbol.upper()
    try:
        data: HeatmapData = await engine.generate(symbol, hours=hours)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        log.exception("Failed to generate data for %s", symbol)
        raise HTTPException(status_code=500, detail=str(e))

    # Convert to JSON-serializable format
    price_axis = [
        data.price_min + i * (data.price_max - data.price_min) / data.price_bins
        for i in range(data.price_bins)
    ]

    return {
        "symbol": data.symbol,
        "mark_price": data.mark_price,
        "oi_value": data.oi_value,
        "price_axis": price_axis,
        "liq_long": data.liq_long.tolist(),
        "liq_short": data.liq_short.tolist(),
        "time_start": data.time_start,
        "time_end": data.time_end,
        "lookback_hours": data.lookback_hours,
        "compute_ms": round(data.compute_time_ms, 1),
    }


@app.get("/summary")
async def summary(
    top_n: int = Query(20, ge=1, le=100,
                       description="Number of symbols to return"),
):
    """Top symbols by estimated liquidation density."""
    try:
        results = await engine.get_summary(top_n=top_n)
    except Exception as e:
        log.exception("Summary generation failed")
        raise HTTPException(status_code=500, detail=str(e))
    return {"count": len(results), "symbols": results}


@app.get("/symbols")
async def symbols():
    """List all symbols with available OHLCV data."""
    syms = await engine.get_symbols()
    return {"count": len(syms), "symbols": syms}


@app.get("/cache/stats")
async def cache_stats():
    """Cache statistics."""
    return {
        "entries": cache.size,
        "memory_bytes": cache.memory_bytes,
        "memory_kb": round(cache.memory_bytes / 1024, 1),
        "ttl_secs": CACHE_TTL_SECS,
    }


@app.delete("/cache")
async def cache_clear(symbol: Optional[str] = None):
    """Clear cache — all or for a specific symbol."""
    cache.invalidate(symbol)
    return {"status": "cleared", "symbol": symbol or "all"}


# ══════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════

def _image_response(img_bytes: bytes, fmt: str) -> Response:
    media_type = "image/webp" if fmt == "webp" else "image/png"
    return Response(
        content=img_bytes,
        media_type=media_type,
        headers={
            "Cache-Control": f"public, max-age={CACHE_TTL_SECS}",
            "X-Heatmap-Size": str(len(img_bytes)),
        },
    )


# ══════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "liquidation_heatmap.server:app",
        host=LISTEN_HOST,
        port=LISTEN_PORT,
        log_level="info",
        access_log=True,
    )
