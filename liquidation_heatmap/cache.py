"""
In-memory TTL cache for rendered heatmap images.
Thread-safe via asyncio (single-threaded event loop).
"""

import asyncio
import logging
import time
from typing import Optional

from .config import CACHE_TTL_SECS, CACHE_SWEEP_INTERVAL

log = logging.getLogger("heatmap.cache")

CacheKey = tuple[str, int, int, int, str]  # (symbol, hours, width, height, fmt)


class HeatmapCache:
    """Simple TTL cache: key → (image_bytes, generated_at)."""

    def __init__(self, ttl: float = CACHE_TTL_SECS):
        self._store: dict[CacheKey, tuple[bytes, float]] = {}
        self._ttl = ttl
        self._sweep_task: Optional[asyncio.Task] = None

    # ── Public API ───────────────────────────────────────────────

    def get(self, key: CacheKey) -> Optional[bytes]:
        """Return cached image bytes if still fresh, else None."""
        entry = self._store.get(key)
        if entry is None:
            return None
        img_bytes, gen_at = entry
        if time.time() - gen_at > self._ttl:
            del self._store[key]
            return None
        return img_bytes

    def put(self, key: CacheKey, img_bytes: bytes):
        """Store rendered image with current timestamp."""
        self._store[key] = (img_bytes, time.time())

    def invalidate(self, symbol: Optional[str] = None):
        """Remove entries — all if symbol is None, else only matching symbol."""
        if symbol is None:
            self._store.clear()
            return
        sym_upper = symbol.upper()
        to_del = [k for k in self._store if k[0] == sym_upper]
        for k in to_del:
            del self._store[k]

    @property
    def size(self) -> int:
        return len(self._store)

    @property
    def memory_bytes(self) -> int:
        return sum(len(v[0]) for v in self._store.values())

    # ── Background sweep ─────────────────────────────────────────

    def start_sweep(self):
        """Start periodic cleanup of expired entries."""
        if self._sweep_task is None or self._sweep_task.done():
            self._sweep_task = asyncio.create_task(self._sweep_loop())

    def stop_sweep(self):
        if self._sweep_task and not self._sweep_task.done():
            self._sweep_task.cancel()

    async def _sweep_loop(self):
        while True:
            try:
                await asyncio.sleep(CACHE_SWEEP_INTERVAL)
                self._evict_expired()
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("Cache sweep error")

    def _evict_expired(self):
        now = time.time()
        expired = [k for k, (_, gen_at) in self._store.items()
                   if now - gen_at > self._ttl]
        for k in expired:
            del self._store[k]
        if expired:
            log.debug("Evicted %d expired heatmap(s), %d remaining",
                      len(expired), len(self._store))
