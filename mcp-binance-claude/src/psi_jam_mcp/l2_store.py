"""
L2 Historical Data Store
========================
Persistent storage and background collector for order book (L2) snapshots.

Uses SQLite for zero-dependency persistence. Data is stored at:
  ~/.psi_jam_mcp/l2_history.db

Architecture:
  - L2Store: SQLite read/write layer
  - L2Collector: asyncio background task that polls Binance orderbook
    at a configurable interval and saves snapshots automatically.

Usage from MCP:
  1. start_l2_recording(symbol, interval_sec)  → begins background capture
  2. stop_l2_recording(symbol)                 → stops capture
  3. get_l2_history(symbol, ...)               → query historical snapshots
  4. get_l2_recording_status()                 → list active recordings
"""

import asyncio
import json
import sqlite3
import time
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ─────────────────────────────────────────────
# DEFAULT CONFIG
# ─────────────────────────────────────────────

DEFAULT_DB_DIR = Path.home() / ".psi_jam_mcp"
DEFAULT_DB_PATH = DEFAULT_DB_DIR / "l2_history.db"
DEFAULT_POLL_INTERVAL = 30  # seconds
DEFAULT_DEPTH = 20          # orderbook levels to capture


# ─────────────────────────────────────────────
# SQLITE STORE
# ─────────────────────────────────────────────

class L2Store:
    """SQLite-backed store for L2 order book snapshots."""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = Path(db_path) if db_path else DEFAULT_DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self):
        """Create tables if they don't exist."""
        conn = self._connect()
        # Core tables — create with minimal schema first (compatible with old DBs)
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS l2_snapshots (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol      TEXT    NOT NULL,
                timestamp   INTEGER NOT NULL,
                mid_price   REAL,
                spread      REAL,
                spread_bps  REAL,
                bid_depth   REAL,
                ask_depth   REAL,
                depth_ratio REAL,
                imbalance   REAL,
                bid_walls   TEXT,
                ask_walls   TEXT,
                top_bids    TEXT,
                top_asks    TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_l2_symbol_ts
                ON l2_snapshots (symbol, timestamp);

            CREATE TABLE IF NOT EXISTS l2_recordings (
                symbol        TEXT PRIMARY KEY,
                interval_sec  INTEGER NOT NULL,
                depth         INTEGER NOT NULL,
                started_at    INTEGER NOT NULL,
                snapshots     INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS l2_sessions (
                session_id   TEXT PRIMARY KEY,
                symbol       TEXT    NOT NULL,
                started_at   INTEGER NOT NULL,
                stopped_at   INTEGER,
                interval_sec INTEGER NOT NULL,
                depth        INTEGER NOT NULL,
                snapshots    INTEGER DEFAULT 0,
                label        TEXT
            );
        """)
        # Migrate: add columns that may be missing from older DB versions
        self._migrate(conn)
        conn.commit()
        conn.close()

    def _migrate(self, conn: sqlite3.Connection):
        """Add columns/indexes that may be missing from older DB versions."""
        # l2_snapshots.session_id
        cursor = conn.execute("PRAGMA table_info(l2_snapshots)")
        columns = {row[1] for row in cursor.fetchall()}
        if "session_id" not in columns:
            conn.execute("ALTER TABLE l2_snapshots ADD COLUMN session_id TEXT")

        # l2_recordings.session_id
        cursor = conn.execute("PRAGMA table_info(l2_recordings)")
        columns = {row[1] for row in cursor.fetchall()}
        if "session_id" not in columns:
            conn.execute("ALTER TABLE l2_recordings ADD COLUMN session_id TEXT")

        # Index on session_id (safe to run after column exists)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_l2_session ON l2_snapshots (session_id)")

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(str(self.db_path))

    # ── WRITE ──

    def save_snapshot(self, symbol: str, orderbook: dict, session_id: Optional[str] = None):
        """Persist a single L2 snapshot from BinanceClient.get_orderbook_snapshot output."""
        metrics = orderbook.get("metrics", {})
        conn = self._connect()
        conn.execute(
            """
            INSERT INTO l2_snapshots
                (symbol, session_id, timestamp, mid_price, spread, spread_bps,
                 bid_depth, ask_depth, depth_ratio, imbalance,
                 bid_walls, ask_walls, top_bids, top_asks)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                symbol.upper(),
                session_id,
                orderbook.get("timestamp", int(time.time() * 1000)),
                metrics.get("mid_price"),
                metrics.get("spread"),
                metrics.get("spread_bps"),
                metrics.get("bid_depth_total"),
                metrics.get("ask_depth_total"),
                metrics.get("depth_ratio"),
                metrics.get("top10_imbalance"),
                json.dumps(metrics.get("bid_walls", [])),
                json.dumps(metrics.get("ask_walls", [])),
                json.dumps(orderbook.get("top5_bids", [])),
                json.dumps(orderbook.get("top5_asks", [])),
            ),
        )
        # Update counters in both recordings and sessions tables
        conn.execute(
            "UPDATE l2_recordings SET snapshots = snapshots + 1 WHERE symbol = ?",
            (symbol.upper(),),
        )
        if session_id:
            conn.execute(
                "UPDATE l2_sessions SET snapshots = snapshots + 1 WHERE session_id = ?",
                (session_id,),
            )
        conn.commit()
        conn.close()

    def register_recording(self, symbol: str, interval_sec: int, depth: int, session_id: Optional[str] = None) -> str:
        """Register that a recording is active. Returns the session_id."""
        if not session_id:
            session_id = self._generate_session_id(symbol)

        now_ms = int(time.time() * 1000)
        conn = self._connect()
        conn.execute(
            """
            INSERT OR REPLACE INTO l2_recordings (symbol, session_id, interval_sec, depth, started_at, snapshots)
            VALUES (?, ?, ?, ?, ?, 0)
            """,
            (symbol.upper(), session_id, interval_sec, depth, now_ms),
        )
        conn.execute(
            """
            INSERT INTO l2_sessions (session_id, symbol, started_at, interval_sec, depth, snapshots)
            VALUES (?, ?, ?, ?, ?, 0)
            """,
            (session_id, symbol.upper(), now_ms, interval_sec, depth),
        )
        conn.commit()
        conn.close()
        return session_id

    @staticmethod
    def _generate_session_id(symbol: str) -> str:
        """Generate a human-friendly session id: SYMBOL_YYYYMMDD_HHMMSS_xxxx."""
        now = datetime.now(timezone.utc)
        short_uuid = uuid.uuid4().hex[:4]
        return f"{symbol.upper()}_{now.strftime('%Y%m%d_%H%M%S')}_{short_uuid}"

    def unregister_recording(self, symbol: str):
        """Remove a recording entry and finalize the session."""
        conn = self._connect()
        # Mark session as stopped
        row = conn.execute(
            "SELECT session_id FROM l2_recordings WHERE symbol = ?", (symbol.upper(),)
        ).fetchone()
        if row and row[0]:
            conn.execute(
                "UPDATE l2_sessions SET stopped_at = ? WHERE session_id = ?",
                (int(time.time() * 1000), row[0]),
            )
        conn.execute("DELETE FROM l2_recordings WHERE symbol = ?", (symbol.upper(),))
        conn.commit()
        conn.close()

    # ── READ ──

    @staticmethod
    def _ms_to_iso(ms: Optional[int]) -> Optional[str]:
        """Convert ms epoch to ISO 8601 UTC string."""
        if ms is None:
            return None
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    def get_history(
        self,
        symbol: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        session_id: Optional[str] = None,
        limit: int = 500,
        metrics_only: bool = False,
    ) -> list[dict]:
        """
        Query historical L2 snapshots.

        Args:
            symbol: Trading pair
            start_time: Start epoch ms (inclusive)
            end_time: End epoch ms (inclusive)
            session_id: Filter by recording session
            limit: Max rows to return
            metrics_only: If True, omit bid/ask level details
        """
        conn = self._connect()
        conn.row_factory = sqlite3.Row

        query = "SELECT * FROM l2_snapshots WHERE symbol = ?"
        params: list = [symbol.upper()]

        if session_id:
            query += " AND session_id = ?"
            params.append(session_id)
        if start_time:
            query += " AND timestamp >= ?"
            params.append(start_time)
        if end_time:
            query += " AND timestamp <= ?"
            params.append(end_time)

        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)

        rows = conn.execute(query, params).fetchall()
        conn.close()

        results = []
        for row in reversed(rows):  # chronological order
            entry = {
                "timestamp": row["timestamp"],
                "datetime": self._ms_to_iso(row["timestamp"]),
                "session_id": row["session_id"],
                "mid_price": row["mid_price"],
                "spread": row["spread"],
                "spread_bps": row["spread_bps"],
                "bid_depth": row["bid_depth"],
                "ask_depth": row["ask_depth"],
                "depth_ratio": row["depth_ratio"],
                "imbalance": row["imbalance"],
                "bid_walls": json.loads(row["bid_walls"]) if row["bid_walls"] else [],
                "ask_walls": json.loads(row["ask_walls"]) if row["ask_walls"] else [],
            }
            if not metrics_only:
                entry["top_bids"] = json.loads(row["top_bids"]) if row["top_bids"] else []
                entry["top_asks"] = json.loads(row["top_asks"]) if row["top_asks"] else []
            results.append(entry)

        return results

    def get_sessions(self, symbol: Optional[str] = None) -> list[dict]:
        """List all recorded sessions with human-readable dates."""
        conn = self._connect()
        conn.row_factory = sqlite3.Row

        if symbol:
            rows = conn.execute(
                "SELECT * FROM l2_sessions WHERE symbol = ? ORDER BY started_at DESC",
                (symbol.upper(),),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM l2_sessions ORDER BY started_at DESC"
            ).fetchall()

        conn.close()

        results = []
        for r in rows:
            started = r["started_at"]
            stopped = r["stopped_at"]
            duration_min = None
            if started and stopped:
                duration_min = round((stopped - started) / 60000, 1)

            results.append({
                "session_id": r["session_id"],
                "symbol": r["symbol"],
                "started_at": started,
                "started_at_human": self._ms_to_iso(started),
                "stopped_at": stopped,
                "stopped_at_human": self._ms_to_iso(stopped),
                "duration_minutes": duration_min,
                "active": stopped is None,
                "interval_sec": r["interval_sec"],
                "depth": r["depth"],
                "snapshots": r["snapshots"],
                "label": r["label"],
            })

        return results

    def get_recordings(self) -> list[dict]:
        """List all registered recordings."""
        conn = self._connect()
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM l2_recordings").fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_stats(self, symbol: Optional[str] = None) -> dict:
        """Get storage statistics."""
        conn = self._connect()

        if symbol:
            row = conn.execute(
                "SELECT COUNT(*) as cnt, MIN(timestamp) as first_ts, MAX(timestamp) as last_ts "
                "FROM l2_snapshots WHERE symbol = ?",
                (symbol.upper(),),
            ).fetchone()
            symbols_info = [{
                "symbol": symbol.upper(),
                "snapshots": row[0],
                "first_timestamp": row[1],
                "last_timestamp": row[2],
            }]
        else:
            rows = conn.execute(
                "SELECT symbol, COUNT(*) as cnt, MIN(timestamp) as first_ts, MAX(timestamp) as last_ts "
                "FROM l2_snapshots GROUP BY symbol"
            ).fetchall()
            symbols_info = [
                {
                    "symbol": r[0],
                    "snapshots": r[1],
                    "first_timestamp": r[2],
                    "last_timestamp": r[3],
                }
                for r in rows
            ]

        total = conn.execute("SELECT COUNT(*) FROM l2_snapshots").fetchone()[0]
        db_size_mb = round(os.path.getsize(self.db_path) / (1024 * 1024), 2) if self.db_path.exists() else 0

        conn.close()
        return {
            "db_path": str(self.db_path),
            "db_size_mb": db_size_mb,
            "total_snapshots": total,
            "symbols": symbols_info,
        }

    def purge(self, symbol: Optional[str] = None, older_than_ms: Optional[int] = None):
        """Delete old snapshots to manage storage."""
        conn = self._connect()
        if symbol and older_than_ms:
            conn.execute(
                "DELETE FROM l2_snapshots WHERE symbol = ? AND timestamp < ?",
                (symbol.upper(), older_than_ms),
            )
        elif symbol:
            conn.execute("DELETE FROM l2_snapshots WHERE symbol = ?", (symbol.upper(),))
        elif older_than_ms:
            conn.execute("DELETE FROM l2_snapshots WHERE timestamp < ?", (older_than_ms,))

        deleted = conn.execute("SELECT changes()").fetchone()[0]
        conn.execute("VACUUM")
        conn.commit()
        conn.close()
        return deleted


# ─────────────────────────────────────────────
# BACKGROUND COLLECTOR
# ─────────────────────────────────────────────

class L2Collector:
    """
    Async background collector that periodically captures L2 snapshots.

    Each symbol gets its own asyncio task. Tasks are managed via
    start()/stop() methods.
    """

    def __init__(self, binance_client, store: Optional[L2Store] = None):
        self.client = binance_client
        self.store = store or L2Store()
        self._tasks: dict[str, asyncio.Task] = {}    # symbol → Task
        self._sessions: dict[str, str] = {}           # symbol → session_id

    async def start(
        self,
        symbol: str,
        interval_sec: int = DEFAULT_POLL_INTERVAL,
        depth: int = DEFAULT_DEPTH,
    ) -> dict:
        """
        Start recording L2 snapshots for a symbol.
        Creates an asyncio background task that polls every interval_sec seconds.
        """
        sym = symbol.upper()

        if sym in self._tasks and not self._tasks[sym].done():
            return {
                "status": "already_recording",
                "symbol": sym,
                "message": f"{sym} is already being recorded.",
            }

        session_id = self.store.register_recording(sym, interval_sec, depth)
        self._sessions[sym] = session_id

        task = asyncio.create_task(
            self._poll_loop(sym, interval_sec, depth, session_id),
            name=f"l2_collector_{sym}",
        )
        self._tasks[sym] = task

        return {
            "status": "started",
            "symbol": sym,
            "session_id": session_id,
            "interval_sec": interval_sec,
            "depth": depth,
            "message": f"Recording L2 for {sym} every {interval_sec}s (depth={depth}). Session: {session_id}",
        }

    async def stop(self, symbol: str) -> dict:
        """Stop recording a symbol."""
        sym = symbol.upper()

        if sym not in self._tasks:
            return {
                "status": "not_recording",
                "symbol": sym,
                "message": f"{sym} is not being recorded.",
            }

        self._tasks[sym].cancel()
        try:
            await self._tasks[sym]
        except asyncio.CancelledError:
            pass

        session_id = self._sessions.pop(sym, None)
        del self._tasks[sym]
        self.store.unregister_recording(sym)

        return {
            "status": "stopped",
            "symbol": sym,
            "session_id": session_id,
            "message": f"Stopped recording {sym}. Session {session_id} finalized.",
        }

    def status(self) -> dict:
        """Get status of all active recordings."""
        active = []
        for sym, task in self._tasks.items():
            active.append({
                "symbol": sym,
                "running": not task.done(),
                "task_name": task.get_name(),
            })

        db_recordings = self.store.get_recordings()

        return {
            "active_tasks": active,
            "db_recordings": db_recordings,
            "db_stats": self.store.get_stats(),
        }

    async def _poll_loop(self, symbol: str, interval_sec: int, depth: int, session_id: str):
        """Internal polling loop — runs until cancelled."""
        while True:
            try:
                snapshot = await self.client.get_orderbook_snapshot(symbol, depth=depth)
                self.store.save_snapshot(symbol, snapshot, session_id=session_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                # Silently skip failed polls (network issues, etc.)
                # The next iteration will try again.
                pass

            await asyncio.sleep(interval_sec)
