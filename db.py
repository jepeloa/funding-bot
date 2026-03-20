"""
Capa de base de datos TimescaleDB para Binance Futures recorder.
Esquema completo: market data + virtual trades + strategy snapshots.
Escritura asíncrona con batch inserts via asyncpg COPY protocol.
"""

import asyncio
import json
import logging
import time
import traceback
from datetime import datetime, timezone

import asyncpg

from config import (
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD,
    DB_POOL_MIN, DB_POOL_MAX, BATCH_SIZE,
)

log = logging.getLogger("db")

# ── Protección contra OOM si la DB no responde ──────────────────
MAX_BUFFER_RECORDS = 100_000    # máximo por tipo de buffer
FLUSH_BACKOFF_MAX_SECS = 60.0   # máximo backoff entre reintentos


# ══════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════

def _ms_to_dt(ms: int) -> datetime:
    """Convierte epoch milisegundos a datetime UTC."""
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def _epoch_to_dt(epoch: float) -> datetime:
    """Convierte epoch segundos a datetime UTC."""
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


def _parse_levels(levels: list, n: int = 20) -> tuple[list[float], list[float]]:
    """Parsea levels [[price, qty], ...] a (prices[], qtys[]) de tamaño fijo n."""
    prices = []
    qtys = []
    for i in range(n):
        if i < len(levels):
            prices.append(float(levels[i][0]))
            qtys.append(float(levels[i][1]))
        else:
            prices.append(0.0)
            qtys.append(0.0)
    return prices, qtys


# ══════════════════════════════════════════════════════════════════
#  INIT
# ══════════════════════════════════════════════════════════════════

async def init_db(pool: asyncpg.Pool) -> bool:
    """
    Verifica conexión y detecta crash previo via heartbeat.
    Retorna True si hubo crash previo.
    """
    async with pool.acquire() as conn:
        # Verificar que las tablas existen
        exists = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
            "WHERE table_name = 'heartbeat')"
        )
        if not exists:
            log.error("Schema no encontrado. Ejecutar init_timescaledb.sh primero.")
            raise RuntimeError("TimescaleDB schema not initialized")

        # Detectar crash
        crashed = False
        row = await conn.fetchrow(
            "SELECT clean_shutdown FROM heartbeat WHERE id = 1"
        )
        if row is not None:
            if row["clean_shutdown"] == 0:
                crashed = True

        # Reset heartbeat
        await conn.execute(
            "INSERT INTO heartbeat (id, last_beat, msg_count, clean_shutdown) "
            "VALUES (1, $1, 0, 0) "
            "ON CONFLICT (id) DO UPDATE SET last_beat = $1, msg_count = 0, clean_shutdown = 0",
            time.time(),
        )
        return crashed


# ══════════════════════════════════════════════════════════════════
#  ASYNC WRITER
# ══════════════════════════════════════════════════════════════════

class AsyncDBWriter:
    """Buffer de escritura asíncrono con batch inserts via COPY."""

    def __init__(self):
        self._pool: asyncpg.Pool | None = None
        self._depth_buf: list[tuple] = []
        self._trade_buf: list[tuple] = []
        self._ticker_buf: list[tuple] = []
        self._mark_buf: list[tuple] = []
        self._oi_buf: list[tuple] = []
        self._funding_buf: list[tuple] = []
        self._snapshot_buf: list[tuple] = []
        self._liq_buf: list[tuple] = []
        self._lsr_buf: list[tuple] = []
        self._tbs_buf: list[tuple] = []
        self._msg_count: int = 0
        self._flush_backoff_until: float = 0.0
        self._consecutive_flush_errors: int = 0

    async def connect(self):
        """Conecta al pool DB con reintentos."""
        for attempt in range(1, 11):
            try:
                self._pool = await asyncpg.create_pool(
                    host=DB_HOST,
                    port=DB_PORT,
                    database=DB_NAME,
                    user=DB_USER,
                    password=DB_PASSWORD,
                    min_size=DB_POOL_MIN,
                    max_size=DB_POOL_MAX,
                    command_timeout=60,
                )
                log.info(
                    f"Conectado a TimescaleDB {DB_HOST}:{DB_PORT}/{DB_NAME} "
                    f"(pool {DB_POOL_MIN}-{DB_POOL_MAX})"
                )
                return
            except Exception as e:
                if attempt == 10:
                    raise
                wait = min(attempt * 3, 30)
                log.warning(
                    f"DB connect intento {attempt}/10 falló: {e} — "
                    f"reintentando en {wait}s"
                )
                await asyncio.sleep(wait)

    async def close(self):
        """Cierre seguro con timeouts de protección."""
        # Flush final con timeout
        try:
            await asyncio.wait_for(self.flush(), timeout=15.0)
        except (asyncio.TimeoutError, Exception) as e:
            log.warning(f"Flush final incompleto: {e}")
        if self._pool:
            # Mark clean shutdown
            try:
                async with self._pool.acquire(timeout=5) as conn:
                    await conn.execute(
                        "UPDATE heartbeat SET clean_shutdown = 1, last_beat = $1 "
                        "WHERE id = 1",
                        time.time(),
                    )
            except Exception as e:
                log.warning(f"No se pudo marcar clean_shutdown: {e}")
            try:
                await self._pool.close()
            except Exception:
                pass
            log.info("Pool de conexiones cerrado")

    # ── Market Data Writers ──────────────────────────────────────

    async def insert_depth(self, symbol: str, data: dict, received_at: float):
        bids_raw = data.get("b", data.get("bids", []))
        asks_raw = data.get("a", data.get("asks", []))
        bid_prices, bid_qtys = _parse_levels(bids_raw)
        ask_prices, ask_qtys = _parse_levels(asks_raw)

        self._depth_buf.append((
            _ms_to_dt(data.get("E", 0)),
            _epoch_to_dt(received_at),
            symbol,
            data.get("lastUpdateId", data.get("u", 0)),
            bid_prices,
            bid_qtys,
            ask_prices,
            ask_qtys,
        ))
        self._msg_count += 1
        if len(self._depth_buf) >= BATCH_SIZE:
            await self._flush_depth()

    async def insert_trade(self, symbol: str, data: dict, received_at: float):
        self._trade_buf.append((
            _ms_to_dt(data["E"]),
            _epoch_to_dt(received_at),
            symbol,
            data["a"],
            float(data["p"]),
            float(data["q"]),
            data["f"],
            data["l"],
            _ms_to_dt(data["T"]),
            data["m"],
        ))
        self._msg_count += 1
        if len(self._trade_buf) >= BATCH_SIZE:
            await self._flush_trades()

    async def insert_ticker(self, symbol: str, data: dict, received_at: float):
        self._ticker_buf.append((
            _ms_to_dt(data.get("E", 0)),
            _epoch_to_dt(received_at),
            symbol,
            float(data["b"]),
            float(data["B"]),
            float(data["a"]),
            float(data["A"]),
            data.get("u", 0),
        ))
        self._msg_count += 1
        if len(self._ticker_buf) >= BATCH_SIZE:
            await self._flush_tickers()

    async def insert_mark_price(self, symbol: str, data: dict, received_at: float):
        self._mark_buf.append((
            _ms_to_dt(data["E"]),
            _epoch_to_dt(received_at),
            symbol,
            float(data["p"]),
            float(data["i"]),
            float(data.get("r", "0")),
            _ms_to_dt(data.get("T", 0)),
        ))
        self._msg_count += 1
        if len(self._mark_buf) >= BATCH_SIZE:
            await self._flush_marks()

    async def insert_oi(self, symbol: str, oi_contracts: str, oi_value: str,
                        polled_at: float):
        self._oi_buf.append((
            _epoch_to_dt(polled_at),
            symbol,
            float(oi_contracts),
            float(oi_value),
        ))
        if len(self._oi_buf) >= BATCH_SIZE:
            await self._flush_oi()

    async def insert_funding(self, symbol: str, funding_time: int, rate: str,
                             polled_at: float):
        self._funding_buf.append((
            _ms_to_dt(funding_time),
            symbol,
            _epoch_to_dt(polled_at),
            float(rate),
        ))
        if len(self._funding_buf) >= BATCH_SIZE:
            await self._flush_funding()

    async def insert_liquidation(self, symbol: str, data: dict, received_at: float):
        """Insert liquidation order from forceOrder stream."""
        o = data.get("o", data)  # nested under 'o' key in WS msg
        self._liq_buf.append((
            _ms_to_dt(data.get("E", 0)),
            _epoch_to_dt(received_at),
            symbol,
            o.get("S", ""),       # side
            o.get("o", "LIMIT"),  # order type
            o.get("f", "IOC"),    # time in force
            float(o.get("q", 0)), # original qty
            float(o.get("p", 0)), # price (bankruptcy)
            float(o.get("ap", 0)),# avg price
            float(o.get("z", 0)), # filled qty
            _ms_to_dt(o.get("T", 0)),  # trade time
            o.get("X", ""),       # order status
        ))
        self._msg_count += 1
        if len(self._liq_buf) >= 100:
            await self._flush_liquidations()

    async def insert_long_short_ratio(self, symbol: str, ratio_type: str,
                                       data: dict, polled_at: float):
        """Insert long/short ratio from REST poll."""
        self._lsr_buf.append((
            _ms_to_dt(int(data.get("timestamp", 0))),
            _epoch_to_dt(polled_at),
            symbol,
            ratio_type,
            float(data.get("longShortRatio", 0)),
            float(data.get("longAccount", data.get("longPosition", 0))),
            float(data.get("shortAccount", data.get("shortPosition", 0))),
        ))
        if len(self._lsr_buf) >= BATCH_SIZE:
            await self._flush_lsr()

    async def insert_taker_buy_sell(self, symbol: str, data: dict,
                                     polled_at: float):
        """Insert taker buy/sell volume from REST poll."""
        self._tbs_buf.append((
            _ms_to_dt(int(data.get("timestamp", 0))),
            _epoch_to_dt(polled_at),
            symbol,
            float(data.get("buySellRatio", 0)),
            float(data.get("buyVol", 0)),
            float(data.get("sellVol", 0)),
        ))
        if len(self._tbs_buf) >= BATCH_SIZE:
            await self._flush_tbs()

    async def insert_snapshot(self, snap: dict):
        self._snapshot_buf.append((
            _epoch_to_dt(snap["timestamp"]),
            snap["symbol"],
            snap.get("score_total", 0),
            snap.get("c_fund", 0),
            snap.get("c_oi", 0),
            snap.get("c_price", 0),
            snap.get("c_taker", 0),
            snap.get("c_vol", 0),
            snap.get("energy_hours", 0),
            snap.get("exhaustion", 0),
            snap.get("mark_price", 0),
            snap.get("funding_rate", 0),
            snap.get("oi_value", 0),
            snap.get("taker_buy_ratio", 0),
            snap.get("volume_ratio", 0),
            snap.get("price_change_12h", 0),
            snap.get("price_change_24h", 0),
            snap.get("sma_24h", 0),
            snap.get("premium_velocity", 0),
        ))
        if len(self._snapshot_buf) >= 200:
            await self._flush_snapshots()

    # ── Virtual Trade Methods ────────────────────────────────────

    async def open_virtual_trade(self, data: dict) -> int:
        """Abre trade virtual, retorna trade_id."""
        from config import STRATEGY_VERSION
        await self.flush()  # flush pending antes de trade
        async with self._pool.acquire() as conn:
            trade_id = await conn.fetchval(
                "INSERT INTO virtual_trades "
                "(symbol, variant, entry_time, entry_price, entry_score, "
                "entry_energy, entry_exhaustion, leverage, position_size, "
                "entry_snapshot, status, strategy_version, trading_mode) "
                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) "
                "RETURNING id",
                data["symbol"],
                data["variant"],
                data["entry_time"],
                data["entry_price"],
                data.get("entry_score"),
                data.get("entry_energy"),
                data.get("entry_exhaustion"),
                data.get("leverage"),
                data.get("position_size"),
                json.dumps(data.get("entry_snapshot", {})),
                "open",
                STRATEGY_VERSION,
                data.get("trading_mode", "paper"),
            )
        return trade_id

    async def close_virtual_trade(self, trade_id: int, data: dict):
        """Cierra trade virtual con datos de salida."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                "UPDATE virtual_trades SET "
                "exit_time=$1, exit_price=$2, exit_reason=$3, "
                "pnl_pct=$4, pnl_leveraged=$5, pnl_usd=$6, "
                "funding_collected=$7, fees_paid=$8, "
                "mfe_pct=$9, mae_pct=$10, hold_hours=$11, status='closed' "
                "WHERE id=$12",
                data["exit_time"],
                data["exit_price"],
                data["exit_reason"],
                data["pnl_pct"],
                data["pnl_leveraged"],
                data["pnl_usd"],
                data.get("funding_collected", 0),
                data.get("fees_paid", 0),
                data.get("mfe_pct", 0),
                data.get("mae_pct", 0),
                data.get("hold_hours", 0),
                trade_id,
            )

    async def update_open_trade_mfe(self, trade_id: int, mfe: float, mae: float,
                                    funding_collected: float = 0.0):
        """Persiste MFE/MAE y funding de un trade abierto."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                "UPDATE virtual_trades SET mfe_pct=$1, mae_pct=$2, "
                "funding_collected=$3 "
                "WHERE id=$4 AND status='open'",
                mfe, mae, funding_collected, trade_id,
            )

    async def get_open_trades(self) -> list[dict]:
        """Retorna todos los trades abiertos para recovery."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM virtual_trades WHERE status = 'open'"
            )
            if not rows:
                return []
            return [dict(r) for r in rows]

    # ── Heartbeat ────────────────────────────────────────────────

    async def heartbeat(self):
        """Actualizar heartbeat en la DB."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                "UPDATE heartbeat SET last_beat = $1, msg_count = $2 WHERE id = 1",
                time.time(),
                self._msg_count,
            )

    # ── Flush Methods (COPY protocol) ────────────────────────────

    async def _flush_depth(self):
        if not self._depth_buf:
            return
        buf = self._depth_buf.copy()
        self._depth_buf.clear()
        try:
            async with self._pool.acquire() as conn:
                await conn.copy_records_to_table(
                    "depth_updates",
                    records=buf,
                    columns=[
                        "event_time", "received_at", "symbol", "last_update_id",
                        "bid_prices", "bid_qtys", "ask_prices", "ask_qtys",
                    ],
                )
        except Exception as e:
            log.error(f"Flush depth error ({len(buf)} rows): {e}")
            self._depth_buf.extend(buf)

    async def _flush_trades(self):
        if not self._trade_buf:
            return
        buf = self._trade_buf.copy()
        self._trade_buf.clear()
        try:
            async with self._pool.acquire() as conn:
                await conn.copy_records_to_table(
                    "agg_trades",
                    records=buf,
                    columns=[
                        "event_time", "received_at", "symbol", "agg_trade_id",
                        "price", "quantity", "first_trade_id", "last_trade_id",
                        "trade_time", "is_buyer_maker",
                    ],
                )
        except Exception as e:
            log.error(f"Flush trades error ({len(buf)} rows): {e}")
            self._trade_buf.extend(buf)

    async def _flush_tickers(self):
        if not self._ticker_buf:
            return
        buf = self._ticker_buf.copy()
        self._ticker_buf.clear()
        try:
            async with self._pool.acquire() as conn:
                await conn.copy_records_to_table(
                    "book_tickers",
                    records=buf,
                    columns=[
                        "event_time", "received_at", "symbol",
                        "best_bid_price", "best_bid_qty",
                        "best_ask_price", "best_ask_qty", "update_id",
                    ],
                )
        except Exception as e:
            log.error(f"Flush tickers error ({len(buf)} rows): {e}")
            self._ticker_buf.extend(buf)

    async def _flush_marks(self):
        if not self._mark_buf:
            return
        buf = self._mark_buf.copy()
        self._mark_buf.clear()
        try:
            async with self._pool.acquire() as conn:
                await conn.copy_records_to_table(
                    "mark_prices",
                    records=buf,
                    columns=[
                        "event_time", "received_at", "symbol",
                        "mark_price", "index_price", "funding_rate",
                        "next_funding_ts",
                    ],
                )
        except Exception as e:
            log.error(f"Flush marks error ({len(buf)} rows): {e}")
            self._mark_buf.extend(buf)

    async def _flush_oi(self):
        if not self._oi_buf:
            return
        buf = self._oi_buf.copy()
        self._oi_buf.clear()
        try:
            async with self._pool.acquire() as conn:
                await conn.copy_records_to_table(
                    "open_interest",
                    records=buf,
                    columns=["polled_at", "symbol", "oi_contracts", "oi_value"],
                )
        except Exception as e:
            log.error(f"Flush OI error ({len(buf)} rows): {e}")
            self._oi_buf.extend(buf)

    async def _flush_funding(self):
        if not self._funding_buf:
            return
        buf = self._funding_buf.copy()
        self._funding_buf.clear()
        try:
            async with self._pool.acquire() as conn:
                await conn.copy_records_to_table(
                    "funding_rates",
                    records=buf,
                    columns=["funding_time", "symbol", "polled_at", "funding_rate"],
                )
        except Exception as e:
            log.error(f"Flush funding error ({len(buf)} rows): {e}")
            self._funding_buf.extend(buf)

    async def _flush_snapshots(self):
        if not self._snapshot_buf:
            return
        buf = self._snapshot_buf.copy()
        self._snapshot_buf.clear()
        try:
            async with self._pool.acquire() as conn:
                await conn.copy_records_to_table(
                    "strategy_snapshots",
                    records=buf,
                    columns=[
                        "timestamp", "symbol", "score_total", "c_fund", "c_oi",
                        "c_price", "c_taker", "c_vol", "energy_hours", "exhaustion",
                        "mark_price", "funding_rate", "oi_value", "taker_buy_ratio",
                        "volume_ratio", "price_change_12h", "price_change_24h",
                        "sma_24h", "premium_velocity",
                    ],
                )
        except Exception as e:
            log.error(f"Flush snapshots error ({len(buf)} rows): {e}")
            self._snapshot_buf.extend(buf)

    async def _flush_liquidations(self):
        if not self._liq_buf:
            return
        buf = self._liq_buf.copy()
        self._liq_buf.clear()
        try:
            async with self._pool.acquire() as conn:
                await conn.copy_records_to_table(
                    "liquidations",
                    records=buf,
                    columns=[
                        "event_time", "received_at", "symbol", "side",
                        "order_type", "time_in_force", "original_qty",
                        "price", "avg_price", "filled_qty", "trade_time",
                        "order_status",
                    ],
                )
        except Exception as e:
            log.error(f"Flush liquidations error ({len(buf)} rows): {e}")
            self._liq_buf.extend(buf)

    async def _flush_lsr(self):
        if not self._lsr_buf:
            return
        buf = self._lsr_buf.copy()
        self._lsr_buf.clear()
        try:
            async with self._pool.acquire() as conn:
                await conn.copy_records_to_table(
                    "long_short_ratio",
                    records=buf,
                    columns=[
                        "timestamp", "polled_at", "symbol", "ratio_type",
                        "long_short_ratio", "long_account_pct",
                        "short_account_pct",
                    ],
                )
        except Exception as e:
            log.error(f"Flush LSR error ({len(buf)} rows): {e}")
            self._lsr_buf.extend(buf)

    async def _flush_tbs(self):
        if not self._tbs_buf:
            return
        buf = self._tbs_buf.copy()
        self._tbs_buf.clear()
        try:
            async with self._pool.acquire() as conn:
                await conn.copy_records_to_table(
                    "taker_buy_sell",
                    records=buf,
                    columns=[
                        "timestamp", "polled_at", "symbol",
                        "buy_sell_ratio", "buy_vol", "sell_vol",
                    ],
                )
        except Exception as e:
            log.error(f"Flush TBS error ({len(buf)} rows): {e}")
            self._tbs_buf.extend(buf)

    def _cap_all_buffers(self):
        """Previene OOM si la DB no responde: descarta registros antiguos."""
        bufs = [
            (self._depth_buf, "depth"),
            (self._trade_buf, "trades"),
            (self._ticker_buf, "tickers"),
            (self._mark_buf, "marks"),
            (self._oi_buf, "oi"),
            (self._funding_buf, "funding"),
            (self._snapshot_buf, "snapshots"),
            (self._liq_buf, "liquidations"),
            (self._lsr_buf, "lsr"),
            (self._tbs_buf, "tbs"),
        ]
        for buf, name in bufs:
            if len(buf) > MAX_BUFFER_RECORDS:
                drop = len(buf) - MAX_BUFFER_RECORDS * 3 // 4
                del buf[:drop]
                log.warning(
                    f"⚠️ Buffer {name} overflow: descartados {drop:,} registros "
                    f"antiguos (quedan {len(buf):,}). ¿DB respondiendo?"
                )

    async def flush(self):
        """Flush todos los buffers con aislamiento y protección."""
        now = time.time()
        # Backoff si hubo errores recientes (evita hammering a DB caída)
        if now < self._flush_backoff_until:
            return

        # Prevenir OOM
        self._cap_all_buffers()

        # Cada tabla se flushea independientemente
        any_error = False
        for name, method in [
            ("depth", self._flush_depth),
            ("trades", self._flush_trades),
            ("tickers", self._flush_tickers),
            ("marks", self._flush_marks),
            ("oi", self._flush_oi),
            ("funding", self._flush_funding),
            ("snapshots", self._flush_snapshots),
            ("liquidations", self._flush_liquidations),
            ("lsr", self._flush_lsr),
            ("tbs", self._flush_tbs),
        ]:
            try:
                await method()
            except Exception as e:
                log.error(f"Flush {name} error no capturado: {e}")
                any_error = True

        if any_error:
            self._consecutive_flush_errors += 1
            backoff = min(
                10.0 * self._consecutive_flush_errors,
                FLUSH_BACKOFF_MAX_SECS,
            )
            self._flush_backoff_until = time.time() + backoff
            log.warning(
                f"⚠️ Flush errors (#{self._consecutive_flush_errors}), "
                f"backoff {backoff:.0f}s"
            )
        else:
            if self._consecutive_flush_errors > 0:
                log.info(
                    f"✓ DB flush recuperado después de "
                    f"{self._consecutive_flush_errors} errores"
                )
            self._consecutive_flush_errors = 0
            self._flush_backoff_until = 0

    @property
    def pending_count(self) -> int:
        return (len(self._depth_buf) + len(self._trade_buf) +
                len(self._ticker_buf) + len(self._mark_buf) +
                len(self._oi_buf) + len(self._funding_buf) +
                len(self._snapshot_buf) + len(self._liq_buf) +
                len(self._lsr_buf) + len(self._tbs_buf))

    @property
    def total_messages(self) -> int:
        return self._msg_count
