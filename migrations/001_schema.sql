-- ════════════════════════════════════════════════════════════════════
--  Binance Futures Recorder — TimescaleDB Schema
--  Ejecutar con: psql -h localhost -U recorder -d binance_futures -f 001_schema.sql
-- ════════════════════════════════════════════════════════════════════

-- Asegurar extensión TimescaleDB
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ────────────────────────────────────────────────────────────────
--  MARKET DATA — Hypertables (time-series, append-only)
-- ────────────────────────────────────────────────────────────────

-- Partial book depth (depth@20@100ms)
-- Bids/asks normalizados a arrays de 20 elementos (no JSON)
CREATE TABLE IF NOT EXISTS depth_updates (
    event_time      TIMESTAMPTZ     NOT NULL,
    received_at     TIMESTAMPTZ     NOT NULL,
    symbol          TEXT            NOT NULL,
    last_update_id  BIGINT          NOT NULL,
    bid_prices      DOUBLE PRECISION[20] NOT NULL,
    bid_qtys        DOUBLE PRECISION[20] NOT NULL,
    ask_prices      DOUBLE PRECISION[20] NOT NULL,
    ask_qtys        DOUBLE PRECISION[20] NOT NULL
);

SELECT create_hypertable('depth_updates', 'event_time',
    chunk_time_interval => INTERVAL '6 hours',
    if_not_exists => TRUE
);

-- Aggregated trades
CREATE TABLE IF NOT EXISTS agg_trades (
    event_time      TIMESTAMPTZ     NOT NULL,
    received_at     TIMESTAMPTZ     NOT NULL,
    symbol          TEXT            NOT NULL,
    agg_trade_id    BIGINT          NOT NULL,
    price           DOUBLE PRECISION NOT NULL,
    quantity        DOUBLE PRECISION NOT NULL,
    first_trade_id  BIGINT          NOT NULL,
    last_trade_id   BIGINT          NOT NULL,
    trade_time      TIMESTAMPTZ     NOT NULL,
    is_buyer_maker  BOOLEAN         NOT NULL
);

SELECT create_hypertable('agg_trades', 'event_time',
    chunk_time_interval => INTERVAL '6 hours',
    if_not_exists => TRUE
);

-- Book ticker (best bid/ask)
CREATE TABLE IF NOT EXISTS book_tickers (
    event_time      TIMESTAMPTZ     NOT NULL,
    received_at     TIMESTAMPTZ     NOT NULL,
    symbol          TEXT            NOT NULL,
    best_bid_price  DOUBLE PRECISION NOT NULL,
    best_bid_qty    DOUBLE PRECISION NOT NULL,
    best_ask_price  DOUBLE PRECISION NOT NULL,
    best_ask_qty    DOUBLE PRECISION NOT NULL,
    update_id       BIGINT          NOT NULL
);

SELECT create_hypertable('book_tickers', 'event_time',
    chunk_time_interval => INTERVAL '6 hours',
    if_not_exists => TRUE
);

-- Mark price + funding rate
CREATE TABLE IF NOT EXISTS mark_prices (
    event_time      TIMESTAMPTZ     NOT NULL,
    received_at     TIMESTAMPTZ     NOT NULL,
    symbol          TEXT            NOT NULL,
    mark_price      DOUBLE PRECISION NOT NULL,
    index_price     DOUBLE PRECISION NOT NULL,
    funding_rate    DOUBLE PRECISION NOT NULL,
    next_funding_ts TIMESTAMPTZ     NOT NULL
);

SELECT create_hypertable('mark_prices', 'event_time',
    chunk_time_interval => INTERVAL '12 hours',
    if_not_exists => TRUE
);

-- Open Interest (polled via REST)
CREATE TABLE IF NOT EXISTS open_interest (
    polled_at       TIMESTAMPTZ     NOT NULL,
    symbol          TEXT            NOT NULL,
    oi_contracts    DOUBLE PRECISION NOT NULL,
    oi_value        DOUBLE PRECISION NOT NULL
);

SELECT create_hypertable('open_interest', 'polled_at',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Funding rate history (polled via REST)
CREATE TABLE IF NOT EXISTS funding_rates (
    funding_time    TIMESTAMPTZ     NOT NULL,
    symbol          TEXT            NOT NULL,
    polled_at       TIMESTAMPTZ     NOT NULL,
    funding_rate    DOUBLE PRECISION NOT NULL
);

SELECT create_hypertable('funding_rates', 'funding_time',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- Strategy snapshots (periodic state)
CREATE TABLE IF NOT EXISTS strategy_snapshots (
    timestamp       TIMESTAMPTZ     NOT NULL,
    symbol          TEXT            NOT NULL,
    score_total     DOUBLE PRECISION,
    c_fund          DOUBLE PRECISION,
    c_oi            DOUBLE PRECISION,
    c_price         DOUBLE PRECISION,
    c_taker         DOUBLE PRECISION,
    c_vol           DOUBLE PRECISION,
    energy_hours    DOUBLE PRECISION,
    exhaustion      INTEGER,
    mark_price      DOUBLE PRECISION,
    funding_rate    DOUBLE PRECISION,
    oi_value        DOUBLE PRECISION,
    taker_buy_ratio DOUBLE PRECISION,
    volume_ratio    DOUBLE PRECISION,
    price_change_12h DOUBLE PRECISION,
    price_change_24h DOUBLE PRECISION,
    sma_24h         DOUBLE PRECISION
);

SELECT create_hypertable('strategy_snapshots', 'timestamp',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);


-- ────────────────────────────────────────────────────────────────
--  INDEXES (dentro de cada chunk, TimescaleDB los mantiene)
-- ────────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_depth_sym_time
    ON depth_updates (symbol, event_time DESC);

CREATE INDEX IF NOT EXISTS idx_trades_sym_time
    ON agg_trades (symbol, event_time DESC);

CREATE INDEX IF NOT EXISTS idx_trades_sym_trade_time
    ON agg_trades (symbol, trade_time DESC);

CREATE INDEX IF NOT EXISTS idx_ticker_sym_time
    ON book_tickers (symbol, event_time DESC);

CREATE INDEX IF NOT EXISTS idx_mark_sym_time
    ON mark_prices (symbol, event_time DESC);

CREATE INDEX IF NOT EXISTS idx_oi_sym_time
    ON open_interest (symbol, polled_at DESC);

CREATE INDEX IF NOT EXISTS idx_funding_sym_time
    ON funding_rates (symbol, funding_time DESC);

CREATE INDEX IF NOT EXISTS idx_snapshots_sym_time
    ON strategy_snapshots (symbol, timestamp DESC);


-- ────────────────────────────────────────────────────────────────
--  STRATEGY / VIRTUAL TRADES — Tabla regular (necesita UPDATE)
-- ────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS virtual_trades (
    id              SERIAL PRIMARY KEY,
    symbol          TEXT            NOT NULL,
    variant         TEXT            NOT NULL,
    -- Entrada
    entry_time      DOUBLE PRECISION NOT NULL,
    entry_price     DOUBLE PRECISION NOT NULL,
    entry_score     DOUBLE PRECISION,
    entry_energy    DOUBLE PRECISION,
    entry_exhaustion INTEGER,
    leverage        INTEGER,
    position_size   DOUBLE PRECISION,
    entry_snapshot  JSONB,
    -- Salida (NULL si trade abierto)
    exit_time       DOUBLE PRECISION,
    exit_price      DOUBLE PRECISION,
    exit_reason     TEXT,
    pnl_pct         DOUBLE PRECISION,
    pnl_leveraged   DOUBLE PRECISION,
    pnl_usd         DOUBLE PRECISION,
    funding_collected DOUBLE PRECISION,
    fees_paid       DOUBLE PRECISION,
    mfe_pct         DOUBLE PRECISION,
    mae_pct         DOUBLE PRECISION,
    hold_hours      DOUBLE PRECISION,
    -- Estado
    status          TEXT            NOT NULL DEFAULT 'open'
);

CREATE INDEX IF NOT EXISTS idx_vtrades_sym_status
    ON virtual_trades (symbol, status);

CREATE INDEX IF NOT EXISTS idx_vtrades_entry_time
    ON virtual_trades (entry_time DESC);


-- ────────────────────────────────────────────────────────────────
--  HEARTBEAT — Crash recovery singleton
-- ────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS heartbeat (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    last_beat       DOUBLE PRECISION NOT NULL,
    msg_count       BIGINT          NOT NULL DEFAULT 0,
    clean_shutdown  INTEGER         NOT NULL DEFAULT 0
);


-- ────────────────────────────────────────────────────────────────
--  COMPRESIÓN — TimescaleDB native columnar compression
-- ────────────────────────────────────────────────────────────────

-- depth_updates: tabla más pesada, compresión agresiva
ALTER TABLE depth_updates SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'event_time DESC'
);
SELECT add_compression_policy('depth_updates', INTERVAL '2 hours', if_not_exists => TRUE);

-- agg_trades
ALTER TABLE agg_trades SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'event_time DESC'
);
SELECT add_compression_policy('agg_trades', INTERVAL '2 hours', if_not_exists => TRUE);

-- book_tickers
ALTER TABLE book_tickers SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'event_time DESC'
);
SELECT add_compression_policy('book_tickers', INTERVAL '2 hours', if_not_exists => TRUE);

-- mark_prices
ALTER TABLE mark_prices SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'event_time DESC'
);
SELECT add_compression_policy('mark_prices', INTERVAL '4 hours', if_not_exists => TRUE);

-- open_interest
ALTER TABLE open_interest SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'polled_at DESC'
);
SELECT add_compression_policy('open_interest', INTERVAL '4 hours', if_not_exists => TRUE);

-- funding_rates
ALTER TABLE funding_rates SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'funding_time DESC'
);
SELECT add_compression_policy('funding_rates', INTERVAL '1 day', if_not_exists => TRUE);

-- strategy_snapshots
ALTER TABLE strategy_snapshots SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'timestamp DESC'
);
SELECT add_compression_policy('strategy_snapshots', INTERVAL '4 hours', if_not_exists => TRUE);


-- ────────────────────────────────────────────────────────────────
--  RETENCIÓN — desactivada (grabación indefinida)
--  Para reactivar: SELECT add_retention_policy('tabla', INTERVAL '30 days');
-- ────────────────────────────────────────────────────────────────


-- ════════════════════════════════════════════════════════════════════
--  FIN DEL SCHEMA
-- ════════════════════════════════════════════════════════════════════
