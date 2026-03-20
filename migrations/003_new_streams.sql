-- ════════════════════════════════════════════════════════════════════
--  003 — Nuevas tablas: liquidaciones, long/short ratio, taker volume
--  Ejecutar: docker exec -i timescaledb psql -U recorder -d binance_futures < migrations/003_new_streams.sql
-- ════════════════════════════════════════════════════════════════════

-- ────────────────────────────────────────────────────────────────
--  LIQUIDATIONS (forceOrder WS stream)
--  Cada fila = una liquidación en el mercado
-- ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS liquidations (
    event_time          TIMESTAMPTZ      NOT NULL,
    received_at         TIMESTAMPTZ      NOT NULL,
    symbol              TEXT             NOT NULL,
    side                TEXT             NOT NULL,        -- BUY | SELL
    order_type          TEXT             NOT NULL,        -- LIMIT (siempre)
    time_in_force       TEXT             NOT NULL,        -- IOC
    original_qty        DOUBLE PRECISION NOT NULL,
    price               DOUBLE PRECISION NOT NULL,        -- precio de bankruptcy
    avg_price           DOUBLE PRECISION NOT NULL,        -- precio ejecutado real
    filled_qty          DOUBLE PRECISION NOT NULL,
    trade_time          TIMESTAMPTZ      NOT NULL,
    order_status        TEXT             NOT NULL         -- FILLED | NEW | PARTIALLY_FILLED
);

SELECT create_hypertable('liquidations', 'event_time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_liq_sym_time
    ON liquidations (symbol, event_time DESC);

-- Compresión
ALTER TABLE liquidations SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'event_time DESC'
);
SELECT add_compression_policy('liquidations', INTERVAL '4 hours', if_not_exists => TRUE);


-- ────────────────────────────────────────────────────────────────
--  LONG/SHORT RATIO (REST poll — top traders + global)
--  Combina topLongShortAccountRatio y globalLongShortAccountRatio
-- ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS long_short_ratio (
    timestamp           TIMESTAMPTZ      NOT NULL,
    polled_at           TIMESTAMPTZ      NOT NULL,
    symbol              TEXT             NOT NULL,
    ratio_type          TEXT             NOT NULL,        -- 'top_account' | 'top_position' | 'global'
    long_short_ratio    DOUBLE PRECISION NOT NULL,
    long_account_pct    DOUBLE PRECISION NOT NULL,        -- % cuentas long
    short_account_pct   DOUBLE PRECISION NOT NULL         -- % cuentas short
);

SELECT create_hypertable('long_short_ratio', 'timestamp',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_lsr_sym_type_time
    ON long_short_ratio (symbol, ratio_type, timestamp DESC);

ALTER TABLE long_short_ratio SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, ratio_type',
    timescaledb.compress_orderby = 'timestamp DESC'
);
SELECT add_compression_policy('long_short_ratio', INTERVAL '4 hours', if_not_exists => TRUE);


-- ────────────────────────────────────────────────────────────────
--  TAKER BUY/SELL VOLUME (REST poll)
--  takerlongshortRatio endpoint
-- ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS taker_buy_sell (
    timestamp           TIMESTAMPTZ      NOT NULL,
    polled_at           TIMESTAMPTZ      NOT NULL,
    symbol              TEXT             NOT NULL,
    buy_sell_ratio      DOUBLE PRECISION NOT NULL,        -- buy_vol / sell_vol
    buy_vol             DOUBLE PRECISION NOT NULL,
    sell_vol            DOUBLE PRECISION NOT NULL
);

SELECT create_hypertable('taker_buy_sell', 'timestamp',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_tbs_sym_time
    ON taker_buy_sell (symbol, timestamp DESC);

ALTER TABLE taker_buy_sell SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'timestamp DESC'
);
SELECT add_compression_policy('taker_buy_sell', INTERVAL '4 hours', if_not_exists => TRUE);


-- ════════════════════════════════════════════════════════════════════
--  FIN 003
-- ════════════════════════════════════════════════════════════════════
