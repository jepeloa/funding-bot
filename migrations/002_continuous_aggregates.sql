-- ════════════════════════════════════════════════════════════════════
--  Continuous Aggregates — OHLCV precalculado desde agg_trades
--  Ejecutar DESPUÉS de 001_schema.sql
-- ════════════════════════════════════════════════════════════════════

-- ────────────────────────────────────────────────────────────────
--  OHLCV 1 minuto — materializado incrementalmente
-- ────────────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', event_time) AS bucket,
    symbol,
    first(price, event_time)            AS open,
    max(price)                          AS high,
    min(price)                          AS low,
    last(price, event_time)             AS close,
    sum(quantity)                       AS volume,
    sum(quantity * price)               AS volume_usdt,
    count(*)                            AS trade_count,
    sum(CASE WHEN NOT is_buyer_maker THEN quantity ELSE 0 END) AS taker_buy_volume,
    sum(CASE WHEN is_buyer_maker THEN quantity ELSE 0 END)     AS taker_sell_volume
FROM agg_trades
GROUP BY bucket, symbol
WITH NO DATA;

-- Refresh policy: actualizar cada 1 minuto, con lag de 5 minutos
-- (los datos de los últimos 5 minutos pueden llegar tarde)
SELECT add_continuous_aggregate_policy('ohlcv_1m',
    start_offset    => INTERVAL '1 hour',
    end_offset      => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists   => TRUE
);


-- ────────────────────────────────────────────────────────────────
--  OHLCV 1 hora — cascade desde ohlcv_1m
-- ────────────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', bucket)       AS bucket,
    symbol,
    first(open, bucket)                 AS open,
    max(high)                           AS high,
    min(low)                            AS low,
    last(close, bucket)                 AS close,
    sum(volume)                         AS volume,
    sum(volume_usdt)                    AS volume_usdt,
    sum(trade_count)                    AS trade_count,
    sum(taker_buy_volume)               AS taker_buy_volume,
    sum(taker_sell_volume)              AS taker_sell_volume
FROM ohlcv_1m
GROUP BY time_bucket('1 hour', bucket), symbol
WITH NO DATA;

SELECT add_continuous_aggregate_policy('ohlcv_1h',
    start_offset    => INTERVAL '3 hours',
    end_offset      => INTERVAL '1 hour',
    schedule_interval => INTERVAL '30 minutes',
    if_not_exists   => TRUE
);


-- ────────────────────────────────────────────────────────────────
--  OHLCV 1 día — cascade desde ohlcv_1h
-- ────────────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv_1d
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', bucket)        AS bucket,
    symbol,
    first(open, bucket)                 AS open,
    max(high)                           AS high,
    min(low)                            AS low,
    last(close, bucket)                 AS close,
    sum(volume)                         AS volume,
    sum(volume_usdt)                    AS volume_usdt,
    sum(trade_count)                    AS trade_count,
    sum(taker_buy_volume)               AS taker_buy_volume,
    sum(taker_sell_volume)              AS taker_sell_volume
FROM ohlcv_1h
GROUP BY time_bucket('1 day', bucket), symbol
WITH NO DATA;

SELECT add_continuous_aggregate_policy('ohlcv_1d',
    start_offset    => INTERVAL '3 days',
    end_offset      => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists   => TRUE
);


-- ────────────────────────────────────────────────────────────────
--  Índices en los continuous aggregates
-- ────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_ohlcv_1m_sym ON ohlcv_1m (symbol, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_ohlcv_1h_sym ON ohlcv_1h (symbol, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_ohlcv_1d_sym ON ohlcv_1d (symbol, bucket DESC);


-- ════════════════════════════════════════════════════════════════════
--  FIN
-- ════════════════════════════════════════════════════════════════════
