-- ============================================
-- Coinbase Streaming Pipeline - Table Creation
-- ============================================
-- Run after 001_create_schemas.sql
-- ============================================

-- ============================================
-- BRONZE LAYER: Raw Data (append-only)
-- ============================================

-- Raw streaming trades from Coinbase WebSocket
CREATE TABLE IF NOT EXISTS bronze.coinbase_trades_raw (
    id              BIGSERIAL PRIMARY KEY,
    ingest_ts       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source          TEXT NOT NULL DEFAULT 'coinbase',
    channel         TEXT NOT NULL DEFAULT 'matches',
    payload         JSONB NOT NULL,
    payload_hash    TEXT NOT NULL,
    event_ts        TIMESTAMPTZ NULL
);

-- Indexes for Bronze
CREATE INDEX IF NOT EXISTS idx_bronze_trades_ingest_ts 
    ON bronze.coinbase_trades_raw (ingest_ts);
CREATE INDEX IF NOT EXISTS idx_bronze_trades_event_ts 
    ON bronze.coinbase_trades_raw (event_ts);
CREATE INDEX IF NOT EXISTS idx_bronze_trades_payload_hash 
    ON bronze.coinbase_trades_raw (payload_hash);
CREATE INDEX IF NOT EXISTS idx_bronze_trades_payload_product 
    ON bronze.coinbase_trades_raw ((payload->>'product_id'));

COMMENT ON TABLE bronze.coinbase_trades_raw IS 'Raw trade events from Coinbase WebSocket feed - append only';
COMMENT ON COLUMN bronze.coinbase_trades_raw.payload IS 'Complete JSON payload as received';
COMMENT ON COLUMN bronze.coinbase_trades_raw.payload_hash IS 'SHA1 hash for deduplication checks';

-- ============================================
-- SILVER LAYER: Clean, Typed, Deduplicated
-- ============================================

-- Cleaned and typed trade records
CREATE TABLE IF NOT EXISTS silver.coinbase_trades (
    trade_id        BIGINT PRIMARY KEY,
    product_id      TEXT NOT NULL,
    price           NUMERIC(18,8) NOT NULL,
    size            NUMERIC(18,8) NOT NULL,
    side            TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
    trade_time      TIMESTAMPTZ NOT NULL,
    maker_order_id  TEXT,
    taker_order_id  TEXT,
    ingest_ts       TIMESTAMPTZ NOT NULL
);

-- Indexes for Silver
CREATE INDEX IF NOT EXISTS idx_silver_trades_product_time 
    ON silver.coinbase_trades (product_id, trade_time);
CREATE INDEX IF NOT EXISTS idx_silver_trades_time 
    ON silver.coinbase_trades (trade_time);

COMMENT ON TABLE silver.coinbase_trades IS 'Cleaned, typed, and deduplicated trade records';

-- Data quality quarantine table
CREATE TABLE IF NOT EXISTS silver.coinbase_trades_quarantine (
    id              BIGSERIAL PRIMARY KEY,
    source_id       BIGINT,
    error_reason    TEXT NOT NULL,
    payload         JSONB NOT NULL,
    quarantine_ts   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE silver.coinbase_trades_quarantine IS 'Records that failed validation during Bronze to Silver processing';

-- ============================================
-- GOLD LAYER: Aggregated Business Metrics
-- ============================================

-- 1-Minute OHLC Candles
CREATE TABLE IF NOT EXISTS gold.ohlc_1m (
    product_id      TEXT NOT NULL,
    bucket_1m       TIMESTAMPTZ NOT NULL,
    open            NUMERIC(18,8) NOT NULL,
    high            NUMERIC(18,8) NOT NULL,
    low             NUMERIC(18,8) NOT NULL,
    close           NUMERIC(18,8) NOT NULL,
    volume          NUMERIC(18,8) NOT NULL,
    trade_count     BIGINT NOT NULL,
    vwap            NUMERIC(18,8),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (product_id, bucket_1m)
);

CREATE INDEX IF NOT EXISTS idx_gold_ohlc_1m_time 
    ON gold.ohlc_1m (bucket_1m);

COMMENT ON TABLE gold.ohlc_1m IS '1-minute OHLC candles with volume-weighted average price';

-- 1-Hour OHLC Candles
CREATE TABLE IF NOT EXISTS gold.ohlc_1h (
    product_id      TEXT NOT NULL,
    bucket_1h       TIMESTAMPTZ NOT NULL,
    open            NUMERIC(18,8) NOT NULL,
    high            NUMERIC(18,8) NOT NULL,
    low             NUMERIC(18,8) NOT NULL,
    close           NUMERIC(18,8) NOT NULL,
    volume          NUMERIC(18,8) NOT NULL,
    trade_count     BIGINT NOT NULL,
    vwap            NUMERIC(18,8),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (product_id, bucket_1h)
);

COMMENT ON TABLE gold.ohlc_1h IS '1-hour OHLC candles aggregated from 1-minute data';

-- Daily KPIs
CREATE TABLE IF NOT EXISTS gold.daily_kpis (
    product_id      TEXT NOT NULL,
    day             DATE NOT NULL,
    trades          BIGINT NOT NULL,
    volume          NUMERIC(18,8) NOT NULL,
    vwap            NUMERIC(18,8) NOT NULL,
    high            NUMERIC(18,8) NOT NULL,
    low             NUMERIC(18,8) NOT NULL,
    open            NUMERIC(18,8),
    close           NUMERIC(18,8),
    price_change_pct NUMERIC(10,4),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (product_id, day)
);

COMMENT ON TABLE gold.daily_kpis IS 'Daily aggregated metrics per trading pair';

-- Top Movers (refreshed periodically)
CREATE TABLE IF NOT EXISTS gold.top_movers (
    snapshot_ts     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    product_id      TEXT NOT NULL,
    period          TEXT NOT NULL, -- '1h', '24h', '7d'
    price_change_pct NUMERIC(10,4) NOT NULL,
    volume          NUMERIC(18,8) NOT NULL,
    trade_count     BIGINT NOT NULL,
    PRIMARY KEY (snapshot_ts, product_id, period)
);

COMMENT ON TABLE gold.top_movers IS 'Price movers snapshot for dashboard display';

-- ============================================
-- METADATA / PIPELINE TRACKING
-- ============================================

-- Pipeline run history
CREATE TABLE IF NOT EXISTS bronze.pipeline_runs (
    id              BIGSERIAL PRIMARY KEY,
    run_id          TEXT NOT NULL UNIQUE,
    pipeline_name   TEXT NOT NULL,
    start_ts        TIMESTAMPTZ NOT NULL,
    end_ts          TIMESTAMPTZ,
    status          TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    records_processed BIGINT,
    error_message   TEXT
);

COMMENT ON TABLE bronze.pipeline_runs IS 'Tracks pipeline execution history for monitoring';

-- Data quality check results
CREATE TABLE IF NOT EXISTS bronze.dq_check_results (
    id              BIGSERIAL PRIMARY KEY,
    check_ts        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    check_name      TEXT NOT NULL,
    check_type      TEXT NOT NULL,
    table_name      TEXT NOT NULL,
    passed          BOOLEAN NOT NULL,
    details         JSONB,
    run_id          TEXT
);

COMMENT ON TABLE bronze.dq_check_results IS 'Data quality check results for monitoring and alerting';
