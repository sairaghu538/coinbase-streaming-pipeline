-- ============================================
-- Silver to Gold Transformation
-- ============================================
-- Builds aggregated metrics for BI consumption
-- from the cleaned Silver layer.
--
-- This script is idempotent - safe to run multiple times.
-- ============================================

-- ============================================
-- 1. Build 1-Minute OHLC Candles
-- ============================================

INSERT INTO gold.ohlc_1m (
    product_id,
    bucket_1m,
    open,
    high,
    low,
    close,
    volume,
    trade_count,
    vwap
)
WITH base AS (
    SELECT
        product_id,
        date_trunc('minute', trade_time) AS bucket_1m,
        trade_time,
        price,
        size,
        price * size AS notional
    FROM silver.coinbase_trades
),
-- Get first and last price per bucket
first_last AS (
    SELECT DISTINCT
        product_id,
        bucket_1m,
        FIRST_VALUE(price) OVER w AS open_price,
        LAST_VALUE(price) OVER w AS close_price
    FROM base
    WINDOW w AS (
        PARTITION BY product_id, bucket_1m 
        ORDER BY trade_time
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
),
-- Aggregate metrics
agg AS (
    SELECT
        product_id,
        bucket_1m,
        MAX(price) AS high,
        MIN(price) AS low,
        SUM(size) AS volume,
        COUNT(*) AS trade_count,
        SUM(notional) / NULLIF(SUM(size), 0) AS vwap
    FROM base
    GROUP BY product_id, bucket_1m
)
SELECT
    a.product_id,
    a.bucket_1m,
    f.open_price AS open,
    a.high,
    a.low,
    f.close_price AS close,
    a.volume,
    a.trade_count,
    a.vwap
FROM agg a
JOIN first_last f USING (product_id, bucket_1m)
ON CONFLICT (product_id, bucket_1m) 
DO UPDATE SET
    high = GREATEST(gold.ohlc_1m.high, EXCLUDED.high),
    low = LEAST(gold.ohlc_1m.low, EXCLUDED.low),
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    trade_count = EXCLUDED.trade_count,
    vwap = EXCLUDED.vwap;

-- ============================================
-- 2. Build 1-Hour OHLC Candles (from 1-minute)
-- ============================================

INSERT INTO gold.ohlc_1h (
    product_id,
    bucket_1h,
    open,
    high,
    low,
    close,
    volume,
    trade_count,
    vwap
)
WITH hourly AS (
    SELECT
        product_id,
        date_trunc('hour', bucket_1m) AS bucket_1h,
        bucket_1m,
        open,
        high,
        low,
        close,
        volume,
        trade_count,
        vwap * volume AS notional_sum
    FROM gold.ohlc_1m
),
first_last AS (
    SELECT DISTINCT
        product_id,
        bucket_1h,
        FIRST_VALUE(open) OVER w AS open_price,
        LAST_VALUE(close) OVER w AS close_price
    FROM hourly
    WINDOW w AS (
        PARTITION BY product_id, bucket_1h 
        ORDER BY bucket_1m
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
),
agg AS (
    SELECT
        product_id,
        bucket_1h,
        MAX(high) AS high,
        MIN(low) AS low,
        SUM(volume) AS volume,
        SUM(trade_count) AS trade_count,
        SUM(notional_sum) / NULLIF(SUM(volume), 0) AS vwap
    FROM hourly
    GROUP BY product_id, bucket_1h
)
SELECT
    a.product_id,
    a.bucket_1h,
    f.open_price AS open,
    a.high,
    a.low,
    f.close_price AS close,
    a.volume,
    a.trade_count,
    a.vwap
FROM agg a
JOIN first_last f USING (product_id, bucket_1h)
ON CONFLICT (product_id, bucket_1h) 
DO UPDATE SET
    high = GREATEST(gold.ohlc_1h.high, EXCLUDED.high),
    low = LEAST(gold.ohlc_1h.low, EXCLUDED.low),
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    trade_count = EXCLUDED.trade_count,
    vwap = EXCLUDED.vwap;

-- ============================================
-- 3. Build Daily KPIs
-- ============================================

INSERT INTO gold.daily_kpis (
    product_id,
    day,
    trades,
    volume,
    vwap,
    high,
    low,
    open,
    close,
    price_change_pct
)
WITH daily AS (
    SELECT
        product_id,
        trade_time::date AS day,
        trade_time,
        price,
        size,
        price * size AS notional
    FROM silver.coinbase_trades
),
first_last AS (
    SELECT DISTINCT
        product_id,
        day,
        FIRST_VALUE(price) OVER w AS open_price,
        LAST_VALUE(price) OVER w AS close_price
    FROM daily
    WINDOW w AS (
        PARTITION BY product_id, day 
        ORDER BY trade_time
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
),
agg AS (
    SELECT
        product_id,
        day,
        COUNT(*) AS trades,
        SUM(size) AS volume,
        SUM(notional) / NULLIF(SUM(size), 0) AS vwap,
        MAX(price) AS high,
        MIN(price) AS low
    FROM daily
    GROUP BY product_id, day
)
SELECT
    a.product_id,
    a.day,
    a.trades,
    a.volume,
    a.vwap,
    a.high,
    a.low,
    f.open_price AS open,
    f.close_price AS close,
    CASE 
        WHEN f.open_price > 0 
        THEN ((f.close_price - f.open_price) / f.open_price) * 100 
        ELSE 0 
    END AS price_change_pct
FROM agg a
JOIN first_last f USING (product_id, day)
ON CONFLICT (product_id, day) 
DO UPDATE SET
    trades = EXCLUDED.trades,
    volume = EXCLUDED.volume,
    vwap = EXCLUDED.vwap,
    high = GREATEST(gold.daily_kpis.high, EXCLUDED.high),
    low = LEAST(gold.daily_kpis.low, EXCLUDED.low),
    close = EXCLUDED.close,
    price_change_pct = EXCLUDED.price_change_pct;

-- ============================================
-- 4. Update Top Movers Snapshot
-- ============================================

-- Clear old snapshot and refresh
DELETE FROM gold.top_movers WHERE snapshot_ts < NOW() - INTERVAL '1 hour';

-- 24-hour movers
INSERT INTO gold.top_movers (product_id, period, price_change_pct, volume, trade_count)
SELECT
    product_id,
    '24h' AS period,
    CASE 
        WHEN first_price > 0 
        THEN ((last_price - first_price) / first_price) * 100 
        ELSE 0 
    END AS price_change_pct,
    volume,
    trade_count
FROM (
    SELECT
        product_id,
        FIRST_VALUE(price) OVER (PARTITION BY product_id ORDER BY trade_time) AS first_price,
        LAST_VALUE(price) OVER (PARTITION BY product_id ORDER BY trade_time 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_price,
        SUM(size) OVER (PARTITION BY product_id) AS volume,
        COUNT(*) OVER (PARTITION BY product_id) AS trade_count,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY trade_time) AS rn
    FROM silver.coinbase_trades
    WHERE trade_time >= NOW() - INTERVAL '24 hours'
) sub
WHERE rn = 1;

-- Log completion
DO $$
DECLARE
    v_ohlc_1m_count bigint;
    v_ohlc_1h_count bigint;
    v_daily_count bigint;
BEGIN
    SELECT COUNT(*) INTO v_ohlc_1m_count FROM gold.ohlc_1m;
    SELECT COUNT(*) INTO v_ohlc_1h_count FROM gold.ohlc_1h;
    SELECT COUNT(*) INTO v_daily_count FROM gold.daily_kpis;
    
    RAISE NOTICE 'Silver to Gold Transform Complete';
    RAISE NOTICE '1-Minute OHLC records: %', v_ohlc_1m_count;
    RAISE NOTICE '1-Hour OHLC records: %', v_ohlc_1h_count;
    RAISE NOTICE 'Daily KPI records: %', v_daily_count;
END $$;
