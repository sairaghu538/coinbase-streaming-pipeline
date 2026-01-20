-- ============================================
-- Data Quality Checks
-- ============================================
-- Run these checks after each pipeline run to
-- validate data integrity and freshness.
--
-- Each check returns a result row with:
-- - check_name: identifier for the check
-- - passed: boolean indicating success/failure
-- - details: JSON with additional context
-- ============================================

-- ============================================
-- 1. FRESHNESS CHECKS
-- ============================================

-- Check: Bronze layer freshness (last 10 minutes)
SELECT 
    'bronze_freshness' AS check_name,
    CASE 
        WHEN MAX(ingest_ts) >= NOW() - INTERVAL '10 minutes' THEN TRUE 
        ELSE FALSE 
    END AS passed,
    jsonb_build_object(
        'last_ingest_ts', MAX(ingest_ts),
        'age_seconds', EXTRACT(EPOCH FROM (NOW() - MAX(ingest_ts))),
        'threshold_seconds', 600
    ) AS details
FROM bronze.coinbase_trades_raw;

-- Check: Silver layer freshness
SELECT 
    'silver_freshness' AS check_name,
    CASE 
        WHEN MAX(trade_time) >= NOW() - INTERVAL '15 minutes' THEN TRUE 
        ELSE FALSE 
    END AS passed,
    jsonb_build_object(
        'last_trade_time', MAX(trade_time),
        'age_seconds', EXTRACT(EPOCH FROM (NOW() - MAX(trade_time))),
        'threshold_seconds', 900
    ) AS details
FROM silver.coinbase_trades;

-- ============================================
-- 2. VOLUME CHECKS
-- ============================================

-- Check: Bronze has records in last 5 minutes
SELECT 
    'bronze_volume_5m' AS check_name,
    CASE WHEN COUNT(*) > 0 THEN TRUE ELSE FALSE END AS passed,
    jsonb_build_object(
        'record_count', COUNT(*),
        'window_minutes', 5
    ) AS details
FROM bronze.coinbase_trades_raw
WHERE ingest_ts >= NOW() - INTERVAL '5 minutes';

-- Check: Silver has records in last 10 minutes
SELECT 
    'silver_volume_10m' AS check_name,
    CASE WHEN COUNT(*) > 0 THEN TRUE ELSE FALSE END AS passed,
    jsonb_build_object(
        'record_count', COUNT(*),
        'window_minutes', 10
    ) AS details
FROM silver.coinbase_trades
WHERE trade_time >= NOW() - INTERVAL '10 minutes';

-- ============================================
-- 3. NULL CHECKS
-- ============================================

-- Check: No null prices in Silver
SELECT 
    'silver_no_null_price' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN TRUE ELSE FALSE END AS passed,
    jsonb_build_object(
        'null_count', COUNT(*),
        'sample_trade_ids', (
            SELECT ARRAY_AGG(trade_id) 
            FROM (
                SELECT trade_id 
                FROM silver.coinbase_trades 
                WHERE price IS NULL 
                LIMIT 5
            ) s
        )
    ) AS details
FROM silver.coinbase_trades
WHERE price IS NULL;

-- Check: No null product_ids in Silver
SELECT 
    'silver_no_null_product' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN TRUE ELSE FALSE END AS passed,
    jsonb_build_object('null_count', COUNT(*)) AS details
FROM silver.coinbase_trades
WHERE product_id IS NULL;

-- ============================================
-- 4. DUPLICATE CHECKS
-- ============================================

-- Check: No duplicate trade_ids in Silver
SELECT 
    'silver_no_duplicates' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN TRUE ELSE FALSE END AS passed,
    jsonb_build_object(
        'duplicate_count', COUNT(*),
        'sample_ids', (
            SELECT ARRAY_AGG(trade_id) 
            FROM (
                SELECT trade_id 
                FROM silver.coinbase_trades 
                GROUP BY trade_id 
                HAVING COUNT(*) > 1
                LIMIT 5
            ) s
        )
    ) AS details
FROM (
    SELECT trade_id 
    FROM silver.coinbase_trades 
    GROUP BY trade_id 
    HAVING COUNT(*) > 1
) dups;

-- ============================================
-- 5. REFERENTIAL INTEGRITY CHECKS
-- ============================================

-- Check: All Gold OHLC products exist in Silver
SELECT 
    'gold_ohlc_product_integrity' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN TRUE ELSE FALSE END AS passed,
    jsonb_build_object(
        'orphan_count', COUNT(*),
        'orphan_products', ARRAY_AGG(DISTINCT g.product_id)
    ) AS details
FROM gold.ohlc_1m g
LEFT JOIN (
    SELECT DISTINCT product_id FROM silver.coinbase_trades
) s ON g.product_id = s.product_id
WHERE s.product_id IS NULL;

-- ============================================
-- 6. VALUE RANGE CHECKS
-- ============================================

-- Check: No negative prices
SELECT 
    'silver_no_negative_price' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN TRUE ELSE FALSE END AS passed,
    jsonb_build_object(
        'negative_count', COUNT(*),
        'min_price', MIN(price)
    ) AS details
FROM silver.coinbase_trades
WHERE price < 0;

-- Check: No zero volume trades
SELECT 
    'silver_no_zero_size' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN TRUE ELSE FALSE END AS passed,
    jsonb_build_object(
        'zero_count', COUNT(*)
    ) AS details
FROM silver.coinbase_trades
WHERE size = 0;

-- ============================================
-- 7. QUARANTINE MONITORING
-- ============================================

-- Check: Quarantine rate under threshold
SELECT 
    'quarantine_rate' AS check_name,
    CASE 
        WHEN (quarantine_count::float / NULLIF(total_count, 0)) < 0.05 THEN TRUE 
        ELSE FALSE 
    END AS passed,
    jsonb_build_object(
        'quarantine_count', quarantine_count,
        'total_count', total_count,
        'quarantine_rate_pct', 
            ROUND((quarantine_count::numeric / NULLIF(total_count, 0)) * 100, 2),
        'threshold_pct', 5
    ) AS details
FROM (
    SELECT 
        (SELECT COUNT(*) FROM silver.coinbase_trades_quarantine) AS quarantine_count,
        (SELECT COUNT(*) FROM bronze.coinbase_trades_raw) AS total_count
) counts;

-- ============================================
-- SUMMARY VIEW (Optional - Create as materialized view)
-- ============================================

-- DROP MATERIALIZED VIEW IF EXISTS bronze.dq_summary;
-- CREATE MATERIALIZED VIEW bronze.dq_summary AS
-- <union all the checks above>
