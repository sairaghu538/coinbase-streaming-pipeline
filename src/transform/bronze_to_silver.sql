-- ============================================
-- Bronze to Silver Transformation
-- ============================================
-- Transforms raw JSON trades from Bronze into 
-- typed, deduplicated records in Silver.
--
-- This script is idempotent - safe to run multiple times.
-- ============================================

-- Transform and load new trades
INSERT INTO silver.coinbase_trades (
    trade_id,
    product_id,
    price,
    size,
    side,
    trade_time,
    maker_order_id,
    taker_order_id,
    ingest_ts
)
SELECT
    (payload->>'trade_id')::bigint           AS trade_id,
    payload->>'product_id'                   AS product_id,
    (payload->>'price')::numeric(18,8)       AS price,
    (payload->>'size')::numeric(18,8)        AS size,
    payload->>'side'                         AS side,
    (payload->>'time')::timestamptz          AS trade_time,
    payload->>'maker_order_id'               AS maker_order_id,
    payload->>'taker_order_id'               AS taker_order_id,
    ingest_ts                                AS ingest_ts
FROM bronze.coinbase_trades_raw b
WHERE 
    -- Only process records with valid trade_id
    payload ? 'trade_id'
    AND payload->>'trade_id' IS NOT NULL
    -- Basic validation
    AND payload->>'price' IS NOT NULL
    AND payload->>'size' IS NOT NULL
    AND payload->>'product_id' IS NOT NULL
    AND payload->>'side' IN ('buy', 'sell')
ON CONFLICT (trade_id) DO NOTHING;

-- Log transformation stats
DO $$
DECLARE
    v_bronze_count bigint;
    v_silver_count bigint;
BEGIN
    SELECT COUNT(*) INTO v_bronze_count FROM bronze.coinbase_trades_raw;
    SELECT COUNT(*) INTO v_silver_count FROM silver.coinbase_trades;
    
    RAISE NOTICE 'Bronze to Silver Transform Complete';
    RAISE NOTICE 'Bronze records: %', v_bronze_count;
    RAISE NOTICE 'Silver records: %', v_silver_count;
END $$;

-- ============================================
-- Quarantine invalid records (optional)
-- ============================================
-- Move records that fail validation to quarantine table

INSERT INTO silver.coinbase_trades_quarantine (
    source_id,
    error_reason,
    payload
)
SELECT
    b.id AS source_id,
    CASE
        WHEN NOT (payload ? 'trade_id') THEN 'missing_trade_id'
        WHEN payload->>'price' IS NULL THEN 'missing_price'
        WHEN payload->>'size' IS NULL THEN 'missing_size'
        WHEN payload->>'product_id' IS NULL THEN 'missing_product_id'
        WHEN payload->>'side' NOT IN ('buy', 'sell') THEN 'invalid_side'
        ELSE 'unknown'
    END AS error_reason,
    payload
FROM bronze.coinbase_trades_raw b
WHERE 
    -- Find invalid records
    NOT (payload ? 'trade_id')
    OR payload->>'trade_id' IS NULL
    OR payload->>'price' IS NULL
    OR payload->>'size' IS NULL
    OR payload->>'product_id' IS NULL
    OR payload->>'side' NOT IN ('buy', 'sell')
    -- Don't re-quarantine already processed records
    AND NOT EXISTS (
        SELECT 1 FROM silver.coinbase_trades_quarantine q 
        WHERE q.source_id = b.id
    );
