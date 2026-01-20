-- ============================================
-- Coinbase Streaming Pipeline - Schema Setup
-- ============================================
-- Medallion Architecture: Bronze / Silver / Gold
-- ============================================

-- Create schemas for each layer
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Grant usage (adjust role name as needed)
-- GRANT USAGE ON SCHEMA bronze TO your_app_user;
-- GRANT USAGE ON SCHEMA silver TO your_app_user;
-- GRANT USAGE ON SCHEMA gold TO your_app_user;

COMMENT ON SCHEMA bronze IS 'Raw layer: append-only, stores raw JSON payloads as received from sources';
COMMENT ON SCHEMA silver IS 'Clean layer: typed, deduplicated, validated data';
COMMENT ON SCHEMA gold IS 'Business layer: aggregated metrics and KPIs ready for BI consumption';
