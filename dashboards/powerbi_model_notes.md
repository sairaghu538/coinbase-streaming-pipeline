# Power BI Dashboard Setup

This document explains how to connect Power BI Desktop to the PostgreSQL Gold layer and build effective dashboards.

## Prerequisites

- Power BI Desktop (free from Microsoft Store)
- PostgreSQL ODBC driver or npgsql connector
- Completed Gold layer with data

## Connection Setup

### Option 1: PostgreSQL Connector (Recommended)

1. Open Power BI Desktop
2. Click **Get Data** → **Database** → **PostgreSQL database**
3. Enter connection details:
   - Server: `localhost`
   - Database: `crypto_dw`
   - Data Connectivity mode: **DirectQuery** (for real-time) or **Import** (for offline)
4. Enter credentials when prompted

### Option 2: ODBC Connection

1. Install PostgreSQL ODBC driver
2. Create DSN in Windows ODBC Data Source Administrator
3. In Power BI: **Get Data** → **ODBC** → Select your DSN

## Recommended Data Model

### Tables to Import

| Table | Purpose | Refresh Frequency |
|-------|---------|-------------------|
| `gold.ohlc_1m` | 1-minute price charts | Real-time / 1 min |
| `gold.ohlc_1h` | Hourly trend analysis | Hourly |
| `gold.daily_kpis` | Daily summary cards | Daily |
| `gold.top_movers` | Volatility snapshots | On-demand |

### Relationships

```
gold.daily_kpis (product_id) ←→ gold.ohlc_1h (product_id)
gold.ohlc_1h (product_id, bucket_1h) ←→ gold.ohlc_1m (product_id, bucket_1m truncated)
```

## Dashboard Pages

### Page 1: Market Overview

**Purpose**: High-level view of all tracked products

**Visuals**:
1. **Card**: Latest BTC-USD price (from `gold.ohlc_1m` last record)
2. **Card**: 24h volume (from `gold.daily_kpis`)
3. **Card**: 24h price change % (from `gold.daily_kpis`)
4. **Table**: All products with daily metrics

**DAX Measures**:
```dax
Latest Price = 
CALCULATE(
    MAX(ohlc_1m[close]),
    FILTER(
        ohlc_1m,
        ohlc_1m[bucket_1m] = MAX(ohlc_1m[bucket_1m])
    )
)

24h Change % = 
VAR CurrentClose = [Latest Price]
VAR Open24h = CALCULATE(
    MIN(ohlc_1h[open]),
    FILTER(
        ohlc_1h,
        ohlc_1h[bucket_1h] >= NOW() - 1
    )
)
RETURN
DIVIDE(CurrentClose - Open24h, Open24h, 0) * 100
```

### Page 2: Price Analysis

**Purpose**: Detailed price movement analysis

**Visuals**:
1. **Line Chart**: OHLC 1-minute prices over time
   - X-axis: `bucket_1m`
   - Y-axis: `close`, `high`, `low`
   - Filter: Product selector

2. **Candlestick Chart** (if available):
   - Use custom visual from AppSource
   - Map: Open, High, Low, Close, bucket_1m

3. **Bar Chart**: Hourly volume
   - X-axis: `bucket_1h`
   - Y-axis: `volume`

### Page 3: Daily Metrics

**Purpose**: Day-over-day comparison

**Visuals**:
1. **Matrix**: Products × Days with volume heatmap
2. **Line Chart**: Daily VWAP trend
3. **Column Chart**: Daily trade count by product
4. **KPI**: Daily high/low range

### Page 4: Data Quality

**Purpose**: Pipeline health monitoring

**Recommended approach**: Create a simple table/view in PostgreSQL:

```sql
CREATE VIEW gold.dq_summary AS
SELECT 
    check_ts::date AS check_date,
    check_name,
    passed,
    details
FROM bronze.dq_check_results
WHERE check_ts >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY check_ts DESC;
```

**Visuals**:
1. **Card**: DQ Pass Rate (last 24h)
2. **Table**: Failed checks detail
3. **Line Chart**: Check pass rate over time

## Slicer Configuration

Add these slicers to every page:

1. **Product Selector**: `product_id` from any gold table
2. **Date Range**: `bucket_1m` or `day` field
3. **Time Granularity**: Toggle between 1m/1h/1d views

## Refresh Schedule

| Mode | Best For | Refresh |
|------|----------|---------|
| DirectQuery | Real-time monitoring | Live |
| Import | Historical analysis | Scheduled (hourly/daily) |

For local development, **Import** mode is simpler.

## Sample Queries for Testing

Before building dashboards, verify data in pgAdmin:

```sql
-- Recent prices
SELECT product_id, bucket_1m, open, close, volume
FROM gold.ohlc_1m
WHERE bucket_1m >= NOW() - INTERVAL '1 hour'
ORDER BY bucket_1m DESC;

-- Daily summary
SELECT * FROM gold.daily_kpis
WHERE day >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY day DESC, product_id;
```

## Alternative: Metabase (Free & Open Source)

If Power BI is not available:

1. Download Metabase JAR or use Docker
2. Connect to PostgreSQL
3. Create similar dashboards
4. Fully browser-based

Metabase query example:
```sql
SELECT 
    bucket_1h,
    product_id,
    close,
    volume
FROM gold.ohlc_1h
WHERE bucket_1h >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
```

## Tips for Interview Demos

1. **Start with the big picture**: Show the architecture diagram first
2. **Show live data**: Run ingestion, then show dashboard updating
3. **Highlight data quality**: Show DQ checks passing
4. **Explain design decisions**: Why Medallion? Why Postgres?
5. **Show the code**: Walk through ingestion and transformation logic

---

*This dashboard setup documentation is part of the coinbase-streaming-pipeline project.*
