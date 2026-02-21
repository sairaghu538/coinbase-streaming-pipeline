"""
API routes for the Crypto Dashboard.
Each endpoint queries the Gold layer of the data warehouse
and returns JSON for the React frontend.
"""
from fastapi import APIRouter, Query
from api.database import query

router = APIRouter()


# ──────────────────────────────────────────────
# Products
# ──────────────────────────────────────────────
@router.get("/products")
def get_products():
    """Return list of distinct product IDs being tracked."""
    rows = query("""
        SELECT DISTINCT product_id
        FROM silver.coinbase_trades
        ORDER BY product_id
    """)
    return [row["product_id"] for row in rows]


# ──────────────────────────────────────────────
# OHLC Candlestick Data
# ──────────────────────────────────────────────
@router.get("/ohlc")
def get_ohlc(
    product_id: str = Query("BTC-USD", description="Product to query"),
    interval: str = Query("1m", description="Candle interval: 1m or 1h"),
    limit: int = Query(200, description="Max candles to return"),
):
    """Return OHLC candlestick data for charting."""
    table = "gold.ohlc_1m" if interval == "1m" else "gold.ohlc_1h"
    bucket_col = "bucket_1m" if interval == "1m" else "bucket_1h"

    rows = query(f"""
        SELECT
            {bucket_col} AS time,
            open::float,
            high::float,
            low::float,
            close::float,
            volume::float,
            trade_count
        FROM {table}
        WHERE product_id = %s
        ORDER BY {bucket_col} DESC
        LIMIT %s
    """, (product_id, limit))

    # Reverse so oldest is first (charts need ascending order)
    rows.reverse()

    # Convert timestamps to epoch seconds for lightweight-charts
    for row in rows:
        if row["time"]:
            row["time"] = int(row["time"].timestamp())

    return rows


# ──────────────────────────────────────────────
# Daily KPIs
# ──────────────────────────────────────────────
@router.get("/kpis")
def get_daily_kpis(
    product_id: str = Query(None, description="Filter by product (optional)"),
):
    """Return daily KPI metrics from the Gold layer."""
    if product_id:
        rows = query("""
            SELECT
                product_id,
                day,
                volume::float,
                trades,
                vwap::float,
                open::float AS open_price,
                close::float AS close_price,
                high::float AS high_price,
                low::float AS low_price,
                price_change_pct::float
            FROM gold.daily_kpis
            WHERE product_id = %s
            ORDER BY day DESC
            LIMIT 30
        """, (product_id,))
    else:
        rows = query("""
            SELECT
                product_id,
                day,
                volume::float,
                trades,
                vwap::float,
                open::float AS open_price,
                close::float AS close_price,
                high::float AS high_price,
                low::float AS low_price,
                price_change_pct::float
            FROM gold.daily_kpis
            ORDER BY day DESC
            LIMIT 30
        """)

    # Convert date objects to strings
    for row in rows:
        if row.get("day"):
            row["day"] = row["day"].isoformat()

    return rows


# ──────────────────────────────────────────────
# Top Movers
# ──────────────────────────────────────────────
@router.get("/top-movers")
def get_top_movers():
    """Return latest top movers snapshot."""
    rows = query("""
        SELECT
            product_id,
            snapshot_ts,
            period,
            price_change_pct::float,
            volume::float,
            trade_count
        FROM gold.top_movers
        ORDER BY snapshot_ts DESC, abs(price_change_pct) DESC
        LIMIT 10
    """)

    for row in rows:
        if row.get("snapshot_ts"):
            row["snapshot_ts"] = row["snapshot_ts"].isoformat()

    return rows


# ──────────────────────────────────────────────
# Pipeline Status / Health
# ──────────────────────────────────────────────
@router.get("/status")
def get_pipeline_status():
    """Return pipeline health: record counts per layer."""
    bronze = query("SELECT COUNT(*) as count FROM bronze.coinbase_trades_raw")
    silver = query("SELECT COUNT(*) as count FROM silver.coinbase_trades")
    gold_1m = query("SELECT COUNT(*) as count FROM gold.ohlc_1m")
    gold_1h = query("SELECT COUNT(*) as count FROM gold.ohlc_1h")

    latest_bronze = query("""
        SELECT MAX(event_ts) as latest
        FROM bronze.coinbase_trades_raw
    """)

    return {
        "bronze_records": bronze[0]["count"],
        "silver_records": silver[0]["count"],
        "gold_1m_records": gold_1m[0]["count"],
        "gold_1h_records": gold_1h[0]["count"],
        "latest_ingest": latest_bronze[0]["latest"].isoformat()
            if latest_bronze[0]["latest"] else None,
    }
