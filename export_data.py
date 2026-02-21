"""Export Gold layer data to CSV for Streamlit Cloud deployment."""
import psycopg2
import csv
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("PGHOST"),
    port=os.getenv("PGPORT"),
    database=os.getenv("PGDATABASE"),
    user=os.getenv("PGUSER"),
    password=os.getenv("PGPASSWORD"),
)

os.makedirs("data", exist_ok=True)

tables = {
    "ohlc_1m": "SELECT product_id, bucket_1m, open, high, low, close, volume, trade_count, vwap FROM gold.ohlc_1m ORDER BY bucket_1m",
    "ohlc_1h": "SELECT product_id, bucket_1h, open, high, low, close, volume, trade_count, vwap FROM gold.ohlc_1h ORDER BY bucket_1h",
    "daily_kpis": "SELECT product_id, day, trades, volume, vwap, high, low, open, close, price_change_pct FROM gold.daily_kpis ORDER BY day",
    "pipeline_status": """
        SELECT 
            (SELECT COUNT(*) FROM bronze.coinbase_trades_raw) as bronze_records,
            (SELECT COUNT(*) FROM silver.coinbase_trades) as silver_records,
            (SELECT COUNT(*) FROM gold.ohlc_1m) as gold_1m_records,
            (SELECT COUNT(*) FROM gold.ohlc_1h) as gold_1h_records
    """,
}

for name, sql in tables.items():
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]
    
    filepath = f"data/{name}.csv"
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(rows)
    
    print(f"Exported {len(rows)} rows to {filepath}")
    cur.close()

conn.close()
print("Done!")
