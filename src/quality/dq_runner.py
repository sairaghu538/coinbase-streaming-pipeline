"""
Data Quality Runner

Executes data quality checks defined in dq_checks.sql and stores
results in the bronze.dq_check_results table. Supports alerting
via console output (can be extended for Slack/email).
"""

import json
import os
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database config
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = int(os.getenv("PGPORT", "5432"))
DB_NAME = os.getenv("PGDATABASE", "crypto_dw")
DB_USER = os.getenv("PGUSER", "postgres")
DB_PASS = os.getenv("PGPASSWORD", "")

# Check definitions - each is a standalone query that returns check_name, passed, details
DQ_CHECKS = [
    {
        "name": "bronze_freshness",
        "type": "freshness",
        "table": "bronze.coinbase_trades_raw",
        "sql": """
            SELECT 
                'bronze_freshness' AS check_name,
                CASE 
                    WHEN MAX(ingest_ts) >= NOW() - INTERVAL '10 minutes' THEN TRUE 
                    ELSE FALSE 
                END AS passed,
                jsonb_build_object(
                    'last_ingest_ts', MAX(ingest_ts)::text,
                    'age_seconds', EXTRACT(EPOCH FROM (NOW() - MAX(ingest_ts)))
                ) AS details
            FROM bronze.coinbase_trades_raw
        """
    },
    {
        "name": "silver_freshness",
        "type": "freshness",
        "table": "silver.coinbase_trades",
        "sql": """
            SELECT 
                'silver_freshness' AS check_name,
                CASE 
                    WHEN MAX(trade_time) >= NOW() - INTERVAL '15 minutes' THEN TRUE 
                    ELSE FALSE 
                END AS passed,
                jsonb_build_object(
                    'last_trade_time', MAX(trade_time)::text,
                    'age_seconds', EXTRACT(EPOCH FROM (NOW() - MAX(trade_time)))
                ) AS details
            FROM silver.coinbase_trades
        """
    },
    {
        "name": "bronze_volume_5m",
        "type": "volume",
        "table": "bronze.coinbase_trades_raw",
        "sql": """
            SELECT 
                'bronze_volume_5m' AS check_name,
                CASE WHEN COUNT(*) > 0 THEN TRUE ELSE FALSE END AS passed,
                jsonb_build_object('record_count', COUNT(*)) AS details
            FROM bronze.coinbase_trades_raw
            WHERE ingest_ts >= NOW() - INTERVAL '5 minutes'
        """
    },
    {
        "name": "silver_no_null_price",
        "type": "null_check",
        "table": "silver.coinbase_trades",
        "sql": """
            SELECT 
                'silver_no_null_price' AS check_name,
                CASE WHEN COUNT(*) = 0 THEN TRUE ELSE FALSE END AS passed,
                jsonb_build_object('null_count', COUNT(*)) AS details
            FROM silver.coinbase_trades
            WHERE price IS NULL
        """
    },
    {
        "name": "silver_no_duplicates",
        "type": "duplicate",
        "table": "silver.coinbase_trades",
        "sql": """
            SELECT 
                'silver_no_duplicates' AS check_name,
                CASE WHEN COUNT(*) = 0 THEN TRUE ELSE FALSE END AS passed,
                jsonb_build_object('duplicate_count', COUNT(*)) AS details
            FROM (
                SELECT trade_id 
                FROM silver.coinbase_trades 
                GROUP BY trade_id 
                HAVING COUNT(*) > 1
            ) dups
        """
    },
    {
        "name": "silver_no_negative_price",
        "type": "range",
        "table": "silver.coinbase_trades",
        "sql": """
            SELECT 
                'silver_no_negative_price' AS check_name,
                CASE WHEN COUNT(*) = 0 THEN TRUE ELSE FALSE END AS passed,
                jsonb_build_object('negative_count', COUNT(*)) AS details
            FROM silver.coinbase_trades
            WHERE price < 0
        """
    }
]


def get_db_connection():
    """Create and return a database connection."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )


def run_check(conn, check: Dict[str, Any]) -> Dict[str, Any]:
    """Execute a single DQ check and return the result."""
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(check["sql"])
            result = cur.fetchone()
            return {
                "check_name": check["name"],
                "check_type": check["type"],
                "table_name": check["table"],
                "passed": result["passed"] if result else False,
                "details": result["details"] if result else {},
                "error": None
            }
    except Exception as e:
        return {
            "check_name": check["name"],
            "check_type": check["type"],
            "table_name": check["table"],
            "passed": False,
            "details": {},
            "error": str(e)
        }


def save_results(conn, results: List[Dict[str, Any]], run_id: str):
    """Save check results to the database."""
    insert_sql = """
        INSERT INTO bronze.dq_check_results 
        (check_name, check_type, table_name, passed, details, run_id)
        VALUES (%s, %s, %s, %s, %s::jsonb, %s)
    """
    
    with conn.cursor() as cur:
        for result in results:
            cur.execute(insert_sql, (
                result["check_name"],
                result["check_type"],
                result["table_name"],
                result["passed"],
                json.dumps(result["details"]),
                run_id
            ))
    conn.commit()


def print_results(results: List[Dict[str, Any]]):
    """Print results to console with formatting."""
    print("\n" + "=" * 60)
    print("DATA QUALITY CHECK RESULTS")
    print("=" * 60)
    
    passed = [r for r in results if r["passed"]]
    failed = [r for r in results if not r["passed"]]
    
    print(f"\n[PASSED] {len(passed)}/{len(results)}")
    for r in passed:
        print(f"   • {r['check_name']}: {r['details']}")
    
    if failed:
        print(f"\n[FAILED] {len(failed)}/{len(results)}")
        for r in failed:
            print(f"   • {r['check_name']}: {r['details']}")
            if r.get("error"):
                print(f"     Error: {r['error']}")
    
    print("\n" + "=" * 60)
    
    return len(failed) == 0


def main():
    """Main entry point for DQ runner."""
    run_id = str(uuid.uuid4())[:8]
    print(f"[DQ] Starting data quality checks (run_id: {run_id})")
    
    try:
        conn = get_db_connection()
    except Exception as e:
        print(f"[FATAL] Could not connect to database: {e}")
        sys.exit(1)
    
    results = []
    for check in DQ_CHECKS:
        print(f"[DQ] Running: {check['name']}...")
        result = run_check(conn, check)
        results.append(result)
    
    # Save results
    try:
        save_results(conn, results, run_id)
        print(f"[DQ] Results saved to bronze.dq_check_results")
    except Exception as e:
        print(f"[WARN] Could not save results: {e}")
    
    # Print summary
    all_passed = print_results(results)
    
    conn.close()
    
    # Exit with appropriate code
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
