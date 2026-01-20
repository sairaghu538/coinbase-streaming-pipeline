"""
Prefect Orchestration Flow

Orchestrates the complete data pipeline:
1. Bronze to Silver transformation
2. Silver to Gold aggregation
3. Data quality checks

Run locally with:
    prefect server start  # (in separate terminal)
    python src/orchestration/prefect_flow.py
"""

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import psycopg2
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.artifacts import create_markdown_artifact

# Load environment variables
load_dotenv()

# Database config
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = int(os.getenv("PGPORT", "5432"))
DB_NAME = os.getenv("PGDATABASE", "crypto_dw")
DB_USER = os.getenv("PGUSER", "postgres")
DB_PASS = os.getenv("PGPASSWORD", "")

# SQL file paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
BRONZE_TO_SILVER_SQL = PROJECT_ROOT / "src" / "transform" / "bronze_to_silver.sql"
SILVER_TO_GOLD_SQL = PROJECT_ROOT / "src" / "transform" / "silver_to_gold.sql"


def get_db_connection():
    """Create and return a database connection."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )


def execute_sql_file(conn, sql_path: Path, description: str) -> dict:
    """Execute a SQL file and return execution stats."""
    logger = get_run_logger()
    
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")
    
    sql = sql_path.read_text()
    
    logger.info(f"Executing {description}...")
    start_time = datetime.now()
    
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    
    duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"Completed {description} in {duration:.2f}s")
    
    return {
        "description": description,
        "duration_seconds": duration,
        "sql_file": str(sql_path.name)
    }


@task(
    name="Bronze to Silver Transform",
    description="Transform raw JSON trades to typed records",
    retries=2,
    retry_delay_seconds=30
)
def transform_bronze_to_silver() -> dict:
    """Execute Bronze to Silver transformation."""
    logger = get_run_logger()
    
    conn = get_db_connection()
    try:
        # Get counts before
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM bronze.coinbase_trades_raw")
            bronze_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM silver.coinbase_trades")
            silver_before = cur.fetchone()[0]
        
        # Execute transform
        result = execute_sql_file(conn, BRONZE_TO_SILVER_SQL, "Bronze to Silver")
        
        # Get counts after
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM silver.coinbase_trades")
            silver_after = cur.fetchone()[0]
        
        result["bronze_records"] = bronze_count
        result["silver_before"] = silver_before
        result["silver_after"] = silver_after
        result["new_records"] = silver_after - silver_before
        
        logger.info(f"Transformed {result['new_records']} new records to Silver")
        
        return result
    finally:
        conn.close()


@task(
    name="Silver to Gold Transform",
    description="Build aggregated metrics for BI",
    retries=2,
    retry_delay_seconds=30
)
def transform_silver_to_gold() -> dict:
    """Execute Silver to Gold transformation."""
    logger = get_run_logger()
    
    conn = get_db_connection()
    try:
        # Get counts before
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM gold.ohlc_1m")
            ohlc_before = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM gold.daily_kpis")
            kpi_before = cur.fetchone()[0]
        
        # Execute transform
        result = execute_sql_file(conn, SILVER_TO_GOLD_SQL, "Silver to Gold")
        
        # Get counts after
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM gold.ohlc_1m")
            ohlc_after = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM gold.daily_kpis")
            kpi_after = cur.fetchone()[0]
        
        result["ohlc_1m_before"] = ohlc_before
        result["ohlc_1m_after"] = ohlc_after
        result["daily_kpis_before"] = kpi_before
        result["daily_kpis_after"] = kpi_after
        
        logger.info(f"OHLC 1m: {ohlc_after} records, Daily KPIs: {kpi_after} records")
        
        return result
    finally:
        conn.close()


@task(
    name="Run Data Quality Checks",
    description="Execute DQ checks and log results"
)
def run_dq_checks() -> dict:
    """Run data quality checks."""
    logger = get_run_logger()
    
    # Import the DQ runner
    sys.path.insert(0, str(PROJECT_ROOT / "src" / "quality"))
    from dq_runner import DQ_CHECKS, run_check, get_db_connection
    
    conn = get_db_connection()
    
    results = []
    for check in DQ_CHECKS:
        result = run_check(conn, check)
        results.append(result)
        
        status = "✅" if result["passed"] else "❌"
        logger.info(f"{status} {check['name']}")
    
    conn.close()
    
    passed = sum(1 for r in results if r["passed"])
    failed = len(results) - passed
    
    summary = {
        "total_checks": len(results),
        "passed": passed,
        "failed": failed,
        "checks": results
    }
    
    if failed > 0:
        logger.warning(f"DQ checks: {failed}/{len(results)} failed!")
    else:
        logger.info(f"DQ checks: All {passed} checks passed!")
    
    return summary


@task(name="Create Pipeline Report")
async def create_pipeline_report(
    bronze_to_silver: dict,
    silver_to_gold: dict,
    dq_results: dict
):
    """Create a markdown artifact summarizing the pipeline run."""
    
    report = f"""
# Pipeline Run Report
**Timestamp:** {datetime.now().isoformat()}

## Bronze → Silver
- Duration: {bronze_to_silver['duration_seconds']:.2f}s
- New records: {bronze_to_silver.get('new_records', 'N/A')}
- Total Silver records: {bronze_to_silver.get('silver_after', 'N/A')}

## Silver → Gold
- Duration: {silver_to_gold['duration_seconds']:.2f}s
- OHLC 1m records: {silver_to_gold.get('ohlc_1m_after', 'N/A')}
- Daily KPI records: {silver_to_gold.get('daily_kpis_after', 'N/A')}

## Data Quality
- Passed: {dq_results['passed']}/{dq_results['total_checks']}
- Status: {'✅ All Passed' if dq_results['failed'] == 0 else '⚠️ Some Failed'}
"""
    
    await create_markdown_artifact(
        key="pipeline-run-report",
        markdown=report,
        description="Summary of the latest pipeline run"
    )


@flow(
    name="Coinbase Data Pipeline",
    description="Transform Coinbase trade data through Medallion layers"
)
def coinbase_pipeline(run_dq: bool = True):
    """
    Main pipeline flow.
    
    Args:
        run_dq: Whether to run data quality checks (default: True)
    """
    logger = get_run_logger()
    logger.info("=" * 60)
    logger.info("Starting Coinbase Data Pipeline")
    logger.info("=" * 60)
    
    # Step 1: Bronze to Silver
    bronze_result = transform_bronze_to_silver()
    
    # Step 2: Silver to Gold
    gold_result = transform_silver_to_gold()
    
    # Step 3: Data Quality (optional)
    dq_result = {"total_checks": 0, "passed": 0, "failed": 0, "checks": []}
    if run_dq:
        dq_result = run_dq_checks()
    
    # Step 4: Create report
    create_pipeline_report(bronze_result, gold_result, dq_result)
    
    logger.info("=" * 60)
    logger.info("Pipeline completed successfully!")
    logger.info("=" * 60)
    
    return {
        "bronze_to_silver": bronze_result,
        "silver_to_gold": gold_result,
        "dq_checks": dq_result
    }


@flow(name="Scheduled Pipeline Runner")
def scheduled_runner():
    """
    Wrapper flow for scheduled execution.
    Runs the main pipeline with default settings.
    """
    return coinbase_pipeline(run_dq=True)


if __name__ == "__main__":
    # Run the pipeline manually
    result = coinbase_pipeline()
    print("\nPipeline Result:")
    print(f"  Bronze→Silver: {result['bronze_to_silver'].get('new_records', 0)} new records")
    print(f"  DQ Checks: {result['dq_checks']['passed']}/{result['dq_checks']['total_checks']} passed")
