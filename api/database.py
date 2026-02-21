"""
Database connection module for the Crypto Dashboard API.
Reads connection details from .env file and provides
a reusable connection function.
"""
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    """Create and return a new database connection with dict cursor."""
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
        database=os.getenv("PGDATABASE", "crypto_dw"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD"),
        cursor_factory=RealDictCursor,
    )


def query(sql: str, params=None) -> list[dict]:
    """Execute a SQL query and return results as list of dicts."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            return [dict(row) for row in rows]
    finally:
        conn.close()
