"""
Coinbase WebSocket to Bronze Layer Ingestion

This script connects to Coinbase's public WebSocket feed and streams
real-time trade data (matches) into the Bronze layer of our data warehouse.

Features:
- Async WebSocket connection with automatic reconnection
- Batch inserts for performance
- Payload hashing for deduplication safety
- Graceful shutdown handling
"""

import asyncio
import hashlib
import json
import os
import signal
import sys
from datetime import datetime, timezone
from typing import Optional

import psycopg2
from psycopg2.extras import execute_values
import websockets
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
WS_URL = os.getenv("COINBASE_WS_URL", "wss://ws-feed.exchange.coinbase.com")
PRODUCTS = os.getenv("COINBASE_PRODUCTS", "BTC-USD,ETH-USD").split(",")
CHANNEL = "matches"

# Database config
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = int(os.getenv("PGPORT", "5432"))
DB_NAME = os.getenv("PGDATABASE", "crypto_dw")
DB_USER = os.getenv("PGUSER", "postgres")
DB_PASS = os.getenv("PGPASSWORD", "")

# Pipeline settings
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
FLUSH_INTERVAL = int(os.getenv("FLUSH_INTERVAL_SECONDS", "5"))

# SQL for batch insert
INSERT_SQL = """
INSERT INTO bronze.coinbase_trades_raw (channel, payload, payload_hash, event_ts)
VALUES %s
"""

# Global state
shutdown_requested = False
message_buffer = []
stats = {"received": 0, "inserted": 0, "errors": 0}


def sha1_hash(payload: dict) -> str:
    """Generate SHA1 hash of payload for deduplication."""
    s = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha1(s).hexdigest()


def parse_event_ts(payload: dict) -> Optional[datetime]:
    """Extract and parse event timestamp from Coinbase payload."""
    t = payload.get("time")
    if not t:
        return None
    try:
        # Handle ISO format with Z suffix
        if t.endswith("Z"):
            t = t[:-1] + "+00:00"
        return datetime.fromisoformat(t)
    except Exception:
        return None


def get_db_connection():
    """Create and return a database connection."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )


def flush_buffer(conn, buffer: list) -> int:
    """Flush the message buffer to the database."""
    if not buffer:
        return 0
    
    try:
        with conn.cursor() as cur:
            # Prepare data for batch insert
            values = [
                (
                    msg["channel"],
                    json.dumps(msg["payload"]),
                    msg["hash"],
                    msg["event_ts"]
                )
                for msg in buffer
            ]
            
            execute_values(
                cur,
                INSERT_SQL,
                values,
                template="(%s, %s::jsonb, %s, %s)"
            )
            conn.commit()
            return len(values)
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Failed to flush buffer: {e}")
        stats["errors"] += 1
        return 0


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    global shutdown_requested
    print(f"\n[INFO] Received signal {signum}, initiating graceful shutdown...")
    shutdown_requested = True


async def flush_timer(conn):
    """Periodically flush the buffer based on time interval."""
    global message_buffer
    
    while not shutdown_requested:
        await asyncio.sleep(FLUSH_INTERVAL)
        
        if message_buffer:
            count = flush_buffer(conn, message_buffer)
            if count > 0:
                stats["inserted"] += count
                print(f"[FLUSH] Timer: Inserted {count} records. Total: {stats['inserted']}")
            message_buffer = []


async def stream_trades(conn):
    """Connect to WebSocket and stream trades to buffer."""
    global message_buffer, shutdown_requested
    
    subscribe_msg = {
        "type": "subscribe",
        "product_ids": PRODUCTS,
        "channels": [CHANNEL],
    }
    
    reconnect_delay = 1
    max_reconnect_delay = 60
    
    while not shutdown_requested:
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5
            ) as ws:
                # Subscribe to channels
                await ws.send(json.dumps(subscribe_msg))
                print(f"[INFO] Connected and subscribed to: {PRODUCTS}")
                reconnect_delay = 1  # Reset on successful connection
                
                async for message in ws:
                    if shutdown_requested:
                        break
                    
                    try:
                        payload = json.loads(message)
                        
                        # Only process match (trade) messages
                        if payload.get("type") != "match":
                            continue
                        
                        stats["received"] += 1
                        
                        # Prepare message for buffer
                        msg = {
                            "channel": CHANNEL,
                            "payload": payload,
                            "hash": sha1_hash(payload),
                            "event_ts": parse_event_ts(payload)
                        }
                        message_buffer.append(msg)
                        
                        # Flush if batch size reached
                        if len(message_buffer) >= BATCH_SIZE:
                            count = flush_buffer(conn, message_buffer)
                            if count > 0:
                                stats["inserted"] += count
                                print(f"[FLUSH] Batch: Inserted {count} records. Total: {stats['inserted']}")
                            message_buffer = []
                        
                        # Progress indicator
                        if stats["received"] % 100 == 0:
                            print(f"[STATS] Received: {stats['received']}, Inserted: {stats['inserted']}, Errors: {stats['errors']}")
                    
                    except json.JSONDecodeError as e:
                        print(f"[WARN] Invalid JSON: {e}")
                        stats["errors"] += 1
        
        except websockets.ConnectionClosed as e:
            print(f"[WARN] Connection closed: {e}. Reconnecting in {reconnect_delay}s...")
        except Exception as e:
            print(f"[ERROR] WebSocket error: {e}. Reconnecting in {reconnect_delay}s...")
            stats["errors"] += 1
        
        if not shutdown_requested:
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)


async def main():
    """Main entry point."""
    global message_buffer
    
    print("=" * 60)
    print("Coinbase Streaming Pipeline - Bronze Ingestion")
    print("=" * 60)
    print(f"Products: {PRODUCTS}")
    print(f"Batch size: {BATCH_SIZE}")
    print(f"Flush interval: {FLUSH_INTERVAL}s")
    print(f"Database: {DB_NAME}@{DB_HOST}:{DB_PORT}")
    print("=" * 60)
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Connect to database
    try:
        conn = get_db_connection()
        conn.autocommit = False
        print("[INFO] Database connected successfully")
    except Exception as e:
        print(f"[FATAL] Could not connect to database: {e}")
        sys.exit(1)
    
    try:
        # Run stream and timer concurrently
        await asyncio.gather(
            stream_trades(conn),
            flush_timer(conn)
        )
    finally:
        # Final flush before exit
        if message_buffer:
            count = flush_buffer(conn, message_buffer)
            stats["inserted"] += count
            print(f"[FLUSH] Final: Inserted {count} records")
        
        conn.close()
        print("\n" + "=" * 60)
        print("Final Statistics:")
        print(f"  Received: {stats['received']}")
        print(f"  Inserted: {stats['inserted']}")
        print(f"  Errors: {stats['errors']}")
        print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
