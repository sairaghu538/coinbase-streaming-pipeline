# Coinbase Streaming Pipeline

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-blue.svg)](https://www.postgresql.org/)
[![Prefect](https://img.shields.io/badge/Prefect-2.x-blue.svg)](https://www.prefect.io/)
[![Streamlit](https://img.shields.io/badge/Streamlit-Live_Demo-ff4b4b.svg)](https://sairaghu538-coinbase-streaming-pipeline.streamlit.app)

A real-time data engineering pipeline that ingests streaming cryptocurrency trade data from Coinbase, implements a **Medallion Architecture** in PostgreSQL, and delivers analytics-ready datasets with **two dashboard options**: a premium React + FastAPI local dashboard and a Streamlit Cloud live demo.

## 🎯 Project Overview

This project demonstrates production-grade data engineering practices:

- **Streaming data ingestion** from Coinbase WebSocket API
- **Medallion Architecture** (Bronze → Silver → Gold layers)
- **Incremental, idempotent transformations**
- **Data quality checks and monitoring**
- **Pipeline orchestration** with Prefect
- **Full-stack dashboard** (React + FastAPI) for local visualization
- **Streamlit Cloud dashboard** for live online demo

## 📸 Live Dashboard

![Crypto Dashboard](docs/dashboard_screenshot.png)

> **[▶ View Live Demo](https://coinbase-streaming.streamlit.app)** — Real-time crypto prices, candlestick charts, and pipeline metrics

## 📊 Architecture

```mermaid
flowchart LR
    subgraph Sources
        WS[("Coinbase\nWebSocket")]
    end
    
    subgraph Ingestion
        PY["Python\n(async websockets)"]
    end
    
    subgraph PostgreSQL
        subgraph Bronze["🥉 Bronze Layer"]
            RAW[("Raw JSON\nTrades")]
        end
        
        subgraph Silver["🥈 Silver Layer"]
            CLEAN[("Typed &\nDeduped")]
            QUARANTINE[("Quarantine")]
        end
        
        subgraph Gold["🥇 Gold Layer"]
            OHLC[("OHLC\nCandles")]
            KPI[("Daily\nKPIs")]
            MOVERS[("Top\nMovers")]
        end
    end
    
    subgraph Orchestration
        PREFECT["Prefect\nFlows"]
        DQ["Data Quality\nChecks"]
    end
    
    subgraph Visualization
        API["FastAPI\nREST API"]
        REACT["React\nDashboard"]
        STREAMLIT["Streamlit\nCloud"]
    end
    
    WS --> PY
    PY --> RAW
    RAW --> CLEAN
    RAW --> QUARANTINE
    CLEAN --> OHLC
    CLEAN --> KPI
    CLEAN --> MOVERS
    PREFECT --> RAW
    PREFECT --> CLEAN
    PREFECT --> OHLC
    DQ --> CLEAN
    OHLC --> API
    KPI --> API
    API --> REACT
    OHLC --> STREAMLIT
    KPI --> STREAMLIT
```

## 🗂 Project Structure

```
coinbase-streaming-pipeline/
├── api/                               # FastAPI REST backend
│   ├── main.py                        # App entry point (CORS, routing)
│   ├── database.py                    # PostgreSQL connection
│   └── routes.py                      # API endpoints (OHLC, KPIs)
├── dashboard/                         # React (Vite) frontend
│   ├── src/
│   │   ├── App.jsx                    # Main dashboard layout
│   │   ├── index.css                  # Glassmorphism dark theme
│   │   └── components/
│   │       ├── CandlestickChart.jsx   # TradingView-style chart
│   │       ├── PriceTicker.jsx        # Animated price cards
│   │       ├── KPICards.jsx           # VWAP, Volume, High/Low
│   │       ├── VolumeChart.jsx        # Volume bar chart
│   │       └── PipelineStatus.jsx     # Pipeline health
│   └── package.json
├── sql/
│   ├── 001_create_schemas.sql         # Bronze/Silver/Gold schemas
│   └── 002_create_tables.sql          # All table definitions
├── src/
│   ├── ingest/
│   │   └── coinbase_ws_to_bronze.py   # WebSocket streaming ingestion
│   ├── transform/
│   │   ├── bronze_to_silver.sql       # JSON → typed records
│   │   └── silver_to_gold.sql         # Aggregations & KPIs
│   ├── quality/
│   │   ├── dq_checks.sql              # DQ check definitions
│   │   └── dq_runner.py               # DQ execution & alerting
│   └── orchestration/
│       └── prefect_flow.py            # Pipeline orchestration
├── data/                              # CSV snapshots for Streamlit Cloud
├── app.py                             # Streamlit dashboard (live demo)
├── .env.example                       # Environment template
├── requirements.txt
└── README.md
```

## 🏗 Medallion Architecture

### 🥉 Bronze Layer (Raw)
- **Purpose**: Store raw data exactly as received
- **Pattern**: Append-only, immutable
- **Storage**: JSONB for schema flexibility
- **Table**: `bronze.coinbase_trades_raw`

### 🥈 Silver Layer (Clean)
- **Purpose**: Validated, typed, deduplicated data
- **Transformations**:
  - Parse JSON to typed columns
  - Deduplicate by `trade_id`
  - Validate constraints (side ∈ {buy, sell})
  - Quarantine invalid records
- **Table**: `silver.coinbase_trades`

### 🥇 Gold Layer (Business)
- **Purpose**: Analytics-ready aggregations
- **Tables**:
  - `gold.ohlc_1m` - 1-minute OHLC candles with VWAP
  - `gold.ohlc_1h` - 1-hour OHLC candles
  - `gold.daily_kpis` - Daily metrics (volume, VWAP, price change)
  - `gold.top_movers` - Price movement snapshots

## 🚀 Quick Start

### Prerequisites

- Python 3.10+
- PostgreSQL 15+ (native Windows install)
- Power BI Desktop (optional, for visualization)

### 1. Clone and Setup

```bash
git clone https://github.com/sairaghu538/coinbase-streaming-pipeline.git
cd coinbase-streaming-pipeline

# Create virtual environment
python -m venv venv
venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Database

Create `.env` file from template:

```bash
copy .env.example .env
```

Edit `.env` with your PostgreSQL credentials:

```env
PGHOST=localhost
PGPORT=5432
PGDATABASE=crypto_dw
PGUSER=postgres
PGPASSWORD=your_password
```

### 3. Create Database & Tables

Using pgAdmin or psql:

```sql
-- Create database
CREATE DATABASE crypto_dw;

-- Connect to crypto_dw and run:
-- sql/001_create_schemas.sql
-- sql/002_create_tables.sql
```

### 4. Start Ingestion

```bash
python src/ingest/coinbase_ws_to_bronze.py
```

You should see:
```
============================================================
Coinbase Streaming Pipeline - Bronze Ingestion
============================================================
Products: ['BTC-USD', 'ETH-USD']
[INFO] Connected and subscribed to: ['BTC-USD', 'ETH-USD']
[FLUSH] Batch: Inserted 100 records. Total: 100
```

### 5. Run Transformations

In a separate terminal:

```bash
# Run pipeline manually
python src/orchestration/prefect_flow.py

# Or use Prefect server (optional)
prefect server start  # Terminal 1
python src/orchestration/prefect_flow.py  # Terminal 2
```

### 6. Launch Dashboards

**Option A: React Dashboard (Premium UI)**

```bash
# Terminal 1: FastAPI backend
python -m uvicorn api.main:app --reload --port 8000

# Terminal 2: React frontend
cd dashboard && npm install && npm run dev
```

Open http://localhost:5173 for the full TradingView-style dashboard.

**Option B: Streamlit Dashboard**

```bash
streamlit run app.py
```

Open http://localhost:8501 for the Streamlit version.

**Live Demo**: [Streamlit Cloud](https://sairaghu538-coinbase-streaming-pipeline.streamlit.app)

## 📈 Key Metrics & KPIs

| Metric | Description | Table |
|--------|-------------|-------|
| OHLC | Open, High, Low, Close prices | `gold.ohlc_1m`, `gold.ohlc_1h` |
| VWAP | Volume-weighted average price | `gold.ohlc_*`, `gold.daily_kpis` |
| Volume | Total traded volume | All gold tables |
| Trade Count | Number of trades | All gold tables |
| Price Change % | Daily percentage change | `gold.daily_kpis` |

## 🔍 Data Quality Checks

| Check | Type | Description |
|-------|------|-------------|
| `bronze_freshness` | Freshness | Last ingest within 10 minutes |
| `silver_freshness` | Freshness | Last trade within 15 minutes |
| `bronze_volume_5m` | Volume | Records exist in last 5 minutes |
| `silver_no_null_price` | Null | No null prices in Silver |
| `silver_no_duplicates` | Duplicate | No duplicate trade_ids |
| `silver_no_negative_price` | Range | No negative prices |

## 🛠 Tech Stack

| Component | Technology | Why |
|-----------|------------|-----|
| **Ingestion** | Python + websockets | Async, non-blocking I/O |
| **Storage** | PostgreSQL | JSONB, Window functions, Production-grade |
| **Transforms** | SQL | Simple, maintainable, version-controlled |
| **Orchestration** | Prefect | Local-first, retries, logging |
| **API Layer** | FastAPI | High-performance Python REST API |
| **Frontend** | React + Vite + TradingView | Premium candlestick charts, glassmorphism UI |
| **Live Demo** | Streamlit + Plotly | Free cloud deployment, interactive charts |

## 📋 Design Decisions

### Why Coinbase?
- Free public WebSocket API (no API key)
- Real streaming data with real-world challenges:
  - High volume
  - Schema evolution
  - Duplicate events
  - Late arrivals

### Why PostgreSQL over Snowflake/Databricks?
- Runs locally (free)
- JSONB handles semi-structured data
- Window functions for OHLC calculations
- Same SQL skills transfer to cloud platforms

### Why Medallion Architecture?
- Clear separation of concerns
- Raw data preserved for reprocessing
- Incremental, idempotent loads
- Industry standard pattern

### Why Prefect over Airflow?
- Lighter weight for local development
- Python-native (no DAG files)
- Easy local server
- Same concepts transfer to Airflow

## 🔮 Future Enhancements

- [x] Full-stack React + FastAPI dashboard
- [x] Streamlit Cloud live demo
- [ ] Add REST API ingestion for product metadata
- [ ] Implement late-arriving data handling
- [ ] Add Slack alerting for DQ failures
- [ ] Deploy to cloud (Azure/GCP)
- [ ] Add dbt for transformation management

## 📄 License

MIT License - feel free to use this for learning and portfolio purposes.

---

**Built as a portfolio project demonstrating real-world data engineering skills.**
**Status: ✅ COMPLETED (Feb 2026)**
