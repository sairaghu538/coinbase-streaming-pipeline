"""
Crypto Dashboard - Streamlit Version
=====================================
Real-time visualization of the Coinbase Streaming Pipeline.
Reads from CSV data snapshots (for cloud deployment)
or live PostgreSQL (for local development).

Deploy: https://share.streamlit.io
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os

# ─────────────────────────────────────────
# Page Config
# ─────────────────────────────────────────
st.set_page_config(
    page_title="Crypto Dashboard | Coinbase Pipeline",
    page_icon="C",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─────────────────────────────────────────
# Custom CSS for Premium Look
# ─────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

    /* Hide Streamlit defaults */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}

    /* Global */
    .stApp {
        font-family: 'Inter', sans-serif;
    }

    /* Metric cards */
    [data-testid="stMetric"] {
        background: rgba(15, 15, 42, 0.6);
        backdrop-filter: blur(20px);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 16px;
        padding: 16px 20px;
        transition: all 0.3s ease;
    }
    [data-testid="stMetric"]:hover {
        border-color: rgba(255, 255, 255, 0.15);
        transform: translateY(-2px);
    }
    [data-testid="stMetricLabel"] {
        font-size: 0.75rem !important;
        text-transform: uppercase;
        letter-spacing: 0.8px;
        color: #8888aa !important;
    }
    [data-testid="stMetricValue"] {
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 1.4rem !important;
        font-weight: 600 !important;
    }
    [data-testid="stMetricDelta"] {
        font-family: 'JetBrains Mono', monospace !important;
    }

    /* Header */
    .dashboard-header {
        display: flex;
        align-items: center;
        gap: 14px;
        padding-bottom: 20px;
        border-bottom: 1px solid rgba(255, 255, 255, 0.08);
        margin-bottom: 24px;
    }
    .logo {
        width: 44px;
        height: 44px;
        background: linear-gradient(135deg, #4f8cff, #8b5cf6);
        border-radius: 12px;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 22px;
        font-weight: 800;
        color: white;
        box-shadow: 0 4px 16px rgba(79, 140, 255, 0.3);
    }
    .header-title {
        font-size: 1.5rem;
        font-weight: 700;
        background: linear-gradient(135deg, #e8e8f0, #4f8cff);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin: 0;
    }
    .header-subtitle {
        font-size: 0.8rem;
        color: #8888aa;
        margin: 0;
    }
    .status-badge {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 6px 14px;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 500;
        background: rgba(0, 230, 118, 0.1);
        color: #00e676;
        border: 1px solid rgba(0, 230, 118, 0.2);
    }
    .status-dot {
        width: 7px;
        height: 7px;
        background: #00e676;
        border-radius: 50%;
        display: inline-block;
        animation: pulse 2s ease-in-out infinite;
    }
    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.4; }
    }

    /* Glass card for charts */
    .glass-card {
        background: rgba(15, 15, 42, 0.6);
        backdrop-filter: blur(20px);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 16px;
        padding: 24px;
        margin-bottom: 16px;
    }

    /* Pipeline stats */
    .pipeline-stat {
        text-align: center;
        padding: 16px;
        background: rgba(255, 255, 255, 0.03);
        border-radius: 8px;
        border: 1px solid rgba(255, 255, 255, 0.08);
    }
    .pipeline-value {
        font-family: 'JetBrains Mono', monospace;
        font-size: 1.3rem;
        font-weight: 600;
        color: #06d6a0;
    }
    .pipeline-label {
        font-size: 0.7rem;
        color: #8888aa;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-top: 4px;
    }

    /* Section titles */
    .section-title {
        font-size: 1rem;
        font-weight: 600;
        color: #e8e8f0;
        margin-bottom: 4px;
    }
    .section-subtitle {
        font-size: 0.75rem;
        color: #8888aa;
        margin-bottom: 16px;
    }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────
# Data Loading
# ─────────────────────────────────────────
@st.cache_data(ttl=300)
def load_data():
    """Load data from CSV files."""
    data_dir = os.path.join(os.path.dirname(__file__), "data")

    ohlc_1m = pd.read_csv(os.path.join(data_dir, "ohlc_1m.csv"), parse_dates=["bucket_1m"])
    ohlc_1h = pd.read_csv(os.path.join(data_dir, "ohlc_1h.csv"), parse_dates=["bucket_1h"])
    daily_kpis = pd.read_csv(os.path.join(data_dir, "daily_kpis.csv"), parse_dates=["day"])
    pipeline_status = pd.read_csv(os.path.join(data_dir, "pipeline_status.csv"))

    return ohlc_1m, ohlc_1h, daily_kpis, pipeline_status


ohlc_1m, ohlc_1h, daily_kpis, pipeline_status = load_data()


# ─────────────────────────────────────────
# Header
# ─────────────────────────────────────────
st.markdown("""
<div class="dashboard-header">
    <div class="logo">C</div>
    <div>
        <p class="header-title">Crypto Dashboard</p>
        <p class="header-subtitle">Real-time Coinbase Streaming Pipeline</p>
    </div>
    <div style="margin-left: auto;">
        <span class="status-badge">
            <span class="status-dot"></span>
            Pipeline Active
        </span>
    </div>
</div>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────
# Product Selector
# ─────────────────────────────────────────
products = sorted(ohlc_1m["product_id"].unique())
selected_product = st.pills("Select Product", products, default=products[0] if products else "BTC-USD")

if not selected_product:
    selected_product = products[0] if products else "BTC-USD"


# ─────────────────────────────────────────
# Price Ticker Row
# ─────────────────────────────────────────
ticker_cols = st.columns(len(products))
for i, product in enumerate(products):
    with ticker_cols[i]:
        kpi = daily_kpis[daily_kpis["product_id"] == product]
        if not kpi.empty:
            latest = kpi.iloc[0]
            price = float(latest["close"])
            change = float(latest["price_change_pct"]) if pd.notna(latest["price_change_pct"]) else 0
            st.metric(
                label=product,
                value=f"${price:,.2f}",
                delta=f"{change:+.2f}%",
            )
        else:
            st.metric(label=product, value="--")


# ─────────────────────────────────────────
# Candlestick Chart
# ─────────────────────────────────────────
product_ohlc = ohlc_1m[ohlc_1m["product_id"] == selected_product].copy()

if not product_ohlc.empty:
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.75, 0.25],
        subplot_titles=None,
    )

    # Candlestick
    fig.add_trace(
        go.Candlestick(
            x=product_ohlc["bucket_1m"],
            open=product_ohlc["open"],
            high=product_ohlc["high"],
            low=product_ohlc["low"],
            close=product_ohlc["close"],
            increasing_line_color="#00e676",
            decreasing_line_color="#ff5252",
            increasing_fillcolor="#00e676",
            decreasing_fillcolor="#ff5252",
            name="OHLC",
        ),
        row=1, col=1,
    )

    # Volume bars
    colors = ["#00e676" if c >= o else "#ff5252"
              for c, o in zip(product_ohlc["close"], product_ohlc["open"])]

    fig.add_trace(
        go.Bar(
            x=product_ohlc["bucket_1m"],
            y=product_ohlc["volume"],
            marker_color=colors,
            opacity=0.5,
            name="Volume",
        ),
        row=2, col=1,
    )

    fig.update_layout(
        height=500,
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", color="#8888aa"),
        xaxis_rangeslider_visible=False,
        showlegend=False,
        margin=dict(l=0, r=0, t=30, b=0),
        xaxis2=dict(gridcolor="rgba(255,255,255,0.04)"),
        yaxis=dict(gridcolor="rgba(255,255,255,0.04)", title="Price ($)"),
        yaxis2=dict(gridcolor="rgba(255,255,255,0.04)", title="Volume"),
    )

    st.markdown(f'<p class="section-title">{selected_product} Price Chart</p>', unsafe_allow_html=True)
    st.markdown('<p class="section-subtitle">1-Minute OHLC Candles with Volume</p>', unsafe_allow_html=True)
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
else:
    st.info(f"No OHLC data for {selected_product}")


# ─────────────────────────────────────────
# KPI Cards Row
# ─────────────────────────────────────────
kpi_data = daily_kpis[daily_kpis["product_id"] == selected_product]

if not kpi_data.empty:
    latest_kpi = kpi_data.iloc[0]

    st.markdown('<p class="section-title">Key Metrics</p>', unsafe_allow_html=True)
    st.markdown(f'<p class="section-subtitle">{selected_product} Daily Summary</p>', unsafe_allow_html=True)

    k1, k2, k3, k4, k5 = st.columns(5)

    with k1:
        vwap = float(latest_kpi["vwap"])
        st.metric("VWAP", f"${vwap:,.2f}")

    with k2:
        vol = float(latest_kpi["volume"])
        st.metric("24h Volume", f"{vol:,.4f}")

    with k3:
        high = float(latest_kpi["high"])
        st.metric("High", f"${high:,.2f}")

    with k4:
        low = float(latest_kpi["low"])
        st.metric("Low", f"${low:,.2f}")

    with k5:
        trades = int(latest_kpi["trades"])
        st.metric("Trades", f"{trades:,}")


# ─────────────────────────────────────────
# Pipeline Status
# ─────────────────────────────────────────
if not pipeline_status.empty:
    st.markdown("---")
    st.markdown('<p class="section-title">Pipeline Status</p>', unsafe_allow_html=True)
    st.markdown('<p class="section-subtitle">Record counts per data layer</p>', unsafe_allow_html=True)

    ps = pipeline_status.iloc[0]
    p1, p2, p3, p4 = st.columns(4)

    with p1:
        st.markdown(f"""
        <div class="pipeline-stat">
            <div class="pipeline-value" style="color: #ffd700;">{int(ps['bronze_records']):,}</div>
            <div class="pipeline-label">Bronze Records</div>
        </div>
        """, unsafe_allow_html=True)

    with p2:
        st.markdown(f"""
        <div class="pipeline-stat">
            <div class="pipeline-value" style="color: #e8e8f0;">{int(ps['silver_records']):,}</div>
            <div class="pipeline-label">Silver Records</div>
        </div>
        """, unsafe_allow_html=True)

    with p3:
        st.markdown(f"""
        <div class="pipeline-stat">
            <div class="pipeline-value">{int(ps['gold_1m_records']):,}</div>
            <div class="pipeline-label">1m Candles</div>
        </div>
        """, unsafe_allow_html=True)

    with p4:
        st.markdown(f"""
        <div class="pipeline-stat">
            <div class="pipeline-value" style="color: #4f8cff;">{int(ps['gold_1h_records']):,}</div>
            <div class="pipeline-label">1h Candles</div>
        </div>
        """, unsafe_allow_html=True)


# ─────────────────────────────────────────
# Footer
# ─────────────────────────────────────────
st.markdown("---")
st.markdown(
    '<p style="text-align: center; color: #555577; font-size: 0.75rem;">'
    'Built with Python, PostgreSQL & Streamlit | '
    '<a href="https://github.com/sairaghu538/coinbase-streaming-pipeline" '
    'style="color: #4f8cff; text-decoration: none;">GitHub Repo</a>'
    '</p>',
    unsafe_allow_html=True,
)
