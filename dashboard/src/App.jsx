import { useState, useEffect, useCallback } from 'react';
import CandlestickChart from './components/CandlestickChart';
import PriceTicker from './components/PriceTicker';
import KPICards from './components/KPICards';
import VolumeChart from './components/VolumeChart';
import PipelineStatus from './components/PipelineStatus';
import './index.css';

const API_BASE = 'http://localhost:8000/api';
const REFRESH_INTERVAL = 30000; // 30 seconds

function App() {
  const [products, setProducts] = useState([]);
  const [selectedProduct, setSelectedProduct] = useState('BTC-USD');
  const [ohlcData, setOhlcData] = useState([]);
  const [kpis, setKpis] = useState([]);
  const [pipelineStatus, setPipelineStatus] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [lastUpdate, setLastUpdate] = useState(null);

  // Fetch helper
  const fetchApi = useCallback(async (endpoint) => {
    try {
      const res = await fetch(`${API_BASE}${endpoint}`);
      if (!res.ok) throw new Error(`API error: ${res.status}`);
      return await res.json();
    } catch (err) {
      console.error(`Failed to fetch ${endpoint}:`, err);
      throw err;
    }
  }, []);

  // Load products once
  useEffect(() => {
    fetchApi('/products')
      .then(data => {
        if (data && data.length > 0) {
          setProducts(data);
          setSelectedProduct(data[0]);
        }
      })
      .catch(() => setError('Cannot connect to API. Is FastAPI running on port 8000?'));
  }, [fetchApi]);

  // Load data for selected product
  const loadData = useCallback(async () => {
    try {
      const [ohlc, kpiData, status] = await Promise.all([
        fetchApi(`/ohlc?product_id=${selectedProduct}&interval=1m&limit=200`),
        fetchApi(`/kpis?product_id=${selectedProduct}`),
        fetchApi('/status'),
      ]);

      setOhlcData(ohlc || []);
      setKpis(kpiData || []);
      setPipelineStatus(status);
      setLastUpdate(new Date());
      setLoading(false);
      setError(null);
    } catch (err) {
      setError('Cannot connect to API. Is FastAPI running on port 8000?');
      setLoading(false);
    }
  }, [selectedProduct, fetchApi]);

  // Initial load + auto-refresh
  useEffect(() => {
    setLoading(true);
    loadData();
    const timer = setInterval(loadData, REFRESH_INTERVAL);
    return () => clearInterval(timer);
  }, [loadData]);

  // Get current KPI for the selected product
  const currentKpi = kpis.length > 0 ? kpis[0] : null;

  return (
    <div className="app">
      {/* Header */}
      <header className="header">
        <div className="header-left">
          <div className="logo">C</div>
          <div>
            <h1>Crypto Dashboard</h1>
            <div className="header-subtitle">
              Real-time Coinbase Streaming Pipeline
            </div>
          </div>
        </div>
        <div className="header-right">
          <div className="status-badge">
            <span className="status-dot"></span>
            {lastUpdate ? `Updated ${lastUpdate.toLocaleTimeString()}` : 'Connecting...'}
          </div>
        </div>
      </header>

      {/* Error State */}
      {error && (
        <div className="glass-card" style={{ padding: '24px', marginBottom: '20px' }}>
          <div className="error-msg">
            <p style={{ fontSize: '1.1rem', marginBottom: '8px' }}>Unable to connect to API</p>
            <p>Make sure FastAPI is running:</p>
            <code style={{
              display: 'block',
              marginTop: '8px',
              padding: '12px',
              background: 'rgba(255,255,255,0.05)',
              borderRadius: '8px',
              color: 'var(--accent-cyan)',
              fontFamily: 'JetBrains Mono, monospace',
            }}>
              python -m uvicorn api.main:app --reload --port 8000
            </code>
          </div>
        </div>
      )}

      {/* Product Selector */}
      <div className="product-selector" style={{ marginBottom: '20px' }}>
        {products.map(p => (
          <button
            key={p}
            className={`product-pill ${p === selectedProduct ? 'active' : ''}`}
            onClick={() => setSelectedProduct(p)}
          >
            {p}
          </button>
        ))}
      </div>

      {/* Price Ticker Row */}
      <div className="ticker-row" style={{ marginBottom: '20px' }}>
        {(products.length > 0 ? products : ['BTC-USD', 'ETH-USD', 'SOL-USD']).map(p => (
          <PriceTicker
            key={p}
            product={p}
            kpi={kpis.find(k => k.product_id === p) || (p === selectedProduct ? currentKpi : null)}
          />
        ))}
      </div>

      {/* Main Grid: Chart + KPI Sidebar */}
      <div className="dashboard-grid">
        {/* Candlestick Chart */}
        <CandlestickChart data={ohlcData} product={selectedProduct} />

        {/* KPI Cards Sidebar */}
        <KPICards kpi={currentKpi} />

        {/* Volume Chart */}
        <VolumeChart data={ohlcData} />

        {/* Pipeline Status */}
        <PipelineStatus status={pipelineStatus} />
      </div>
    </div>
  );
}

export default App;
