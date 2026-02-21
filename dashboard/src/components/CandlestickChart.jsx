import { useEffect, useRef } from 'react';
import { createChart, ColorType, CrosshairMode } from 'lightweight-charts';

/**
 * CandlestickChart - TradingView-style OHLC chart
 * Uses TradingView's lightweight-charts library
 */
export default function CandlestickChart({ data, product }) {
    const chartContainerRef = useRef(null);
    const chartRef = useRef(null);

    useEffect(() => {
        if (!chartContainerRef.current || !data || data.length === 0) return;

        // Remove old chart
        if (chartRef.current) {
            chartRef.current.remove();
            chartRef.current = null;
        }

        const chart = createChart(chartContainerRef.current, {
            layout: {
                background: { type: ColorType.Solid, color: 'transparent' },
                textColor: '#8888aa',
                fontSize: 12,
                fontFamily: 'Inter, sans-serif',
            },
            grid: {
                vertLines: { color: 'rgba(255, 255, 255, 0.04)' },
                horzLines: { color: 'rgba(255, 255, 255, 0.04)' },
            },
            crosshair: {
                mode: CrosshairMode.Normal,
                vertLine: {
                    color: 'rgba(79, 140, 255, 0.3)',
                    labelBackgroundColor: '#4f8cff',
                },
                horzLine: {
                    color: 'rgba(79, 140, 255, 0.3)',
                    labelBackgroundColor: '#4f8cff',
                },
            },
            rightPriceScale: {
                borderColor: 'rgba(255, 255, 255, 0.08)',
            },
            timeScale: {
                borderColor: 'rgba(255, 255, 255, 0.08)',
                timeVisible: true,
                secondsVisible: false,
            },
            width: chartContainerRef.current.clientWidth,
            height: 350,
        });

        // Candlestick series
        const candleSeries = chart.addCandlestickSeries({
            upColor: '#00e676',
            downColor: '#ff5252',
            borderDownColor: '#ff5252',
            borderUpColor: '#00e676',
            wickDownColor: '#ff5252',
            wickUpColor: '#00e676',
        });

        candleSeries.setData(data);

        // Volume series as histogram at the bottom
        const volumeSeries = chart.addHistogramSeries({
            priceFormat: { type: 'volume' },
            priceScaleId: 'volume',
        });

        chart.priceScale('volume').applyOptions({
            scaleMargins: { top: 0.85, bottom: 0 },
        });

        const volumeData = data.map(d => ({
            time: d.time,
            value: d.volume || 0,
            color: d.close >= d.open
                ? 'rgba(0, 230, 118, 0.2)'
                : 'rgba(255, 82, 82, 0.2)',
        }));

        volumeSeries.setData(volumeData);

        // Fit content
        chart.timeScale().fitContent();

        chartRef.current = chart;

        // Resize handler
        const handleResize = () => {
            if (chartContainerRef.current && chartRef.current) {
                chartRef.current.applyOptions({
                    width: chartContainerRef.current.clientWidth,
                });
            }
        };

        window.addEventListener('resize', handleResize);
        return () => {
            window.removeEventListener('resize', handleResize);
            if (chartRef.current) {
                chartRef.current.remove();
                chartRef.current = null;
            }
        };
    }, [data, product]);

    return (
        <div className="chart-section glass-card">
            <div className="chart-header">
                <div>
                    <div className="chart-title">{product} Price Chart</div>
                    <div className="chart-subtitle">1-Minute OHLC Candles</div>
                </div>
            </div>
            <div className="chart-container" ref={chartContainerRef}>
                {(!data || data.length === 0) && (
                    <div className="loading">
                        <div className="spinner"></div>
                        Loading chart data...
                    </div>
                )}
            </div>
        </div>
    );
}
