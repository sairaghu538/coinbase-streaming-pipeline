import { useState, useEffect, useRef } from 'react';

/**
 * PriceTicker - Animated price card with green/red flash on change
 */
export default function PriceTicker({ product, kpi }) {
    const [flash, setFlash] = useState('');
    const prevPrice = useRef(null);

    useEffect(() => {
        if (!kpi) return;
        const currentPrice = kpi.close_price;

        if (prevPrice.current !== null && currentPrice !== prevPrice.current) {
            setFlash(currentPrice > prevPrice.current ? 'flash-green' : 'flash-red');
            setTimeout(() => setFlash(''), 1000);
        }

        prevPrice.current = currentPrice;
    }, [kpi]);

    if (!kpi) {
        return (
            <div className="glass-card kpi-card">
                <div className="kpi-label">{product}</div>
                <div className="kpi-value" style={{ color: 'var(--text-muted)' }}>--</div>
            </div>
        );
    }

    const priceChange = kpi.price_change_pct || 0;
    const isPositive = priceChange >= 0;

    return (
        <div className="glass-card kpi-card">
            <div className="kpi-label">{product}</div>
            <div className={`kpi-value ${flash}`}>
                ${Number(kpi.close_price).toLocaleString('en-US', {
                    minimumFractionDigits: 2,
                    maximumFractionDigits: 2,
                })}
            </div>
            <div className={`kpi-change ${isPositive ? 'positive' : 'negative'}`}>
                {isPositive ? '▲' : '▼'} {Math.abs(priceChange).toFixed(2)}%
            </div>
        </div>
    );
}
