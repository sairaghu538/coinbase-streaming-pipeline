/**
 * KPICards - Glassmorphism stat cards for the sidebar
 */
export default function KPICards({ kpi }) {
    if (!kpi) {
        return (
            <div className="kpi-sidebar">
                {['VWAP', 'Volume', 'High', 'Low', 'Trades'].map(label => (
                    <div key={label} className="glass-card kpi-card">
                        <div className="kpi-label">{label}</div>
                        <div className="kpi-value" style={{ color: 'var(--text-muted)' }}>--</div>
                    </div>
                ))}
            </div>
        );
    }

    const fmt = (val) => {
        if (val == null || isNaN(val)) return '--';
        return Number(val).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    };

    const cards = [
        {
            label: 'VWAP',
            value: `$${fmt(kpi.vwap)}`,
            color: 'var(--accent-blue)',
        },
        {
            label: '24h Volume',
            value: fmt(kpi.volume),
            color: 'var(--accent-purple)',
        },
        {
            label: 'High',
            value: `$${fmt(kpi.high_price)}`,
            color: 'var(--green)',
        },
        {
            label: 'Low',
            value: `$${fmt(kpi.low_price)}`,
            color: 'var(--red)',
        },
        {
            label: 'Total Trades',
            value: kpi.trades != null ? Number(kpi.trades).toLocaleString('en-US') : '--',
            color: 'var(--accent-cyan)',
        },
    ];

    return (
        <div className="kpi-sidebar">
            {cards.map(card => (
                <div key={card.label} className="glass-card kpi-card">
                    <div className="kpi-label">{card.label}</div>
                    <div className="kpi-value" style={{ color: card.color }}>
                        {card.value}
                    </div>
                </div>
            ))}
        </div>
    );
}
