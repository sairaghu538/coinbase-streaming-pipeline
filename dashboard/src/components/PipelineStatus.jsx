/**
 * PipelineStatus - Shows record counts per layer
 */
export default function PipelineStatus({ status }) {
    if (!status) return null;

    const stats = [
        { label: 'Bronze Records', value: status.bronze_records?.toLocaleString() || '0', color: 'var(--accent-gold)' },
        { label: 'Silver Records', value: status.silver_records?.toLocaleString() || '0', color: 'var(--text-primary)' },
        { label: '1m Candles', value: status.gold_1m_records?.toLocaleString() || '0', color: 'var(--accent-cyan)' },
        { label: '1h Candles', value: status.gold_1h_records?.toLocaleString() || '0', color: 'var(--accent-blue)' },
    ];

    return (
        <div className="pipeline-section glass-card">
            <div className="chart-header">
                <div>
                    <div className="chart-title">Pipeline Status</div>
                    <div className="chart-subtitle">
                        Last ingest: {status.latest_ingest
                            ? new Date(status.latest_ingest).toLocaleString()
                            : 'N/A'}
                    </div>
                </div>
            </div>
            <div className="pipeline-grid">
                {stats.map(s => (
                    <div key={s.label} className="pipeline-stat">
                        <div className="pipeline-stat-value" style={{ color: s.color }}>
                            {s.value}
                        </div>
                        <div className="pipeline-stat-label">{s.label}</div>
                    </div>
                ))}
            </div>
        </div>
    );
}
