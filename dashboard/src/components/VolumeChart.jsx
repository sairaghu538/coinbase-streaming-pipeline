/**
 * VolumeChart - Custom volume bars with green/red gradient fills
 */
export default function VolumeChart({ data }) {
    if (!data || data.length === 0) {
        return (
            <div className="volume-section glass-card">
                <div className="chart-header">
                    <div className="chart-title">Volume</div>
                </div>
                <div className="loading">
                    <div className="spinner"></div>
                    Loading volume data...
                </div>
            </div>
        );
    }

    // Take last 60 candles for volume display
    const volumeData = data.slice(-60);
    const maxVolume = Math.max(...volumeData.map(d => d.volume || 0), 0.001);

    return (
        <div className="volume-section glass-card">
            <div className="chart-header">
                <div>
                    <div className="chart-title">Volume</div>
                    <div className="chart-subtitle">Last 60 candles</div>
                </div>
            </div>
            <div className="volume-bars">
                {volumeData.map((d, i) => {
                    const height = ((d.volume || 0) / maxVolume) * 100;
                    const isGreen = d.close >= d.open;
                    return (
                        <div
                            key={i}
                            className={`volume-bar ${isGreen ? 'green' : 'red'}`}
                            style={{ height: `${Math.max(height, 2)}%` }}
                            title={`Vol: ${(d.volume || 0).toFixed(4)}`}
                        />
                    );
                })}
            </div>
        </div>
    );
}
