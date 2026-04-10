import { useState, useEffect } from 'react';
import { fetcher } from '../api/api';
import Chart from './Chart';

export default function DeviceModal({ deviceId, devices, onClose }) {
  const [stats, setStats] = useState(null);
  const [timeseries, setTimeseries] = useState([]);
  const [loading, setLoading] = useState(false);

  const device = devices.find(d => d.device_id === deviceId);

  useEffect(() => {
    loadDeviceData();
  }, [deviceId]);

  const loadDeviceData = async () => {
    setLoading(true);
    try {
      const [statsData, seriesData] = await Promise.all([
        fetcher(`/api/device/${deviceId}/stats`),
        fetcher(`/api/device/${deviceId}/timeseries`)
      ]);
      setStats(statsData.data);
      setTimeseries(seriesData.data || []);
    } catch (err) {
      console.error('Error loading device data:', err);
    } finally {
      setLoading(false);
    }
  };

  if (!device) return null;

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-content" onClick={(e) => e.stopPropagation()}>
        {/* Modal Header */}
        <div className="modal-header">
          <h2 className="modal-title">{device.device_name} - Detailed Statistics</h2>
          <button className="btn-close" onClick={onClose}>✕</button>
        </div>

        {loading ? (
          <div className="loading">⏳ Loading data...</div>
        ) : (
          <>
            {/* Current Value */}
            <div className="modal-section">
              <h3 className="modal-section-title">📊 Current Value</h3>
              <div className="card-value-box" style={{ marginTop: '10px' }}>
                <div className="card-value-label">Real-time Value</div>
                <div className="card-value">{device.latest_value}</div>
                <div className="card-unit">{device.unit}</div>
              </div>
            </div>

            {/* Statistics */}
            {stats && (
              <div className="modal-section">
                <h3 className="modal-section-title">📈 Statistics (Last 2 Hours)</h3>
                <div className="stats-row">
                  <div className="stat-box">
                    <div className="stat-label">📊 Average</div>
                    <div className="stat-value">{stats.avg_value || 'N/A'}</div>
                  </div>
                  <div className="stat-box">
                    <div className="stat-label">📉 Minimum</div>
                    <div className="stat-value">{stats.min_value || 'N/A'}</div>
                  </div>
                  <div className="stat-box">
                    <div className="stat-label">📈 Maximum</div>
                    <div className="stat-value">{stats.max_value || 'N/A'}</div>
                  </div>
                </div>
              </div>
            )}

            {/* Chart */}
            <div className="modal-section">
              <h3 className="modal-section-title">📉 Trend Chart (Last 2 Hours)</h3>
              {timeseries.length > 0 ? (
                <Chart data={timeseries} unit={device.unit} />
              ) : (
                <div className="error">No data available for chart</div>
              )}
            </div>
          </>
        )}
      </div>
    </div>
  );
}
