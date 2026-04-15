export default function DeviceCard({ device, onView }) {
  return (
    <div className="device-card">
      <div className="card-header">
        <div className="card-title">{device.device_name}</div>
        <div className="card-type-badge">{device.device_type}</div>
      </div>

      <div className="card-meta">
        Source: sensor_1
      </div>

      <div className="card-location">📍 {device.location}</div>

      <div className="card-value-box">
        <div className="card-value-label">Real-time Value</div>
        <div className="card-value">{device.latest_value}</div>
        <div className="card-unit">{device.unit}</div>
      </div>

      <div className="card-status">
        <div className="status-dot"></div>
        Active
      </div>

      <div className="card-buttons">
        <button className="btn-card btn-view" onClick={onView}>
          👁️ View Details
        </button>
        <button className="btn-card btn-delete">
          🗑️ Delete
        </button>
      </div>
    </div>
  );
}
