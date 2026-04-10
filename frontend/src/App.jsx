import { useState, useEffect } from 'react';
import './App.css';
import DeviceCard from './components/DeviceCard';
import DeviceModal from './components/DeviceModal';
import { fetcher } from './api/api';

export default function App() {
  const [devices, setDevices] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [selectedDevice, setSelectedDevice] = useState(null);

  useEffect(() => {
    loadDevices();
  }, []);

  const loadDevices = async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await fetcher('/api/devices');
      setDevices(data.data || []);
    } catch (err) {
      setError(err.message);
      console.error('Error loading devices:', err);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div>
      {/* Header */}
      <header className="header">
        <div className="header-content">
          <div>
            <h1 className="header-title">📱 IoT Devices</h1>
            <p className="header-subtitle">Manage your IoT sensors and devices</p>
          </div>
          <div className="header-actions">
            <button className="btn-primary" onClick={loadDevices}>
              🔄 Refresh
            </button>
            <button className="btn-secondary">➕ Add Device</button>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <div className="container">
        {error && (
          <div className="error">
            ❌ {error}
          </div>
        )}

        {loading && (
          <div className="loading">
            ⏳ Loading devices...
          </div>
        )}

        {!loading && devices.length === 0 && !error && (
          <div className="error">
            ❌ No devices found. Make sure the streaming pipeline is running.
          </div>
        )}

        {!loading && devices.length > 0 && (
          <div className="device-grid">
            {devices.map((device) => (
              <DeviceCard
                key={device.device_id}
                device={device}
                onView={() => setSelectedDevice(device.device_id)}
              />
            ))}
          </div>
        )}
      </div>

      {/* Modal */}
      {selectedDevice && (
        <DeviceModal
          deviceId={selectedDevice}
          devices={devices}
          onClose={() => setSelectedDevice(null)}
        />
      )}
    </div>
  );
}
