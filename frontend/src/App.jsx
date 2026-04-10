import { useState, useEffect } from 'react';
import './App.css';
import DeviceCard from './components/DeviceCard';
import DeviceModal from './components/DeviceModal';
import { fetcher } from './api/api';

export default function App() {
  const [devices, setDevices] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [selectedDevice, setSelectedDevice] = useState(null);
  const [connectionStatus, setConnectionStatus] = useState('connecting');

  const loadDevices = async () => {
    try {
      const data = await fetcher('/api/devices');
      setDevices(data.data || []);
      setError(null);
      setConnectionStatus('rest-api');
    } catch (err) {
      setError(err.message);
      console.error('Error loading devices:', err);
      setConnectionStatus('error');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    let ws = null;
    let reconnectTimeout = null;
    let attemptCount = 0;

    const connectWebSocket = () => {
      try {
        const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        ws = new WebSocket(`${wsProtocol}//localhost:8000/ws/devices`);

        ws.onopen = () => {
          console.log('✅ WebSocket connected');
          setConnectionStatus('websocket');
          setError(null);
          attemptCount = 0;
          setLoading(false);
        };

        ws.onmessage = (event) => {
          try {
            const message = JSON.parse(event.data);
            if (message.success && message.data) {
              setDevices(message.data);
              setLoading(false);
              setError(null);
            } else if (!message.success) {
              console.warn('Server error:', message.error);
              setError(message.error);
            }
          } catch (err) {
            console.error('WebSocket message error:', err);
          }
        };

        ws.onerror = (error) => {
          console.error('WebSocket error:', error);
          setConnectionStatus('error');
        };

        ws.onclose = () => {
          console.log('WebSocket disconnected - attempting to reconnect in 3s...');
          setConnectionStatus('disconnected');
          
          // Attempt to reconnect
          attemptCount++;
          if (attemptCount <= 5) {
            reconnectTimeout = setTimeout(() => {
              console.log(`🔄 Reconnect attempt ${attemptCount}/5`);
              connectWebSocket();
            }, 3000);
          } else {
            // After 5 failed attempts, fallback to REST API
            console.log('❌ WebSocket failed, using REST API fallback');
            setConnectionStatus('fallback');
            loadDevices();
          }
        };
      } catch (err) {
        console.error('WebSocket connection error:', err);
        setConnectionStatus('error');
        loadDevices(); // Fallback to REST API
      }
    };

    setLoading(true);
    connectWebSocket();

    return () => {
      if (reconnectTimeout) clearTimeout(reconnectTimeout);
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.close();
      }
    };
  }, []);

  return (
    <div>
      {/* Header */}
      <header className="header">
        <div className="header-content">
          <div>
            <h1 className="header-title">📱 IoT Devices</h1>
            <p className="header-subtitle">
              Manage your IoT sensors and devices
              {connectionStatus === 'websocket' && ' • 🟢 Realtime (WebSocket)'}
              {connectionStatus === 'rest-api' && ' • 🔵 REST API Polling'}
              {connectionStatus === 'fallback' && ' • 🟡 Fallback Mode'}
              {connectionStatus === 'connecting' && ' • ⏳ Connecting...'}
              {connectionStatus === 'error' && ' • 🔴 Connection Error'}
            </p>
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
