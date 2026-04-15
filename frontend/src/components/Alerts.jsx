import { useState, useEffect } from 'react'
import { AlertTriangle, AlertCircle, CheckCircle } from 'lucide-react'
import api from '../api'

export default function Alerts() {
  const [alerts, setAlerts] = useState([])
  const [lastUpdated, setLastUpdated] = useState(null)

  // Format metric name for display
  const formatMetricName = (metric) => {
    const names = {
      temperature: 'Temperature',
      humidity: 'Humidity',
      soil_moisture: 'Soil Moisture',
      light_intensity: 'Light Intensity',
      pressure: 'Pressure',
    }
    return names[metric] || metric
  }

  // Format metric unit
  const getMetricUnit = (metric) => {
    const units = {
      temperature: '°C',
      humidity: '%',
      soil_moisture: '%',
      light_intensity: 'lux',
      pressure: 'hPa',
    }
    return units[metric] || ''
  }

  // Fetch alerts from database
  const fetchAlerts = async () => {
    try {
      const response = await api.get('/api/alerts/recent?hours=24&limit=100')
      setAlerts(response.data?.alerts || [])
      setLastUpdated(new Date())
    } catch (error) {
      console.error('Error fetching alerts:', error)
    }
  }

  // Fetch alerts on mount and set up polling (2 seconds for live alerts)
  useEffect(() => {
    fetchAlerts()
    const interval = setInterval(fetchAlerts, 2000)
    return () => clearInterval(interval)
  }, [])

  const getAlertIcon = (status) => {
    switch (status) {
      case 'critical':
        return <AlertTriangle className="w-6 h-6 flex-shrink-0" />
      case 'warning':
        return <AlertCircle className="w-6 h-6 flex-shrink-0" />
      default:
        return <CheckCircle className="w-6 h-6 flex-shrink-0" />
    }
  }

  const getAlertColor = (status) => {
    switch (status) {
      case 'critical':
        return 'text-neon-red'
      case 'warning':
        return 'text-neon-yellow'
      default:
        return 'text-neon-green'
    }
  }

  const getStatusColor = (status) => {
    switch (status) {
      case 'critical':
        return '#ff3333'
      case 'warning':
        return '#ffcc00'
      default:
        return '#00ff88'
    }
  }

  return (
    <div className="p-6 space-y-6">
      <div>
        <h1 className="text-3xl font-bold text-white">Alerts & Anomalies</h1>
        <p className="text-gray-400 mt-2">
          System notifications and warnings
          {lastUpdated && (
            <span className="ml-4 text-xs text-gray-500">
              Last updated: {lastUpdated.toLocaleTimeString('vi-VN')}
            </span>
          )}
        </p>
      </div>

      {/* Alert Dashboard */}
      <div className="grid grid-cols-3 gap-4">
        <div className="card-border p-4 bg-dark-800">
          <p className="text-gray-400 text-sm">Critical Alerts</p>
          <p className="text-2xl font-bold text-neon-red">
            {alerts.filter(a => a.status === 'critical').length}
          </p>
        </div>
        <div className="card-border p-4 bg-dark-800">
          <p className="text-gray-400 text-sm">Warning Alerts</p>
          <p className="text-2xl font-bold text-neon-yellow">
            {alerts.filter(a => a.status === 'warning').length}
          </p>
        </div>
        <div className="card-border p-4 bg-dark-800">
          <p className="text-gray-400 text-sm">Total Alerts (24h)</p>
          <p className="text-2xl font-bold text-neon-cyan">
            {alerts.length}
          </p>
        </div>
      </div>

      {/* Active Alerts */}
      {alerts.length === 0 ? (
        <div className="card-border p-6 bg-dark-800">
          <div className="flex items-center gap-4">
            <CheckCircle className="w-8 h-8 text-neon-green flex-shrink-0" />
            <div>
              <h3 className="text-white font-semibold">All Systems Healthy</h3>
              <p className="text-gray-400 text-sm mt-1">
                No alerts in the last 24 hours
              </p>
            </div>
          </div>
        </div>
      ) : (
        <div className="space-y-4">
          {alerts.map((alert) => (
            <div
              key={alert.id}
              className="card-border card-hover p-6 bg-dark-800 flex items-start gap-4 border border-opacity-50"
              style={{ borderColor: getStatusColor(alert.status) }}
            >
              <div className={`${getAlertColor(alert.status)}`}>
                {getAlertIcon(alert.status)}
              </div>
              <div className="flex-1">
                <div className="flex items-center justify-between">
                  <h3 className="text-white font-semibold">{formatMetricName(alert.metric_type)}</h3>
                  <span
                    className="px-3 py-1 rounded text-xs font-semibold text-dark-900"
                    style={{ backgroundColor: getStatusColor(alert.status) }}
                  >
                    {alert.status.toUpperCase()}
                  </span>
                </div>
                <p className="text-gray-400 text-sm mt-2">{alert.message}</p>
                <div className="flex items-center gap-4 mt-3">
                  <div className="flex items-center gap-2">
                    <p className="text-sm text-gray-500">Current:</p>
                    <p className="text-lg font-bold text-white">
                      {alert.current_value}
                      <span className="text-xs text-gray-400 ml-1">{getMetricUnit(alert.metric_type)}</span>
                    </p>
                  </div>
                  <div className="flex items-center gap-2">
                    <p className="text-sm text-gray-500">Threshold:</p>
                    <p className="text-lg font-bold text-neon-yellow">
                      {alert.threshold}
                      <span className="text-xs text-gray-400 ml-1">{getMetricUnit(alert.metric_type)}</span>
                    </p>
                  </div>
                  <p className="text-xs text-gray-500 ml-auto">{new Date(alert.created_at).toLocaleString('vi-VN')}</p>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Alert Information */}
      <div className="card-border p-6 bg-dark-800">
        <h3 className="text-white font-semibold mb-4">Alert Thresholds</h3>
        <div className="grid grid-cols-1 gap-4 text-sm text-gray-400">
          <div>
            <p className="font-semibold text-white mb-2">IoT Sensors</p>
            <ul className="space-y-1">
              <li>🟡 <strong>Temperature:</strong> Warn 30°C, Crit 35°C</li>
              <li>🟡 <strong>Humidity:</strong> Warn &lt;30% or &gt;85%</li>
              <li>🟡 <strong>Soil Moisture:</strong> Warn &lt;20% or &gt;90%</li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  )
}
