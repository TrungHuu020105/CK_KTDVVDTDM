import { useState, useEffect, useRef } from 'react'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'
import { AlertCircle, Lock } from 'lucide-react'
import api from '../api'
import { checkMetricAlert } from '../utils/alertUtils'
import { saveAlert } from '../utils/alertService'

const MS_IN_SECOND = 1000

const parseSensorTimestampMs = (rawTimestamp) => {
  if (!rawTimestamp) return null
  if (rawTimestamp instanceof Date) return rawTimestamp.getTime()

  const raw = String(rawTimestamp).trim()
  if (!raw) return null

  const withDateTimeSeparator = raw.includes('T') ? raw : raw.replace(' ', 'T')
  const hasZone = /Z$|[+-]\d{2}:\d{2}$/.test(withDateTimeSeparator)
  const normalized = hasZone ? withDateTimeSeparator : `${withDateTimeSeparator}Z`
  const parsed = Date.parse(normalized)

  return Number.isNaN(parsed) ? null : parsed
}

const formatLatency = (ms) => {
  if (ms === null || ms === undefined) return '--'
  if (ms < MS_IN_SECOND) return `${Math.round(ms)} ms`
  return `${(ms / MS_IN_SECOND).toFixed(2)} s`
}

const FAST_POLL_MS = 1000
const HISTORY_POLL_MS = 4000
const HISTORY_MINUTES = 30

export default function IoTMetrics() {
  const [metricType, setMetricType] = useState('temperature')
  const [data, setData] = useState([])
  const [current, setCurrent] = useState(0)
  const [stats, setStats] = useState({ avg: 0, max: 0, min: 0 })
  const [latestInsight, setLatestInsight] = useState(null)
  const [latencyStats, setLatencyStats] = useState({
    latestMs: null,
    avgMs: null,
    sampleTimestamp: null,
    measuredAt: null,
  })
  const [alert, setAlert] = useState(null)
  const [loading, setLoading] = useState(true)
  const [accessDenied, setAccessDenied] = useState(false)
  const lastAlertRef = useRef(null)
  const wsRef = useRef(null)
  const realtimeDataRef = useRef({}) // Store realtime data from WebSocket

  const iotMetrics = {
    temperature: { label: '🌡️ Temperature', unit: '°C', color: '#ff6b6b', min: 15, max: 35 },
    humidity: { label: '💧 Humidity', unit: '%', color: '#4ecdc4', min: 30, max: 90 },
    soil_moisture: { label: '🌱 Soil Moisture', unit: '%', color: '#95e1d3', min: 0, max: 100 },
    light_intensity: { label: '💡 Light Intensity', unit: 'lux', color: '#ffe66d', min: 0, max: 1000 },
    pressure: { label: '🌊 Pressure', unit: 'hPa', color: '#a8dadc', min: 900, max: 1100 },
  }

  const applyAlertForValue = (latestValue) => {
    const newAlert = checkMetricAlert(metricType, latestValue)
    setAlert(newAlert)

    if (newAlert.status === 'normal') return

    const now = Date.now()
    const lastAlertTime = lastAlertRef.current?.timestamp
    const timeSinceLastAlert = lastAlertTime ? now - lastAlertTime : Infinity

    if (!lastAlertRef.current || lastAlertRef.current.status !== newAlert.status || timeSinceLastAlert > 5 * 60 * 1000) {
      const thresholdMap = {
        temperature: { warning: 30, critical: 35 },
        humidity: { warning: 75, critical: 90 },
        soil_moisture: { warning: 30, critical: 20 },
        light_intensity: { warning: 500, critical: 100 },
        pressure: { warning: 1020, critical: 1050 }
      }
      const threshold = thresholdMap[metricType][newAlert.status]

      saveAlert(metricType, newAlert.status, latestValue, threshold, newAlert.fullMessage)
      lastAlertRef.current = { status: newAlert.status, timestamp: now }
    }
  }

  const fetchLatestMetricFast = async () => {
    try {
      const latestRes = await api.get(`/api/databricks/latest?metric_type=${metricType}`)
      const rows = latestRes.data?.data || []
      if (rows.length === 0) return

      const latestRow = rows[0]
      const latestValue = Number(latestRow.value)
      if (!Number.isNaN(latestValue)) {
        setCurrent(latestValue)
        applyAlertForValue(latestValue)
      }

      const sensorTsMs = parseSensorTimestampMs(latestRow.timestamp)
      if (sensorTsMs !== null) {
        const frontendReceivedAtMs = Date.now()
        const latestLatencyMs = Math.max(0, frontendReceivedAtMs - sensorTsMs)
        setLatencyStats(prev => ({
          ...prev,
          latestMs: latestLatencyMs,
          sampleTimestamp: new Date(sensorTsMs).toISOString(),
          measuredAt: new Date(frontendReceivedAtMs).toISOString(),
        }))
      }
    } catch (error) {
      console.debug('[IoTMetrics] Fast latest poll failed:', error?.message || error)
    }
  }

  const fetchData = async () => {
    try {
      setAccessDenied(false)
      const [historyRes] = await Promise.all([
        api.get(`/api/metrics/history?metric_type=${metricType}&minutes=${HISTORY_MINUTES}`)
      ])
      let metrics = historyRes.data.data || []
      
      // Combine with realtime data from WebSocket
      if (realtimeDataRef.current[metricType]) {
        const realtimeMetrics = realtimeDataRef.current[metricType].map(d => ({
          value: d.value,
          timestamp: d.timestamp.toISOString()
        }))
        // Add realtime data that's not already in history
        metrics = [...metrics, ...realtimeMetrics]
        // Remove duplicates and sort by timestamp
        const uniqueMetrics = {}
        metrics.forEach(m => {
          const key = `${m.value}_${m.timestamp}`
          uniqueMetrics[key] = m
        })
        metrics = Object.values(uniqueMetrics).sort((a, b) => 
          new Date(a.timestamp) - new Date(b.timestamp)
        )
      }
      
      // Set current to latest value
      const frontendReceivedAtMs = Date.now()
      const validLatencySamples = []
      let latestDataTimestampMs = null

      metrics.forEach(metric => {
        const sensorTsMs = parseSensorTimestampMs(metric.timestamp)
        if (sensorTsMs === null) return

        const latencyMs = frontendReceivedAtMs - sensorTsMs
        if (latencyMs < 0) return

        validLatencySamples.push(latencyMs)
        if (latestDataTimestampMs === null || sensorTsMs > latestDataTimestampMs) {
          latestDataTimestampMs = sensorTsMs
        }
      })

      if (validLatencySamples.length > 0 && latestDataTimestampMs !== null) {
        const latestLatencyMs = frontendReceivedAtMs - latestDataTimestampMs
        const avgLatencyMs = validLatencySamples.reduce((sum, value) => sum + value, 0) / validLatencySamples.length
        setLatencyStats({
          latestMs: latestLatencyMs,
          avgMs: avgLatencyMs,
          sampleTimestamp: new Date(latestDataTimestampMs).toISOString(),
          measuredAt: new Date(frontendReceivedAtMs).toISOString(),
        })
      } else {
        setLatencyStats({ latestMs: null, avgMs: null, sampleTimestamp: null, measuredAt: null })
      }

      if (metrics.length > 0) {
        const latestValue = metrics[metrics.length - 1].value
        applyAlertForValue(latestValue)
      }

      const grouped = {}
      metrics.forEach(metric => {
        const time = new Date(metric.timestamp).toLocaleTimeString('vi-VN', {
          hour: '2-digit',
          minute: '2-digit'
        })
        if (!grouped[time]) {
          grouped[time] = []
        }
        grouped[time].push(metric.value)
      })

      const chartData = Object.entries(grouped).map(([time, values]) => ({
        time,
        value: (values.reduce((a, b) => a + b, 0) / values.length).toFixed(2)
      }))

      const allValues = metrics.map(m => m.value)
      if (allValues.length > 0) {
        setStats({
          avg: (allValues.reduce((a, b) => a + b, 0) / allValues.length).toFixed(2),
          max: Math.max(...allValues).toFixed(2),
          min: Math.min(...allValues).toFixed(2)
        })
      }

      setData(chartData)
      setLoading(false)
    } catch (error) {
      if (error.response?.status === 403) {
        setAccessDenied(true)
      }
      console.error('Failed to fetch IoT metrics:', error)
      setData([])
      setLoading(false)
    }
  }

  useEffect(() => {
    setLoading(true)
    fetchLatestMetricFast()
    fetchData()
  }, [metricType])

  useEffect(() => {
    const fastLatestInterval = setInterval(fetchLatestMetricFast, FAST_POLL_MS)
    const historyInterval = setInterval(fetchData, HISTORY_POLL_MS)
    return () => {
      clearInterval(fastLatestInterval)
      clearInterval(historyInterval)
    }
  }, [metricType])

  // Connect to WebSocket for realtime IoT data
  useEffect(() => {
    const connectWebSocket = () => {
      try {
        const serverUrl = import.meta.env.VITE_SERVER_IP || 'localhost'
        const serverPort = import.meta.env.VITE_SERVER_PORT || '8000'
        const clientId = `frontend_metrics_${Date.now()}`
        const wsUrl = `ws://${serverUrl}:${serverPort}/api/ws/${clientId}`
        
        console.log('[IoTMetrics] Connecting to WebSocket:', wsUrl)
        wsRef.current = new WebSocket(wsUrl)
        
        wsRef.current.onopen = () => {
          console.log('[IoTMetrics] WebSocket connected')
        }
        
        wsRef.current.onmessage = (event) => {
          try {
            const message = JSON.parse(event.data)
            console.log('[IoTMetrics] Received WebSocket message:', message)
            
            // Handle realtime IoT metric broadcasts
            if ((message.type === 'iot_metric' || message.type === 'iot_metric_realtime') && message.metric_type && message.value !== undefined) {
              // Store realtime data by metric_type
              if (!realtimeDataRef.current[message.metric_type]) {
                realtimeDataRef.current[message.metric_type] = []
              }
              realtimeDataRef.current[message.metric_type].push({
                value: message.value,
                timestamp: new Date(message.timestamp || Date.now())
              })
              
              // Keep only last 120 minutes of data
              const twoHoursMs = 120 * 60 * 1000
              realtimeDataRef.current[message.metric_type] = realtimeDataRef.current[message.metric_type].filter(
                d => Date.now() - d.timestamp.getTime() < twoHoursMs
              )
              
              // Update current value if it matches current metric type
              if (message.metric_type === metricType) {
                setCurrent(message.value)
                console.log(`[IoTMetrics] Updated ${metricType} to ${message.value}`)
              }
            }

            if (message.type === 'insight_event') {
              const messageMetricType = String(message.metric_type || '').toLowerCase()
              if (!messageMetricType || messageMetricType === metricType) {
                setLatestInsight({
                  metric_type: message.metric_type || metricType,
                  severity: message.severity || 'info',
                  message: message.message || 'Insight detected',
                  value: message.value,
                  threshold: message.threshold,
                  timestamp: message.timestamp || new Date().toISOString(),
                  score: message.score
                })
              }
            }
          } catch (err) {
            console.error('[IoTMetrics] Failed to parse WebSocket data:', err)
          }
        }
        
        wsRef.current.onerror = (error) => {
          console.error('[IoTMetrics] WebSocket error:', error)
        }
        
        wsRef.current.onclose = () => {
          console.log('[IoTMetrics] WebSocket disconnected, reconnecting...')
          setTimeout(connectWebSocket, 3000)
        }
      } catch (err) {
        console.error('[IoTMetrics] Failed to connect WebSocket:', err)
      }
    }
    
    connectWebSocket()
    
    return () => {
      if (wsRef.current) {
        wsRef.current.close()
      }
    }
  }, [metricType])

  const currentMetric = iotMetrics[metricType]

  return (
    <div className="p-6 space-y-6">
      <div>
        <h1 className="text-3xl font-bold text-white">📡 IoT Sensor Metrics</h1>
        <p className="text-gray-400 mt-2">Real-time IoT sensor data monitoring</p>
      </div>

      {/* Access Denied Message */}
      {accessDenied && (
        <div className="card-border p-6 bg-red-500/10 border-red-500/30 flex items-center gap-4">
          <Lock className="w-6 h-6 text-red-400" />
          <div>
            <p className="text-red-400 font-semibold">No Access</p>
            <p className="text-red-300 text-sm mt-1">You don't have access to any IoT metrics. Please contact your administrator to request device access.</p>
          </div>
        </div>
      )}

      {/* Metric Type Selector */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
        {Object.entries(iotMetrics).map(([key, metric]) => (
          <button
            key={key}
            onClick={() => setMetricType(key)}
            className={`p-4 rounded-lg border transition-all ${
              metricType === key
                ? 'border-white bg-dark-700'
                : 'border-gray-600 hover:border-gray-400 bg-dark-800'
            }`}
          >
            <p className="text-sm">{metric.label}</p>
            <p className="text-xs text-gray-400 mt-1">{metric.unit}</p>
          </button>
        ))}
      </div>

      {/* Current Status */}
      <div 
        className="card-border p-6 bg-dark-700 border-2"
        style={{ borderColor: alert?.color }}
      >
        <div className="flex items-start justify-between">
          <div>
            <p className="text-gray-400 text-sm mb-2">CURRENT STATUS</p>
            <div className="text-5xl font-bold neon-glow" style={{ color: alert?.color }}>
              {current}{currentMetric.unit}
            </div>
            <p className="text-gray-500 text-sm mt-2">Live {currentMetric.label}</p>
          </div>
          {alert && alert.status !== 'normal' && (
            <div className="text-right">
              <span
                className="px-3 py-1 rounded text-xs font-semibold text-dark-900"
                style={{ backgroundColor: alert.color }}
              >
                {alert.message}
              </span>
              <p className="text-xs mt-2 text-gray-500 max-w-xs">{alert.fullMessage}</p>
            </div>
          )}
        </div>
      </div>

      {latestInsight && (
        <div
          className="card-border p-5"
          style={{
            backgroundColor:
              latestInsight.severity === 'critical'
                ? 'rgba(239, 68, 68, 0.15)'
                : latestInsight.severity === 'warning'
                  ? 'rgba(234, 179, 8, 0.15)'
                  : 'rgba(59, 130, 246, 0.15)',
            borderColor:
              latestInsight.severity === 'critical'
                ? '#ef4444'
                : latestInsight.severity === 'warning'
                  ? '#eab308'
                  : '#3b82f6'
          }}
        >
          <div className="flex items-center justify-between gap-4">
            <div>
              <p className="text-sm font-semibold text-white">Analytics Insight ({latestInsight.severity?.toUpperCase()})</p>
              <p className="text-sm text-gray-200 mt-1">{latestInsight.message}</p>
              <p className="text-xs text-gray-400 mt-2">
                Metric: {latestInsight.metric_type}
                {latestInsight.value !== undefined && latestInsight.value !== null ? ` | Value: ${latestInsight.value}` : ''}
                {latestInsight.threshold !== undefined && latestInsight.threshold !== null ? ` | Threshold: ${latestInsight.threshold}` : ''}
              </p>
            </div>
            <div className="text-right text-xs text-gray-300">
              {latestInsight.score !== undefined && latestInsight.score !== null && (
                <p>Score: {latestInsight.score}</p>
              )}
              <p>{new Date(latestInsight.timestamp).toLocaleTimeString('vi-VN')}</p>
            </div>
          </div>
        </div>
      )}

      <div className="card-border p-6 bg-dark-800">
        <h3 className="text-gray-300 font-semibold mb-3">⏱️ End-to-End Latency (Sensor → Frontend)</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div className="bg-dark-900 p-4 rounded">
            <p className="text-gray-400 text-sm">Latest Total Delay</p>
            <p className="text-2xl font-bold text-neon-cyan mt-2">{formatLatency(latencyStats.latestMs)}</p>
          </div>
          <div className="bg-dark-900 p-4 rounded">
            <p className="text-gray-400 text-sm">Average Delay (Current Window)</p>
            <p className="text-2xl font-bold text-neon-yellow mt-2">{formatLatency(latencyStats.avgMs)}</p>
          </div>
        </div>
        <div className="mt-3 text-xs text-gray-500 space-y-1">
          <p>Sensor latest timestamp: {latencyStats.sampleTimestamp || '--'}</p>
          <p>Frontend measured at: {latencyStats.measuredAt || '--'}</p>
        </div>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-3 gap-4">
        <div className="card-border p-4 bg-dark-800">
          <p className="text-gray-400 text-sm">Average</p>
          <p className="text-2xl font-bold mt-2" style={{ color: alert?.color }}>
            {stats.avg}{currentMetric.unit}
          </p>
        </div>
        <div className="card-border p-4 bg-dark-800">
          <p className="text-gray-400 text-sm">Peak</p>
          <p className="text-2xl font-bold text-neon-yellow mt-2">{stats.max}{currentMetric.unit}</p>
        </div>
        <div className="card-border p-4 bg-dark-800">
          <p className="text-gray-400 text-sm">Low</p>
          <p className="text-2xl font-bold text-neon-green mt-2">{stats.min}{currentMetric.unit}</p>
        </div>
      </div>

      {/* Chart */}
      <div className="card-border p-6 bg-dark-800">
        <h3 className="text-gray-300 font-semibold mb-4">{currentMetric.label} Timeline</h3>
        {loading ? (
          <p className="text-gray-400 py-12 text-center">Loading...</p>
        ) : data.length > 0 ? (
          <ResponsiveContainer width="100%" height={350}>
            <LineChart data={data} margin={{ top: 5, right: 30, left: 0, bottom: 5 }}>
              <defs>
                <linearGradient id="iotGradient" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor={currentMetric.color} stopOpacity={0.3}/>
                  <stop offset="95%" stopColor={currentMetric.color} stopOpacity={0}/>
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" vertical={false} />
              <XAxis dataKey="time" stroke="#666" style={{ fontSize: '12px' }} />
              <YAxis stroke="#666" style={{ fontSize: '12px' }} />
              <Tooltip
                contentStyle={{
                  backgroundColor: '#1a1f3a',
                  border: `1px solid ${currentMetric.color}`,
                  borderRadius: '8px',
                  fontSize: '12px'
                }}
                cursor={{ stroke: currentMetric.color, strokeWidth: 2 }}
              />
              <Legend wrapperStyle={{ fontSize: '12px' }} />
              <Line
                type="monotone"
                dataKey="value"
                stroke={currentMetric.color}
                dot={false}
                strokeWidth={2.5}
                isAnimationActive={false}
                fill="url(#iotGradient)"
                name={`${currentMetric.label} (${currentMetric.unit})`}
              />
            </LineChart>
          </ResponsiveContainer>
        ) : (
          <p className="text-gray-400 py-12 text-center">
            No data available. Generate IoT data with: <code className="bg-dark-700 px-2 py-1 rounded text-xs">python generate_iot_data.py</code>
          </p>
        )}
      </div>

      {/* Reference Ranges */}
      <div className="card-border p-6 bg-dark-800">
        <h3 className="text-gray-300 font-semibold mb-4">📊 Sensor Reference Ranges</h3>
        <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
          {Object.entries(iotMetrics).map(([key, metric]) => (
            <div key={key} className="bg-dark-900 p-3 rounded">
              <p className="text-sm text-gray-400">{metric.label}</p>
              <p className="text-xs text-gray-500 mt-2">Range: {metric.min}-{metric.max} {metric.unit}</p>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
