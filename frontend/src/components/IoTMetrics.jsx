import { useState, useEffect, useRef } from 'react'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'
import { Lock } from 'lucide-react'
import api from '../api'
import { formatVNTime } from '../utils/vnTime'

export default function IoTMetrics() {
  const [metricType, setMetricType] = useState('temperature')
  const [data, setData] = useState([])
  const [current, setCurrent] = useState(0)
  const [stats, setStats] = useState({ avg: 0, max: 0, min: 0 })
  const [loading, setLoading] = useState(true)
  const [accessDenied, setAccessDenied] = useState(false)
  const wsRef = useRef(null)
  const realtimeDataRef = useRef({}) // Store realtime data from WebSocket
  const getMetricValue = (m) => m?.metric_value ?? m?.value
  const getMetricTimestamp = (m) => m?.event_ts ?? m?.timestamp

  const iotMetrics = {
    temperature: { label: '🌡️ Temperature', unit: '°C', color: '#ff6b6b' },
    humidity: { label: '💧 Humidity', unit: '%', color: '#4ecdc4' },
    soil_moisture: { label: '🌱 Soil Moisture', unit: '%', color: '#95e1d3' },
    light_intensity: { label: '💡 Light Intensity', unit: 'lux', color: '#ffe66d' },
    pressure: { label: '🌊 Pressure', unit: 'hPa', color: '#a8dadc' },
  }

  const fetchData = async () => {
    try {
      setAccessDenied(false)
      const [historyRes] = await Promise.all([
        api.get(`/api/metrics/history?metric_type=${metricType}&minutes=120`)
      ])
      let metrics = historyRes.data.data || []
      
      // Combine with realtime data from WebSocket
      if (realtimeDataRef.current[metricType]) {
        const realtimeMetrics = realtimeDataRef.current[metricType].map(d => ({
          metric_value: d.value,
          event_ts: d.timestamp.toISOString()
        }))
        // Add realtime data that's not already in history
        metrics = [...metrics, ...realtimeMetrics]
        // Remove duplicates and sort by timestamp
        const uniqueMetrics = {}
        metrics.forEach(m => {
          const key = `${getMetricValue(m)}_${getMetricTimestamp(m)}`
          uniqueMetrics[key] = m
        })
        metrics = Object.values(uniqueMetrics).sort((a, b) => 
          new Date(getMetricTimestamp(a)) - new Date(getMetricTimestamp(b))
        )
      }
      
      // Set current to latest value
      if (metrics.length > 0) {
        const latestValue = getMetricValue(metrics[metrics.length - 1])
        setCurrent(latestValue)
      }

      const grouped = {}
      metrics.forEach(metric => {
        const time = formatVNTime(getMetricTimestamp(metric))
        if (!grouped[time]) {
          grouped[time] = []
        }
        grouped[time].push(getMetricValue(metric))
      })

      const chartData = Object.entries(grouped).map(([time, values]) => ({
        time,
        value: (values.reduce((a, b) => a + b, 0) / values.length).toFixed(2)
      }))

      const allValues = metrics.map(m => getMetricValue(m)).filter(v => v !== undefined && v !== null)
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
    fetchData()
  }, [metricType])

  useEffect(() => {
    const interval = setInterval(fetchData, 3000)
    return () => clearInterval(interval)
  }, [metricType])

  // Connect to WebSocket for realtime IoT data
  useEffect(() => {
    const connectWebSocket = () => {
      try {
        const useSameOriginApi = String(import.meta.env.VITE_USE_SAME_ORIGIN_API || 'false').toLowerCase() === 'true'
        const coreServerUrl = import.meta.env.VITE_CORE_SERVER_IP || import.meta.env.VITE_SERVER_IP || 'localhost'
        const corePort = import.meta.env.VITE_CORE_SERVER_PORT || import.meta.env.VITE_SERVER_PORT || '8000'
        const clientId = `frontend_metrics_${Date.now()}`
        const token = localStorage.getItem('access_token') || ''
        const wsProtocol = window.location.protocol === 'https:' ? 'wss' : 'ws'
        const wsHost = useSameOriginApi ? window.location.host : `${coreServerUrl}:${corePort}`
        const wsUrl = `${wsProtocol}://${wsHost}/api/ws/${clientId}?token=${encodeURIComponent(token)}`
        
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
            if (message.type === 'iot_metric' && message.metric_type && message.value !== undefined) {
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
        style={{ borderColor: currentMetric.color }}
      >
        <div className="flex items-start justify-between">
          <div>
            <p className="text-gray-400 text-sm mb-2">CURRENT STATUS</p>
            <div className="text-5xl font-bold neon-glow" style={{ color: currentMetric.color }}>
              {current}{currentMetric.unit}
            </div>
            <p className="text-gray-500 text-sm mt-2">Live {currentMetric.label}</p>
          </div>
        </div>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-3 gap-4">
        <div className="card-border p-4 bg-dark-800">
          <p className="text-gray-400 text-sm">Average</p>
          <p className="text-2xl font-bold mt-2" style={{ color: currentMetric.color }}>
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
