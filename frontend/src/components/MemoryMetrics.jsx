import { useState, useEffect, useRef } from 'react'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'
import { AlertCircle, Lock } from 'lucide-react'
import api from '../api'
import { checkMetricAlert } from '../utils/alertUtils'
import { saveAlert } from '../utils/alertService'
import { formatVNTime } from '../utils/vnTime'

export default function MemoryMetrics() {
  const [data, setData] = useState([])
  const [current, setCurrent] = useState(0)
  const [stats, setStats] = useState({ avg: 0, max: 0, min: 0 })
  const [alert, setAlert] = useState(null)
  const [loading, setLoading] = useState(true)
  const [accessDenied, setAccessDenied] = useState(false)
  const lastAlertRef = useRef(null)

  const fetchData = async () => {
    try {
      setAccessDenied(false)
      const [historyRes, latestRes] = await Promise.all([
        api.get('/api/metrics/history?metric_type=memory&minutes=60'),
        api.get('/api/metrics/latest')
      ])
      
      const metrics = historyRes.data.data || []
      const memoryValue = latestRes.data?.latest_memory || 0
      setCurrent(memoryValue)
      
      // Check for alerts
      const newAlert = checkMetricAlert('memory', memoryValue)
      setAlert(newAlert)

      // Save alert if status is not normal and hasn't been saved recently
      if (newAlert.status !== 'normal') {
        const now = Date.now()
        const lastAlertTime = lastAlertRef.current?.timestamp
        const timeSinceLastAlert = lastAlertTime ? now - lastAlertTime : Infinity

        // Save alert if status changed or 5 minutes have passed
        if (!lastAlertRef.current || lastAlertRef.current.status !== newAlert.status || timeSinceLastAlert > 5 * 60 * 1000) {
          const thresholdMap = { warning: 75, critical: 85 }
          const threshold = thresholdMap[newAlert.status]
          
          saveAlert('memory', newAlert.status, memoryValue, threshold, newAlert.fullMessage)
          lastAlertRef.current = { status: newAlert.status, timestamp: now }
        }
      }

      const grouped = {}
      metrics.forEach(metric => {
        const time = formatVNTime(metric.timestamp)
        if (!grouped[time]) {
          grouped[time] = []
        }
        grouped[time].push(metric.value)
      })

      const chartData = Object.entries(grouped).map(([time, values]) => ({
        time,
        memory: (values.reduce((a, b) => a + b, 0) / values.length).toFixed(2)
      }))

      const allValues = metrics.map(m => m.value)
      setStats({
        avg: (allValues.reduce((a, b) => a + b, 0) / allValues.length || 0).toFixed(2),
        max: (allValues.length > 0 ? Math.max(...allValues) : 0).toFixed(2),
        min: (allValues.length > 0 ? Math.min(...allValues) : 0).toFixed(2)
      })

      setData(chartData)
    } catch (error) {
      if (error.response?.status === 403) {
        setAccessDenied(true)
      }
      console.error('Failed to fetch memory metrics:', error)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchData()
    const interval = setInterval(fetchData, 2000)
    return () => clearInterval(interval)
  }, [])

  return (
    <div className="p-6 space-y-6">
      <div>
        <h1 className="text-3xl font-bold text-white">Memory Metrics</h1>
        <p className="text-gray-400 mt-2">Memory usage over the last 60 minutes</p>
      </div>

      {/* Access Denied Message */}
      {accessDenied && (
        <div className="card-border p-6 bg-red-500/10 border-red-500/30 flex items-center gap-4">
          <Lock className="w-6 h-6 text-red-400" />
          <div>
            <p className="text-red-400 font-semibold">No Access</p>
            <p className="text-red-300 text-sm mt-1">You don't have access to any memory metrics. Please contact your administrator to request device access.</p>
          </div>
        </div>
      )}

      {/* Current Status */}
      <div 
        className="card-border p-6 bg-dark-700 border-2"
        style={{ borderColor: alert?.color }}
      >
        <div className="flex items-start justify-between">
          <div>
            <p className="text-gray-400 text-sm mb-2">CURRENT STATUS</p>
            <div className="text-5xl font-bold neon-glow" style={{ color: alert?.color }}>
              {current}%
            </div>
            <p className="text-gray-500 text-sm mt-2">Live Memory Usage</p>
          </div>
          {alert && alert.status !== 'normal' && (
            <div className="text-right">
              <span
                className="px-3 py-1 rounded text-xs font-semibold text-dark-900"
                style={{ backgroundColor: alert.color }}
              >
                {alert.message}
              </span>
              <p className="text-xs mt-2 text-gray-500">{alert.fullMessage}</p>
            </div>
          )}
        </div>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-3 gap-4">
        <div className="card-border p-4 bg-dark-800">
          <p className="text-gray-400 text-sm">Average</p>
          <p className="text-2xl font-bold text-neon-purple mt-2">{stats.avg}%</p>
        </div>
        <div className="card-border p-4 bg-dark-800">
          <p className="text-gray-400 text-sm">Peak</p>
          <p className="text-2xl font-bold text-neon-yellow mt-2">{stats.max}%</p>
        </div>
        <div className="card-border p-4 bg-dark-800">
          <p className="text-gray-400 text-sm">Low</p>
          <p className="text-2xl font-bold text-neon-green mt-2">{stats.min}%</p>
        </div>
      </div>

      {/* Chart */}
      <div className="card-border p-6 bg-dark-800">
        <h3 className="text-gray-300 font-semibold mb-4">Memory Usage Timeline</h3>
        {loading ? (
          <p className="text-gray-400 py-12 text-center">Loading...</p>
        ) : data.length > 0 ? (
          <ResponsiveContainer width="100%" height={350}>
            <LineChart data={data} margin={{ top: 5, right: 30, left: 0, bottom: 5 }}>
              <defs>
                <linearGradient id="memoryGradient" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#c400ff" stopOpacity={0.3}/>
                  <stop offset="95%" stopColor="#c400ff" stopOpacity={0}/>
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(196,0,255,0.1)" vertical={false} />
              <XAxis dataKey="time" stroke="#666" style={{ fontSize: '12px' }} />
              <YAxis stroke="#666" style={{ fontSize: '12px' }} domain={[0, 100]} />
              <Tooltip
                contentStyle={{
                  backgroundColor: '#1a1f3a',
                  border: '1px solid #c400ff',
                  borderRadius: '8px',
                  fontSize: '12px'
                }}
                cursor={{ stroke: '#c400ff', strokeWidth: 2 }}
              />
              <Legend wrapperStyle={{ fontSize: '12px' }} />
              <Line
                type="monotone"
                dataKey="memory"
                stroke="#c400ff"
                dot={false}
                strokeWidth={2.5}
                isAnimationActive={false}
                fill="url(#memoryGradient)"
                name="Memory Usage (%)"
              />
            </LineChart>
          </ResponsiveContainer>
        ) : (
          <p className="text-gray-400 py-12 text-center">No data available</p>
        )}
      </div>
    </div>
  )
}
