import { useState, useEffect } from 'react'
import { RefreshCw, AlertTriangle, AlertCircle, CheckCircle } from 'lucide-react'
import GaugeChart from './GaugeChart'
import MetricCard from './MetricCard'
import api from '../api'
import { checkMetricAlert } from '../utils/alertUtils'
import { formatVNTime } from '../utils/vnTime'

export default function Dashboard() {
  const [data, setData] = useState({
    latest: {},
    summary: {},
    latestIoT: {},
    iotLatestValues: {
      temperature: 0,
      humidity: 0,
      soil_moisture: 0,
      light_intensity: 0,
      pressure: 0
    }
  })
  const [loading, setLoading] = useState(true)
  const [lastUpdate, setLastUpdate] = useState(new Date())
  const [hasData, setHasData] = useState(true)
  const [activeAlerts, setActiveAlerts] = useState([])

  const fetchAlerts = async (latestData) => {
    try {
      const alerts = []
      
      // Check CPU
      if (latestData.latest_cpu !== null && latestData.latest_cpu !== undefined) {
        const cpuAlert = checkMetricAlert('cpu', latestData.latest_cpu)
        if (cpuAlert.status !== 'normal') {
          alerts.push({ metric: 'CPU', ...cpuAlert })
        }
      }

      // Check Memory
      if (latestData.latest_memory !== null && latestData.latest_memory !== undefined) {
        const memAlert = checkMetricAlert('memory', latestData.latest_memory)
        if (memAlert.status !== 'normal') {
          alerts.push({ metric: 'Memory', ...memAlert })
        }
      }

      setActiveAlerts(alerts)
    } catch (error) {
      console.error('Error checking alerts:', error)
    }
  }

  const fetchData = async () => {
    try {
      setLoading(true)
      const [latestRes, summaryRes, tempRes, humidityRes, soilRes, lightRes, pressureRes] = await Promise.all([
        api.get('/api/metrics/latest'),
        api.get('/api/metrics/summary?minutes=1'),
        api.get('/api/metrics/history?metric_type=temperature&minutes=120'),
        api.get('/api/metrics/history?metric_type=humidity&minutes=120'),
        api.get('/api/metrics/history?metric_type=soil_moisture&minutes=120'),
        api.get('/api/metrics/history?metric_type=light_intensity&minutes=120'),
        api.get('/api/metrics/history?metric_type=pressure&minutes=120')
      ])

      // Check if we have data
      if (latestRes.data && latestRes.data.latest_cpu !== null) {
        setHasData(true)
        
        // Get latest values for each IoT metric (last value in history)
        const iotLatestValues = {
          temperature: tempRes.data?.data?.[tempRes.data.data.length - 1]?.value || 0,
          humidity: humidityRes.data?.data?.[humidityRes.data.data.length - 1]?.value || 0,
          soil_moisture: soilRes.data?.data?.[soilRes.data.data.length - 1]?.value || 0,
          light_intensity: lightRes.data?.data?.[lightRes.data.data.length - 1]?.value || 0,
          pressure: pressureRes.data?.data?.[pressureRes.data.data.length - 1]?.value || 0
        }
        
        setData({
          latest: latestRes.data,
          summary: summaryRes.data,
          latestIoT: tempRes.data?.data?.[0] || {},
          iotLatestValues
        })
        // Check for alerts
        fetchAlerts(latestRes.data)
      } else {
        setHasData(false)
      }
      setLastUpdate(new Date())
    } catch (error) {
      console.error('Failed to fetch data:', error)
      setHasData(false)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchData()
    const interval = setInterval(fetchData, 1000) // Auto-refresh
    return () => clearInterval(interval)
  }, [])

  const formatTime = (date) => {
    return formatVNTime(date, true)
  }

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold text-white">Real-Time Metrics Dashboard</h1>
          <p className="text-gray-400 mt-2">Live system performance monitoring</p>
        </div>
        <div className="flex items-center gap-4">
          <div className="text-right">
            <p className="text-sm text-gray-400">Last updated</p>
            <p className="text-lg text-neon-cyan font-mono">{formatTime(lastUpdate)}</p>
          </div>
          <button
            onClick={fetchData}
            className="p-3 bg-dark-800 border border-neon-cyan/30 rounded-lg hover:border-neon-cyan transition-all"
          >
            <RefreshCw className={`w-5 h-5 text-neon-cyan ${loading ? 'animate-spin' : ''}`} />
          </button>
        </div>
      </div>

      {/* Status Warning */}
      {!hasData ? (
        <div className="p-6 bg-dark-800 border border-neon-yellow/40 rounded-lg flex items-start gap-4">
          <AlertTriangle className="w-6 h-6 text-neon-yellow mt-1 flex-shrink-0" />
          <div>
            <h3 className="text-neon-yellow font-semibold">⚠️ Warning — No Recent Data</h3>
            <p className="text-gray-400 text-sm mt-1">
              Generate sample data by running: <code className="bg-dark-700 px-2 py-1 rounded">python generate_iot_data.py</code>
            </p>
          </div>
        </div>
      ) : null}

      {/* Gauge Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <GaugeChart
          title="CURRENT CPU"
          value={data.latest?.latest_cpu || 0}
          unit="%"
          color="neon-cyan"
          max={100}
        />
        <GaugeChart
          title="CURRENT MEMORY"
          value={data.latest?.latest_memory || 0}
          unit="%"
          color="neon-purple"
          max={100}
        />
        <div className="card-border card-hover p-6 bg-dark-800 flex flex-col justify-center">
          <h3 className="text-gray-400 text-sm mb-4">SYSTEM STATUS</h3>
          <div className="space-y-3">
            {hasData ? (
              <>
                <div className="flex items-center gap-2">
                  <div className="w-3 h-3 bg-green-500 rounded-full animate-pulse"></div>
                  <span className="text-green-400 text-sm">System Active</span>
                </div>
                <div className="text-2xl text-green-400 font-bold">Healthy</div>
              </>
            ) : (
              <>
                <div className="flex items-center gap-2">
                  <div className="w-3 h-3 bg-neon-yellow rounded-full animate-pulse"></div>
                  <span className="text-neon-yellow text-sm">Waiting for data</span>
                </div>
                <div className="text-2xl text-neon-yellow font-bold">Standby</div>
              </>
            )}
          </div>
        </div>
      </div>

      {/* Alerts & Anomalies */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="card-border card-hover p-6 bg-dark-800">
          <h3 className="text-gray-400 text-sm mb-4">⚠️ Active Alerts</h3>
          {activeAlerts.length === 0 ? (
            <div className="flex items-center gap-2">
              <CheckCircle className="w-5 h-5 text-neon-green" />
              <p className="text-neon-green text-sm">All systems normal</p>
            </div>
          ) : (
            <div className="space-y-2">
              {activeAlerts.map((alert, idx) => (
                <div key={idx} className="flex items-center gap-2">
                  <AlertCircle className="w-4 h-4 flex-shrink-0" style={{ color: alert.color }} />
                  <span className="text-xs" style={{ color: alert.color }}>
                    <strong>{alert.metric}</strong>: {alert.message}
                  </span>
                </div>
              ))}
            </div>
          )}
        </div>
        <div className="card-border card-hover p-6 bg-dark-800">
          <h3 className="text-gray-400 text-sm mb-4">📊 Recommendations</h3>
          <p className="text-gray-500 text-sm">
            {activeAlerts.length > 0 
              ? `${activeAlerts.length} active alert${activeAlerts.length > 1 ? 's' : ''}. Check the Alerts page for details.`
              : 'System performing optimally'
            }
          </p>
        </div>
      </div>

      {/* Metric Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        <MetricCard
          title="CPU"
          value={data.latest?.latest_cpu || 0}
          unit="%"
          icon="Cpu"
          color="neon-cyan"
        />
        <MetricCard
          title="MEMORY"
          value={data.latest?.latest_memory || 0}
          unit="%"
          icon="Zap"
          color="neon-purple"
        />
        <MetricCard
          title="AVG CPU (1M)"
          value={data.summary?.avg_cpu_1m || 0}
          unit="%"
          icon="TrendingUp"
          color="neon-cyan"
        />
        <MetricCard
          title="AVG MEM (1M)"
          value={data.summary?.avg_memory_1m || 0}
          unit="%"
          icon="BarChart3"
          color="neon-purple"
        />
      </div>

      {/* IoT Sensors Dashboard */}
      {data.iotLatestValues && data.iotLatestValues.temperature !== undefined && (
        <div className="space-y-4">
          <h2 className="text-xl font-bold text-white">📡 IoT Sensors Status</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
            {/* Temperature */}
            <div className="card-border card-hover p-5 bg-dark-800 border-l-4 border-red-500">
              <h4 className="text-gray-400 text-xs font-semibold mb-2">🌡️ TEMPERATURE</h4>
              <div className="text-3xl font-bold text-red-400">{data.iotLatestValues.temperature.toFixed(1)}</div>
              <p className="text-gray-500 text-xs mt-1">°C</p>
            </div>

            {/* Humidity */}
            <div className="card-border card-hover p-5 bg-dark-800 border-l-4 border-cyan-500">
              <h4 className="text-gray-400 text-xs font-semibold mb-2">💧 HUMIDITY</h4>
              <div className="text-3xl font-bold text-cyan-400">{data.iotLatestValues.humidity.toFixed(1)}</div>
              <p className="text-gray-500 text-xs mt-1">%</p>
            </div>

            {/* Soil Moisture */}
            <div className="card-border card-hover p-5 bg-dark-800 border-l-4 border-green-500">
              <h4 className="text-gray-400 text-xs font-semibold mb-2">🌱 SOIL MOISTURE</h4>
              <div className="text-3xl font-bold text-green-400">{data.iotLatestValues.soil_moisture.toFixed(1)}</div>
              <p className="text-gray-500 text-xs mt-1">%</p>
            </div>

            {/* Light Intensity */}
            <div className="card-border card-hover p-5 bg-dark-800 border-l-4 border-yellow-500">
              <h4 className="text-gray-400 text-xs font-semibold mb-2">💡 LIGHT INTENSITY</h4>
              <div className="text-3xl font-bold text-yellow-400">{data.iotLatestValues.light_intensity.toFixed(1)}</div>
              <p className="text-gray-500 text-xs mt-1">lux</p>
            </div>

            {/* Pressure */}
            <div className="card-border card-hover p-5 bg-dark-800 border-l-4 border-blue-500">
              <h4 className="text-gray-400 text-xs font-semibold mb-2">🌊 PRESSURE</h4>
              <div className="text-3xl font-bold text-blue-400">{data.iotLatestValues.pressure.toFixed(1)}</div>
              <p className="text-gray-500 text-xs mt-1">hPa</p>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
