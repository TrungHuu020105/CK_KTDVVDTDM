import { useState, useEffect } from 'react'
import { AlertTriangle, CheckCircle, Sparkles, X } from 'lucide-react'
import api from '../api'

export default function Alerts() {
  const [alerts, setAlerts] = useState([])
  const [lastUpdated, setLastUpdated] = useState(null)
  const [explainingAlertId, setExplainingAlertId] = useState(null)
  const [aiExplainModal, setAiExplainModal] = useState({ open: false, title: '', content: '', meta: null })
  const [filters, setFilters] = useState({
    search: '',
    metric: 'all',
    source: 'all',
  })

  const formatMetricName = (metric) => {
    const names = {
      cpu: 'CPU Usage',
      memory: 'Memory Usage',
      temperature: 'Temperature',
      humidity: 'Humidity',
      soil_moisture: 'Soil Moisture',
      light_intensity: 'Light Intensity',
      pressure: 'Pressure',
    }
    return names[metric] || metric
  }

  const getMetricUnit = (metric) => {
    const units = {
      cpu: '%',
      memory: '%',
      temperature: '°C',
      humidity: '%',
      soil_moisture: '%',
      light_intensity: 'lux',
      pressure: 'hPa',
    }
    return units[metric] || ''
  }

  const formatVNDateTime = (value) => {
    if (!value) return 'N/A'
    const date = new Date(value)
    if (Number.isNaN(date.getTime())) return String(value)
    return date.toLocaleString('vi-VN', { hour12: false })
  }

  const formatVNTime = (value) => {
    const date = value instanceof Date ? value : new Date(value)
    if (Number.isNaN(date.getTime())) return 'N/A'
    return date.toLocaleTimeString('vi-VN', { hour12: false })
  }

  const fetchAlerts = async () => {
    try {
      const response = await api.get('/api/alerts?hours=24&limit=100')
      setAlerts(response.data?.alerts || [])
      setLastUpdated(new Date())
    } catch (error) {
      console.error('Error fetching alerts:', error)
    }
  }

  useEffect(() => {
    fetchAlerts()
    const interval = setInterval(fetchAlerts, 2000)
    return () => clearInterval(interval)
  }, [])

  const getAlertIcon = () => <AlertTriangle className="w-6 h-6 flex-shrink-0" />
  const getAlertColor = () => 'text-neon-red'
  const getStatusColor = () => '#ff3333'

  const explainWithAI = async (alert) => {
    try {
      setExplainingAlertId(alert.id)
      const response = await api.get(`/api/alerts/${alert.id}/explain-ai`)
      const apiSuccess = Boolean(response?.data?.success)
      const apiMessage = response?.data?.message || ''
      const apiErrorCode = response?.data?.error_code || null
      const apiErrorDetail = response?.data?.error_detail || null
      const apiRetryAfter = response?.data?.retry_after_seconds
      const shortMessage = apiMessage?.split('Chi tiet ky thuat:')[0]?.trim() || apiMessage
      const explanation = apiSuccess
        ? (response?.data?.explanation || 'Không có nội dung giải thích.')
        : `Không thể tạo giải thích AI: ${shortMessage || 'Lỗi không xác định từ backend.'}`
      const retryLine = !apiSuccess && apiRetryAfter
        ? `\n[Debug] retry_after_seconds: ${apiRetryAfter}`
        : ''
      const debugText = !apiSuccess
        ? `\n\n[Debug] error_code: ${apiErrorCode || 'N/A'}${retryLine}\n[Debug] error_detail: ${apiErrorDetail || 'N/A'}`
        : ''
      setAiExplainModal({
        open: true,
        title: `${formatMetricName(alert.metric_type)} - ${alert.status.toUpperCase()}`,
        content: `${explanation}${debugText}`,
        meta: response?.data?.context || null,
      })
    } catch (error) {
      const httpStatus = error?.response?.status
      const backendDetail = error?.response?.data?.detail
      setAiExplainModal({
        open: true,
        title: 'Giải thích bằng AI',
        content:
          `Không thể tạo giải thích AI: ${backendDetail || error.message}\n\n` +
          `[Debug] http_status: ${httpStatus || 'N/A'}\n` +
          `[Debug] backend_detail: ${backendDetail || 'N/A'}`,
        meta: null,
      })
    } finally {
      setExplainingAlertId(null)
    }
  }

  const metricOptions = Array.from(new Set(alerts.map((a) => a.metric_type).filter(Boolean)))
  const sourceOptions = Array.from(new Set(alerts.map((a) => a.source).filter(Boolean)))

  const filteredAlerts = alerts.filter((alert) => {
    const metricOk = filters.metric === 'all' || alert.metric_type === filters.metric
    const sourceOk = filters.source === 'all' || alert.source === filters.source
    const searchText = filters.search.trim().toLowerCase()
    const searchOk = !searchText || [
      alert.metric_type,
      alert.source,
      alert.location,
      alert.message,
      alert.status,
    ].some((field) => String(field || '').toLowerCase().includes(searchText))

    return metricOk && sourceOk && searchOk
  })

  return (
    <div className="p-6 space-y-6">
      <div>
        <h1 className="text-3xl font-bold text-white">Alerts & Anomalies</h1>
        <p className="text-gray-400 mt-2">
          System alerts
          {lastUpdated && (
            <span className="ml-4 text-xs text-gray-500">
              Last updated: {formatVNTime(lastUpdated)}
            </span>
          )}
        </p>
      </div>

      <div className="grid grid-cols-2 gap-4">
        <div className="card-border p-4 bg-dark-800">
          <p className="text-gray-400 text-sm">Showing</p>
          <p className="text-2xl font-bold text-neon-cyan">{filteredAlerts.length}/{alerts.length}</p>
        </div>
        <div className="card-border p-4 bg-dark-800">
          <p className="text-gray-400 text-sm">Alert Type</p>
          <p className="text-2xl font-bold text-neon-red">ALERT</p>
        </div>
      </div>

      <div className="card-border p-4 bg-dark-800">
        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-3">
          <input
            type="text"
            value={filters.search}
            onChange={(e) => setFilters((prev) => ({ ...prev, search: e.target.value }))}
            className="bg-dark-900 border border-gray-700 rounded-lg px-3 py-2 text-white focus:border-neon-cyan outline-none"
            placeholder="Search alert message/source/location..."
          />
          <select
            value={filters.metric}
            onChange={(e) => setFilters((prev) => ({ ...prev, metric: e.target.value }))}
            className="bg-dark-900 border border-gray-700 rounded-lg px-3 py-2 text-white focus:border-neon-cyan outline-none"
          >
            <option value="all">All Metrics</option>
            {metricOptions.map((metric) => (
              <option key={metric} value={metric}>{formatMetricName(metric)}</option>
            ))}
          </select>
          <select
            value={filters.source}
            onChange={(e) => setFilters((prev) => ({ ...prev, source: e.target.value }))}
            className="bg-dark-900 border border-gray-700 rounded-lg px-3 py-2 text-white focus:border-neon-cyan outline-none"
          >
            <option value="all">All Sources</option>
            {sourceOptions.map((source) => (
              <option key={source} value={source}>{source}</option>
            ))}
          </select>
          <button
            type="button"
            onClick={() => setFilters({ search: '', metric: 'all', source: 'all' })}
            className="px-3 py-2 rounded-lg bg-gray-700 text-white hover:bg-gray-600 transition-all"
          >
            Reset Filters
          </button>
        </div>
      </div>

      {filteredAlerts.length === 0 ? (
        <div className="card-border p-6 bg-dark-800">
          <div className="flex items-center gap-4">
            <CheckCircle className="w-8 h-8 text-neon-green flex-shrink-0" />
            <div>
              <h3 className="text-white font-semibold">{alerts.length === 0 ? 'All Systems Healthy' : 'No Matching Alerts'}</h3>
              <p className="text-gray-400 text-sm mt-1">
                {alerts.length === 0 ? 'No alerts in the last 24 hours' : 'Try changing filters to see more alerts'}
              </p>
            </div>
          </div>
        </div>
      ) : (
        <div className="space-y-4">
          {filteredAlerts.map((alert) => (
            <div
              key={alert.id}
              className="card-border card-hover p-6 bg-dark-800 flex items-start gap-4 border border-opacity-50"
              style={{ borderColor: getStatusColor() }}
            >
              <div className={getAlertColor()}>{getAlertIcon()}</div>
              <div className="flex-1">
                <div className="flex items-center justify-between">
                  <h3 className="text-white font-semibold">{formatMetricName(alert.metric_type)}</h3>
                  <span className="px-3 py-1 rounded text-xs font-semibold text-dark-900" style={{ backgroundColor: getStatusColor() }}>
                    ALERT
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
                  <p className="text-xs text-gray-500 ml-auto">{formatVNDateTime(alert.created_at)}</p>
                </div>
                <div className="mt-4">
                  <button
                    onClick={() => explainWithAI(alert)}
                    disabled={explainingAlertId === alert.id}
                    className="inline-flex items-center gap-2 px-3 py-2 rounded-lg bg-neon-cyan/15 border border-neon-cyan/40 text-neon-cyan hover:bg-neon-cyan/25 disabled:opacity-50 transition-all text-sm"
                  >
                    <Sparkles className="w-4 h-4" />
                    {explainingAlertId === alert.id ? 'Đang phân tích...' : 'Giải thích bằng AI'}
                  </button>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}

      {aiExplainModal.open && (
        <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50 p-4">
          <div className="bg-dark-800 border border-neon-cyan/30 rounded-xl w-full max-w-4xl p-6 md:p-8">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-2xl md:text-3xl font-bold text-neon-cyan">{aiExplainModal.title}</h3>
              <button onClick={() => setAiExplainModal({ open: false, title: '', content: '', meta: null })} className="text-gray-400 hover:text-white">
                <X className="w-6 h-6" />
              </button>
            </div>
            {aiExplainModal.meta && (
              <p className="text-sm md:text-base text-gray-400 mb-4">
                Weather context: {aiExplainModal.meta.has_weather ? 'available' : 'not available'} | Environment: {aiExplainModal.meta.environment_type || 'unknown'}
              </p>
            )}
            <pre className="whitespace-pre-wrap text-gray-200 leading-8 font-sans text-base md:text-lg bg-dark-900/60 rounded-lg p-5 md:p-6 border border-gray-700 max-h-[60vh] overflow-y-auto">
              {aiExplainModal.content}
            </pre>
          </div>
        </div>
      )}
    </div>
  )
}
