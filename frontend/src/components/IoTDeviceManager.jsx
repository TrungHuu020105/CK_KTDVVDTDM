import { useState, useEffect, useRef } from 'react'
import { Plus, Trash2, Edit2, Home, Radio, X, Power, PowerOff, AlertCircle } from 'lucide-react'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'
import { useDevices } from '../context/DeviceContext'
import AddDeviceModal from './AddDeviceModal'
import EditAlertThresholdsModal from './EditAlertThresholdsModal'
import api from '../api'

const LATEST_METRICS_CACHE_KEY = 'iotLatestMetricsCacheV1'

const normalizeKey = (value) => String(value || '').trim().toLowerCase()

const readLatestMetricsCache = () => {
  try {
    const raw = localStorage.getItem(LATEST_METRICS_CACHE_KEY)
    if (!raw) return {}
    const parsed = JSON.parse(raw)
    return parsed && typeof parsed === 'object' ? parsed : {}
  } catch {
    return {}
  }
}

const writeLatestMetricsCache = (metrics) => {
  try {
    localStorage.setItem(LATEST_METRICS_CACHE_KEY, JSON.stringify(metrics || {}))
  } catch {
    // Ignore cache write errors.
  }
}

export default function IoTDeviceManager() {
  const { iotDevices, createIoTDevice, updateIoTDevice, deleteIoTDevice, fetchIoTDevices } = useDevices()
  const isAdmin = false
  
  const displayDevices = iotDevices
  
  // ADMIN: State for admin summary view
  const [adminSummary, setAdminSummary] = useState({
    users_summary: [],
    total_devices: 0,
    total_users: 0
  })
  
  // Debug log
  useEffect(() => {
    console.log('IoTDeviceManager - iotDevices:', iotDevices)
    console.log('IoTDeviceManager - displayDevices:', displayDevices)
    if (displayDevices && displayDevices.length > 0) {
      console.log('First device details:', displayDevices[0])
      console.log('Device keys:', Object.keys(displayDevices[0]))
    }
  }, [iotDevices, displayDevices])
  const wsRef = useRef(null)
  const [showCreateModal, setShowCreateModal] = useState(false)
  const [showAddDeviceModal, setShowAddDeviceModal] = useState(false)
  const [showChartModal, setShowChartModal] = useState(false)
  const [showAlertThresholdsModal, setShowAlertThresholdsModal] = useState(false)
  const [selectedDeviceForChart, setSelectedDeviceForChart] = useState(null)
  const [selectedDeviceForAlert, setSelectedDeviceForAlert] = useState(null)
  const [editingId, setEditingId] = useState(null)
  const [formData, setFormData] = useState({
    name: '',
    device_type: 'temperature',
    source: '',
    location: ''
  })
  const [loading, setLoading] = useState(false)
  const [addingDevice, setAddingDevice] = useState(false)
  const [latestMetrics, setLatestMetrics] = useState(() => readLatestMetricsCache())
  const [chartData, setChartData] = useState([])
  const [chartLoading, setChartLoading] = useState(false)
  const [chartStats, setChartStats] = useState({
    average: 0,
    min: 0,
    max: 0
  })
  const [chartLastUpdated, setChartLastUpdated] = useState(null)
  const [savingAlerts, setSavingAlerts] = useState(false)
  const latestFetchInFlightRef = useRef(false)

  // Fetch latest metrics for all devices
  useEffect(() => {
    const fetchLatestMetrics = async () => {
      if (!displayDevices.length) return
      if (latestFetchInFlightRef.current) return

      try {
        latestFetchInFlightRef.current = true

        // Fetch latest snapshot once to avoid N requests and timeout issues.
        const response = await api.get('/api/databricks/latest')
        const latestRows = response.data?.data || []

        const bySource = {}
        const byType = {}
        latestRows.forEach((row) => {
          const src = normalizeKey(row?.device_id || row?.source)
          if (!src) return
          bySource[src] = {
            value: row?.value ?? null,
            timestamp: row?.timestamp ?? null
          }

          const t = normalizeKey(row?.device_type)
          if (t && !byType[t]) {
            byType[t] = {
              value: row?.value ?? null,
              timestamp: row?.timestamp ?? null
            }
          }
        })

        setLatestMetrics((prev) => {
          const next = { ...prev }

          displayDevices.forEach((device) => {
            const sourceKey = normalizeKey(device.source)
            const idKey = normalizeKey(device.id)
            const typeKey = normalizeKey(device.device_type)

            const matched = bySource[sourceKey] || bySource[idKey] || byType[typeKey]
            if (matched && matched.value !== null && matched.value !== undefined) {
              next[device.id] = matched
            } else if (!next[device.id]) {
              next[device.id] = { value: null, timestamp: null }
            }
          })

          writeLatestMetricsCache(next)
          return next
        })
      } catch (err) {
        console.error('Failed to fetch latest metrics:', err)
      } finally {
        latestFetchInFlightRef.current = false
      }
    }

    // Fetch immediately and then every 2 seconds (balanced for Databricks latency)
    fetchLatestMetrics()
    const interval = setInterval(fetchLatestMetrics, 2000)
    return () => clearInterval(interval)
  }, [displayDevices])

  // Connect to WebSocket for realtime IoT data updates
  useEffect(() => {
    const connectWebSocket = () => {
      try {
        const serverUrl = import.meta.env.VITE_SERVER_IP || 'localhost'
        const serverPort = import.meta.env.VITE_SERVER_PORT || '8000'
        const clientId = `frontend_iot_${Date.now()}`
        const wsUrl = `ws://${serverUrl}:${serverPort}/api/ws/${clientId}`
        
        console.log('[IoTDeviceManager] Connecting to WebSocket:', wsUrl)
        wsRef.current = new WebSocket(wsUrl)
        
        wsRef.current.onopen = () => {
          console.log('[IoTDeviceManager] WebSocket connected for realtime updates')
        }
        
        wsRef.current.onmessage = (event) => {
          try {
            const message = JSON.parse(event.data)
            console.log('[IoTDeviceManager] Received WebSocket message:', message)
            
            // Handle realtime IoT metric broadcasts
            if ((message.type === 'iot_metric' || message.type === 'iot_metric_realtime') && message.metric_type && message.value !== undefined) {
              setLatestMetrics(prev => {
                const updated = { ...prev }
                // Prefer exact source match to avoid writing one metric into all same-type cards.
                displayDevices.forEach(device => {
                  if (
                    (message.source && device.source === message.source) ||
                    (!message.source && device.device_type === message.metric_type)
                  ) {
                    updated[device.id] = {
                      value: message.value,
                      timestamp: message.timestamp
                    }
                  }
                })
                return updated
              })
            }
          } catch (err) {
            console.error('[IoTDeviceManager] Failed to parse WebSocket data:', err)
          }
        }
        
        wsRef.current.onerror = (event) => {
          console.error('[IoTDeviceManager] WebSocket error:', event)
          console.error('[IoTDeviceManager] WebSocket error type:', event.type)
          console.error('[IoTDeviceManager] WebSocket ready state:', wsRef.current?.readyState)
          // 0=CONNECTING, 1=OPEN, 2=CLOSING, 3=CLOSED
          console.error('[IoTDeviceManager] Error connecting to:', wsUrl)
        }
        
        wsRef.current.onclose = () => {
          console.log('[IoTDeviceManager] WebSocket disconnected, reconnecting...')
          setTimeout(connectWebSocket, 3000)
        }
      } catch (err) {
        console.error('[IoTDeviceManager] Failed to connect WebSocket:', err)
      }
    }
    
    connectWebSocket()
    
    return () => {
      if (wsRef.current) {
        wsRef.current.close()
      }
    }
  }, [displayDevices])

  // Helper function to aggregate data by minute (1 point per minute)
  const aggregateDataByMinute = (rawData) => {
    if (!rawData || rawData.length === 0) return []
    
    // Group data by minute
    const minuteGroups = {}
    rawData.forEach(item => {
      const date = new Date(item.timestamp)
      // Round to nearest minute
      date.setSeconds(0, 0)
      const minuteKey = date.getTime()
      
      if (!minuteGroups[minuteKey]) {
        minuteGroups[minuteKey] = []
      }
      minuteGroups[minuteKey].push(item.value)
    })
    
    // Calculate average for each minute and format
    const aggregatedData = Object.keys(minuteGroups)
      .sort((a, b) => parseInt(a) - parseInt(b))
      .map(minuteKey => {
        const values = minuteGroups[minuteKey]
        const avgValue = values.reduce((a, b) => a + b, 0) / values.length
        const date = new Date(parseInt(minuteKey))
        
        return {
          time: date.toLocaleTimeString('en-US', {
            hour: '2-digit',
            minute: '2-digit'
          }),
          value: parseFloat(avgValue.toFixed(2)),
          timestamp: date.toISOString()
        }
      })
    
    return aggregatedData
  }

  // Fetch chart data for selected device (past 2 hours = 120 minutes)
  const handleOpenChart = async (device) => {
    setSelectedDeviceForChart(device)
    setShowChartModal(true)
    setChartLoading(true)
    try {
      const response = await api.get(`/api/databricks/history/${encodeURIComponent(device.source)}`, {
        params: {
          minutes: 120 // 2 hours
        }
      })
      
      // Aggregate data by minute (1 point per minute)
      const formattedData = aggregateDataByMinute(response.data.data)
      
      // Calculate statistics from original data (not aggregated)
      if (response.data.data && response.data.data.length > 0) {
        const values = response.data.data.map(d => d.value)
        const average = values.reduce((a, b) => a + b, 0) / values.length
        const min = Math.min(...values)
        const max = Math.max(...values)
        
        setChartStats({
          average: parseFloat(average.toFixed(2)),
          min: parseFloat(min.toFixed(2)),
          max: parseFloat(max.toFixed(2))
        })
      }
      
      setChartData(formattedData)
      setChartLastUpdated(new Date())
    } catch (err) {
      console.error('Failed to fetch chart data:', err)
      setChartData([])
      setChartStats({ average: 0, min: 0, max: 0 })
    } finally {
      setChartLoading(false)
    }
  }

  // Auto-refresh chart every 1 minute when modal is open
  useEffect(() => {
    if (!showChartModal || !selectedDeviceForChart) return

    const refreshChartData = async () => {
      try {
        console.log('Refreshing chart data for device:', selectedDeviceForChart.device_type)
        const response = await api.get(`/api/databricks/history/${encodeURIComponent(selectedDeviceForChart.source)}`, {
          params: {
            minutes: 120 // 2 hours
          }
        })
        
        // Aggregate data by minute (1 point per minute)
        const formattedData = aggregateDataByMinute(response.data.data)
        
        // Calculate statistics from original data (not aggregated)
        if (response.data.data && response.data.data.length > 0) {
          const values = response.data.data.map(d => d.value)
          const average = values.reduce((a, b) => a + b, 0) / values.length
          const min = Math.min(...values)
          const max = Math.max(...values)
          
          setChartStats({
            average: parseFloat(average.toFixed(2)),
            min: parseFloat(min.toFixed(2)),
            max: parseFloat(max.toFixed(2))
          })
        }
        
        setChartData(formattedData)
        setChartLastUpdated(new Date())
        console.log('Chart updated at:', new Date().toLocaleTimeString())
      } catch (err) {
        console.error('Failed to refresh chart data:', err)
      }
    }

    // Refresh immediately when modal opens
    refreshChartData()
    
    // Then refresh every 1 minute (60000 ms)
    const interval = setInterval(refreshChartData, 60000)
    
    return () => clearInterval(interval)
  }, [showChartModal, selectedDeviceForChart])

  // Handle adding device via AddDeviceModal (for users)
  const handleAddDevice = async (deviceData) => {
    try {
      setAddingDevice(true)
      await createIoTDevice(deviceData)
      setShowAddDeviceModal(false)
    } catch (err) {
      console.error('Failed to add device:', err)
      alert('Error: ' + (err.response?.data?.detail || err.message))
    } finally {
      setAddingDevice(false)
    }
  }

  const handleSubmit = async (e) => {
    e.preventDefault()
    try {
      setLoading(true)
      
      if (editingId) {
        await updateIoTDevice(editingId, formData)
        setEditingId(null)
      } else {
        await createIoTDevice(formData)
      }
      
      setFormData({
        name: '',
        device_type: 'temperature',
        source: '',
        location: ''
      })
      setShowCreateModal(false)
    } catch (err) {
      alert('Error: ' + err.message)
    } finally {
      setLoading(false)
    }
  }

  const handleDelete = async (deviceId) => {
    if (confirm('Delete this IoT device?')) {
      try {
        await deleteIoTDevice(deviceId)
      } catch (err) {
        alert('Error: ' + err.message)
      }
    }
  }

  // Toggle device active/inactive status (for admin - Device model)
  const toggleDeviceActive = async (deviceId) => {
    try {
      setLoading(true)
      alert('Admin functionality has been removed')
    } catch (err) {
      console.error('Error toggling device:', err)
      alert('Error: ' + (err.response?.data?.detail || err.message))
    } finally {
      setLoading(false)
    }
  }

  // Toggle IoT device active/inactive (user's own devices)
  const toggleIoTDeviceActive = async (deviceId, currentStatus) => {
    try {
      setLoading(true)
      await updateIoTDevice(deviceId, { is_active: !currentStatus })
      alert(`Device ${!currentStatus ? 'enabled' : 'disabled'} successfully`)
      // Devices list will auto-update via context
    } catch (err) {
      console.error('Error toggling IoT device:', err)
      alert('Error: ' + (err.response?.data?.detail || err.message))
    } finally {
      setLoading(false)
    }
  }

  // Admin functions
  const disconnectIoTDevice = async (deviceId) => {
    try {
      alert('Admin functionality has been removed')
    } catch (err) {
      alert('Error disconnecting device: ' + (err.response?.data?.detail || err.message))
    }
  }

  const reconnectIoTDevice = async (deviceId) => {
    try {
      alert('Admin functionality has been removed')
    } catch (err) {
      alert('Error reconnecting device: ' + (err.response?.data?.detail || err.message))
    }
  }

  const adminDeleteIoTDevice = async (deviceId) => {
    if (confirm('Delete this IoT device? (Admin action)')) {
      try {
        alert('Admin functionality has been removed')
      } catch (err) {
        alert('Error deleting device: ' + (err.response?.data?.detail || err.message))
      }
    }
  }

  const startEdit = (device) => {
    setFormData({
      name: device.name,
      device_type: device.device_type,
      source: device.source,
      location: device.location || ''
    })
    setEditingId(device.id)
    setShowCreateModal(true)
  }

  const openAlertThresholdsModal = (device) => {
    setSelectedDeviceForAlert(device)
    setShowAlertThresholdsModal(true)
  }

  const handleSaveAlertThresholds = async (thresholdData) => {
    if (!selectedDeviceForAlert) return

    try {
      setSavingAlerts(true)
      console.log('API URL:', api.defaults.baseURL)
      console.log('Auth Header:', api.defaults.headers.common['Authorization'])
      console.log('Sending threshold data:', thresholdData)
      
      const response = await api.put(`/api/iot-devices/${selectedDeviceForAlert.id}/alert-thresholds`, thresholdData)
      console.log('Response received:', response.data)
      alert('✅ ' + response.data.message)
      setShowAlertThresholdsModal(false)
      
      // Refresh devices to get updated threshold values
      console.log('Refreshing device list...')
      await fetchIoTDevices()
      console.log('Device list refreshed successfully')
    } catch (err) {
      console.error('Full error object:', err)
      console.error('Error message:', err.message)
      console.error('Error config:', err.config)
      console.error('Error response:', err.response)
      alert('Error: ' + (err.response?.data?.detail || err.message || 'Unknown error'))
    } finally {
      setSavingAlerts(false)
    }
  }

  const getDeviceTypeColor = (type) => {
    const colors = {
      temperature: 'neon-orange',
      humidity: 'neon-cyan',
      soil_moisture: 'neon-green',
      light_intensity: 'neon-yellow',
      pressure: 'neon-purple'
    }
    return colors[type] || 'neon-cyan'
  }

  const getMetricUnit = (type) => {
    const units = {
      temperature: '°C',
      humidity: '%',
      soil_moisture: '%',
      light_intensity: 'lux',
      pressure: 'hPa'
    }
    return units[type] || ''
  }

  const getLatestValue = (device) => {
    const metric = latestMetrics[device.id]
    if (!metric || metric.value === null || metric.value === undefined) {
      if (typeof device.mean_value === 'number') {
        return device.mean_value.toFixed(2)
      }
      return 'N/A'
    }
    return typeof metric.value === 'number' ? metric.value.toFixed(2) : metric.value
  }

  // Check if alert is triggered for a device
  const checkAlertTriggered = (device) => {
    const latestValue = latestMetrics[device.id]?.value
    console.log(`[checkAlertTriggered] Device: ${device.name}, latestValue: ${latestValue}, alert_enabled: ${device.alert_enabled}`)
    console.log(`[checkAlertTriggered] Thresholds - Lower: ${device.lower_threshold}, Upper: ${device.upper_threshold}`)
    
    // If no value, no alert
    if (!latestValue) return { triggered: false, status: null }

    // Check if thresholds are set (either alert_enabled=true OR thresholds exist)
    const hasThresholds = (device.lower_threshold !== null && device.lower_threshold !== undefined) ||
                          (device.upper_threshold !== null && device.upper_threshold !== undefined)
    
    if (!hasThresholds) return { triggered: false, status: null }

    const lowerThreshold = device.lower_threshold
    const upperThreshold = device.upper_threshold

    // Check upper threshold (exceeds high limit)
    if (upperThreshold !== null && upperThreshold !== undefined && latestValue > upperThreshold) {
      console.log(`[checkAlertTriggered] ALERT triggered (Upper) for ${device.name}: ${latestValue} > ${upperThreshold}`)
      return { triggered: true, status: 'upper', threshold: upperThreshold }
    }

    // Check lower threshold (falls below low limit)
    if (lowerThreshold !== null && lowerThreshold !== undefined && latestValue < lowerThreshold) {
      console.log(`[checkAlertTriggered] ALERT triggered (Lower) for ${device.name}: ${latestValue} < ${lowerThreshold}`)
      return { triggered: true, status: 'lower', threshold: lowerThreshold }
    }

    return { triggered: false, status: null }
  }

  return (
    <div className="p-8 space-y-8">
      {/* Admin View - User Device Count Summary */}
      {isAdmin ? (
        <>
          <div>
            <h1 className="text-4xl font-bold text-white mb-2">IoT Devices Overview</h1>
            <p className="text-gray-400">Users and their device counts (Admin view - no device details)</p>
          </div>

          {/* User Device Count Table */}
          <div className="bg-slate-800 rounded-lg border border-slate-700 overflow-hidden">
            <table className="w-full">
              <thead className="bg-slate-900 border-b border-slate-700">
                <tr>
                  <th className="px-6 py-3 text-left text-cyan-400 font-semibold">User</th>
                  <th className="px-6 py-3 text-left text-cyan-400 font-semibold">Email</th>
                  <th className="px-6 py-3 text-left text-cyan-400 font-semibold">Device Count</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-700">
                {adminSummary?.users_summary?.map((item) => (
                  <tr key={item.user_id} className="hover:bg-slate-700/50 transition">
                    <td className="px-6 py-3 text-white font-medium">{item.username}</td>
                    <td className="px-6 py-3 text-gray-400">{item.email}</td>
                    <td className="px-6 py-3">
                      <span className="bg-cyan-600/20 text-cyan-400 px-3 py-1 rounded-full text-sm font-medium">
                        {item.device_count} device{item.device_count !== 1 ? 's' : ''}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            {!adminSummary?.users_summary || adminSummary.users_summary.length === 0 && (
              <div className="text-center py-8 text-gray-400">No users with devices</div>
            )}
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="p-4 bg-slate-700/50 rounded-lg">
              <p className="text-gray-300 text-sm">Total Devices</p>
              <p className="text-cyan-400 font-bold text-2xl">{adminSummary?.total_devices || 0}</p>
            </div>
            <div className="p-4 bg-slate-700/50 rounded-lg">
              <p className="text-gray-300 text-sm">Total Users</p>
              <p className="text-cyan-400 font-bold text-2xl">{adminSummary?.total_users || 0}</p>
            </div>
          </div>
        </>
      ) : (
        <>
          {/* User View - Device Details and Add Button */}
          <div className="flex justify-between items-start">
            <div>
              <h1 className="text-4xl font-bold text-white mb-2">IoT Devices</h1>
              <p className="text-gray-400">Manage your IoT sensors and devices</p>
            </div>
            <button
              onClick={() => setShowAddDeviceModal(true)}
              className="flex items-center gap-2 bg-neon-cyan/20 text-neon-cyan border border-neon-cyan/40 hover:border-neon-cyan px-4 py-2 rounded-lg transition-all"
            >
              <Plus className="w-5 h-5" />
              Add Device
            </button>
          </div>

          {/* Devices Grid - Only for Users */}
          {displayDevices.length === 0 ? (
            <div className="text-center py-16">
              <Radio className="w-16 h-16 text-gray-600 mx-auto mb-4 opacity-50" />
              <h3 className="text-xl font-semibold text-white mb-2">No IoT Devices</h3>
              <p className="text-gray-400 mb-6">Create your first IoT device to start monitoring</p>
              <button
                onClick={() => setShowAddDeviceModal(true)}
                className="inline-flex items-center gap-2 bg-neon-cyan/20 text-neon-cyan border border-neon-cyan/40 px-6 py-3 rounded-lg hover:border-neon-cyan transition-all"
              >
                <Plus className="w-5 h-5" />
                Create First Device
              </button>
            </div>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {displayDevices.map(device => {
                const alertStatus = checkAlertTriggered(device)
                const cardBorderColor = alertStatus.triggered
                  ? 'border-red-500/60 bg-red-950/20'
                  : 'border-neon-cyan/20 bg-dark-800'
                
                return (
                <div
                  key={device.id}
                  onClick={() => handleOpenChart(device)}
                  className={`rounded-xl p-6 hover:border-neon-cyan/60 hover:cursor-pointer transition-all hover:bg-dark-700 ${cardBorderColor}`}
                >
                  <div className="flex items-start justify-between mb-4">
                    <div>
                      <h3 className="text-lg font-bold text-white mb-1">{device.name}</h3>
                      <p className="text-xs text-gray-500 font-mono">Source: {device.source}</p>
                    </div>
                    <span className={`text-xs text-${getDeviceTypeColor(device.device_type)} bg-${getDeviceTypeColor(device.device_type)}/20 px-2 py-1 rounded`}>
                      {device.device_type}
                    </span>
                  </div>

                  {device.location && (
                    <div className="flex items-center gap-2 text-sm text-gray-400 mb-4">
                      <Home className="w-4 h-4" />
                      {device.location}
                    </div>
                  )}

                  {/* Real-time Metric Display - Integrated with Thresholds */}
                  <div className={`rounded-lg p-4 mb-4 transition-all ${
                    alertStatus.triggered
                      ? 'bg-red-950/40 border-2 border-red-500/60'
                      : 'bg-dark-900/50 border border-neon-cyan/20'
                  }`}>
                    {/* Sensor Value + Status */}
                    <div className="flex items-end justify-between mb-3">
                      <div>
                        <p className="text-xs text-gray-400 mb-1">Real-time Value</p>
                        <div className="flex items-baseline gap-2">
                          <span className={`text-3xl font-bold ${
                            alertStatus.triggered
                              ? 'text-red-400'
                              : 'text-neon-cyan'
                          }`}>
                            {getLatestValue(device)}
                          </span>
                          <span className="text-sm text-gray-400">
                            {getMetricUnit(device.device_type)}
                          </span>
                        </div>
                      </div>
                      {alertStatus.triggered && (
                        <div className="flex items-center gap-1 px-2 py-1 rounded text-xs font-semibold bg-red-500/30 text-red-400">
                          🚨 OUT OF RANGE
                        </div>
                      )}
                    </div>

                    {/* Alert Thresholds - Inline */}
                    {((device.lower_threshold !== null && device.lower_threshold !== undefined) ||
                      (device.upper_threshold !== null && device.upper_threshold !== undefined)) && (
                      <div className="grid grid-cols-2 gap-2 pt-3 border-t border-gray-700/50">
                        {device.lower_threshold !== null && device.lower_threshold !== undefined && (
                          <div className={`rounded px-2 py-2 border text-center ${
                            alertStatus.triggered && alertStatus.status === 'lower'
                              ? 'bg-red-500/20 border-red-500/60'
                              : 'bg-blue-500/10 border-blue-500/30'
                          }`}>
                            <p className="text-xs text-blue-400/70 font-semibold">⬇️ Min</p>
                            <p className="text-sm font-mono text-blue-300">{
                              typeof device.lower_threshold === 'number' 
                                ? device.lower_threshold.toFixed(1) 
                                : device.lower_threshold
                            }</p>
                          </div>
                        )}
                        {device.upper_threshold !== null && device.upper_threshold !== undefined && (
                          <div className={`rounded px-2 py-2 border text-center ${
                            alertStatus.triggered && alertStatus.status === 'upper'
                              ? 'bg-red-500/20 border-red-500/60'
                              : 'bg-green-500/10 border-green-500/30'
                          }`}>
                            <p className="text-xs text-green-400/70 font-semibold">⬆️ Max</p>
                            <p className="text-sm font-mono text-green-300">{
                              typeof device.upper_threshold === 'number' 
                                ? device.upper_threshold.toFixed(1) 
                                : device.upper_threshold
                            }</p>
                          </div>
                        )}
                      </div>
                    )}
                  </div>

                  <div className="flex items-center gap-2 mb-4">
                    <span className={`w-2 h-2 ${device.is_active ? 'bg-green-500' : 'bg-red-500'} rounded-full`}></span>
                    <span className={`text-xs ${device.is_active ? 'text-green-400' : 'text-red-400'}`}>
                      {device.is_active ? 'Active' : 'Inactive'}
                    </span>
                    {isAdmin && (
                      <button
                        onClick={(e) => {
                          e.stopPropagation()
                          toggleDeviceActive(device.id)
                        }}
                        className="ml-auto flex items-center gap-1 px-2 py-1 bg-yellow-500/20 text-yellow-400 rounded hover:bg-yellow-500/30 transition-all text-xs"
                        title={device.is_active ? 'Disable metric generation' : 'Enable metric generation'}
                      >
                        {device.is_active ? <PowerOff className="w-3 h-3" /> : <Power className="w-3 h-3" />}
                        {device.is_active ? 'Disable' : 'Enable'}
                      </button>
                    )}
                  </div>

                  <div className="flex flex-col gap-2">
                    {/* User buttons */}
                    <div className="flex gap-2">
                      <button
                        onClick={(e) => {
                          e.stopPropagation()
                          openAlertThresholdsModal(device)
                        }}
                        className="flex-1 flex items-center justify-center gap-2 px-3 py-2 bg-amber-500/20 text-amber-400 rounded hover:bg-amber-500/30 transition-all text-sm"
                        title="Configure alert thresholds"
                      >
                        <AlertCircle className="w-4 h-4" />
                        Alerts
                      </button>
                      <button
                        onClick={(e) => {
                          e.stopPropagation()
                          startEdit(device)
                        }}
                        className="flex-1 flex items-center justify-center gap-2 px-3 py-2 bg-blue-500/20 text-blue-400 rounded hover:bg-blue-500/30 transition-all text-sm"
                      >
                        <Edit2 className="w-4 h-4" />
                        Edit
                      </button>
                      <button
                        onClick={(e) => {
                          e.stopPropagation()
                          handleDelete(device.id)
                        }}
                        className="flex-1 flex items-center justify-center gap-2 px-3 py-2 bg-red-500/20 text-red-400 rounded hover:bg-red-500/30 transition-all text-sm"
                      >
                        <Trash2 className="w-4 h-4" />
                        Delete
                      </button>
                    </div>
                  </div>
                </div>
                )
              })}
            </div>
          )}
        </>
      )}

      {/* Modal */}
      {showCreateModal && (
        <div className="fixed inset-0 bg-black/80 flex items-center justify-center z-50">
          <div className="bg-dark-800 border border-neon-cyan/20 rounded-xl p-8 max-w-md w-full mx-4">
            <h3 className="text-2xl font-bold text-white mb-6">
              {editingId ? 'Edit IoT Device' : 'Add New IoT Device'}
            </h3>

            <form onSubmit={handleSubmit} className="space-y-4">
              <div>
                <label className="block text-sm text-gray-400 mb-2">Device Name</label>
                <input
                  type="text"
                  required
                  value={formData.name}
                  onChange={(e) => setFormData({...formData, name: e.target.value})}
                  className="w-full bg-dark-900 border border-gray-700 rounded-lg px-4 py-2 text-white focus:border-neon-cyan outline-none"
                  placeholder="E.g., Room 1 Temperature"
                />
              </div>

              <div>
                <label className="block text-sm text-gray-400 mb-2">Device Type</label>
                <select
                  value={formData.device_type}
                  onChange={(e) => setFormData({...formData, device_type: e.target.value})}
                  className="w-full bg-dark-900 border border-gray-700 rounded-lg px-4 py-2 text-white focus:border-neon-cyan outline-none"
                >
                  <option value="temperature">🌡️  Temperature</option>
                  <option value="humidity">💧 Humidity</option>
                  <option value="soil_moisture">🌱 Soil Moisture</option>
                  <option value="light_intensity">☀️  Light Intensity</option>
                  <option value="pressure">📊 Pressure</option>
                </select>
              </div>

              <div>
                <label className="block text-sm text-gray-400 mb-2">Source ID (Unique)</label>
                <input
                  type="text"
                  required
                  value={formData.source}
                  onChange={(e) => setFormData({...formData, source: e.target.value})}
                  className="w-full bg-dark-900 border border-gray-700 rounded-lg px-4 py-2 text-white focus:border-neon-cyan outline-none font-mono"
                  placeholder="E.g., room1_temp_sensor"
                  disabled={!!editingId}
                  title="Cannot change source after creation"
                />
              </div>

              <div>
                <label className="block text-sm text-gray-400 mb-2">Location (Optional)</label>
                <input
                  type="text"
                  value={formData.location}
                  onChange={(e) => setFormData({...formData, location: e.target.value})}
                  className="w-full bg-dark-900 border border-gray-700 rounded-lg px-4 py-2 text-white focus:border-neon-cyan outline-none"
                  placeholder="E.g., Living Room"
                />
              </div>

              <div className="flex gap-3 pt-4">
                <button
                  type="button"
                  onClick={() => {
                    setShowCreateModal(false)
                    setEditingId(null)
                  }}
                  className="flex-1 px-4 py-2 bg-gray-700 text-white rounded-lg hover:bg-gray-600 transition-all"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={loading}
                  className="flex-1 px-4 py-2 bg-neon-cyan/20 text-neon-cyan border border-neon-cyan/40 rounded-lg hover:border-neon-cyan transition-all disabled:opacity-50"
                >
                  {loading ? 'Saving...' : editingId ? 'Update' : 'Create'}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* Chart Modal */}
      {showChartModal && selectedDeviceForChart && (
        <div className="fixed inset-0 bg-black/80 flex items-center justify-center z-50 p-4">
          <div className="bg-dark-800 border border-neon-cyan/20 rounded-xl p-8 max-w-4xl w-full mx-4">
            <div className="flex items-center justify-between mb-6">
              <div>
                <h3 className="text-2xl font-bold text-white mb-1">
                  {selectedDeviceForChart.name}
                </h3>
                <p className="text-sm text-gray-400">
                  Last 2 hours of {selectedDeviceForChart.device_type} data
                </p>
                {chartLastUpdated && (
                  <p className="text-xs text-gray-600 mt-1">
                    Updated: {chartLastUpdated.toLocaleTimeString()}
                  </p>
                )}
              </div>
              <button
                onClick={() => {
                  setShowChartModal(false)
                  setSelectedDeviceForChart(null)
                  setChartData([])
                  setChartStats({ average: 0, min: 0, max: 0 })
                }}
                className="p-2 hover:bg-dark-700 rounded-lg transition-all"
              >
                <X className="w-6 h-6 text-gray-400" />
              </button>
            </div>

            {chartLoading ? (
              <div className="flex items-center justify-center h-96">
                <p className="text-gray-400">Loading chart data...</p>
              </div>
            ) : chartData.length > 0 ? (
              <div className="space-y-6">
                {/* Statistics Cards */}
                <div className="grid grid-cols-3 gap-4">
                  <div className="bg-dark-900/50 border border-neon-cyan/30 rounded-lg p-4">
                    <p className="text-xs text-gray-400 mb-2">Average</p>
                    <p className="text-2xl font-bold text-neon-cyan">
                      {chartStats.average}
                      <span className="text-sm text-gray-400 ml-1">{getMetricUnit(selectedDeviceForChart.device_type)}</span>
                    </p>
                  </div>
                  <div className="bg-dark-900/50 border border-neon-green/30 rounded-lg p-4">
                    <p className="text-xs text-gray-400 mb-2">Minimum</p>
                    <p className="text-2xl font-bold text-neon-green">
                      {chartStats.min}
                      <span className="text-sm text-gray-400 ml-1">{getMetricUnit(selectedDeviceForChart.device_type)}</span>
                    </p>
                  </div>
                  <div className="bg-dark-900/50 border border-neon-orange/30 rounded-lg p-4">
                    <p className="text-xs text-gray-400 mb-2">Maximum</p>
                    <p className="text-2xl font-bold text-neon-orange">
                      {chartStats.max}
                      <span className="text-sm text-gray-400 ml-1">{getMetricUnit(selectedDeviceForChart.device_type)}</span>
                    </p>
                  </div>
                </div>

                {/* Chart */}
                <div className="bg-dark-900/50 rounded-lg p-6 border border-gray-700">
                  <ResponsiveContainer width="100%" height={400}>
                    <LineChart data={chartData} margin={{ top: 5, right: 30, left: 0, bottom: 5 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                      <XAxis 
                        dataKey="time" 
                        stroke="#9CA3AF"
                        tick={{ fontSize: 12 }}
                      />
                      <YAxis 
                        stroke="#9CA3AF"
                        tick={{ fontSize: 12 }}
                        label={{ value: getMetricUnit(selectedDeviceForChart.device_type), angle: -90, position: 'insideLeft' }}
                      />
                      <Tooltip 
                        contentStyle={{
                          backgroundColor: '#1a1a2e',
                          border: '1px solid #00d4ff',
                          borderRadius: '8px'
                        }}
                        labelStyle={{ color: '#fff' }}
                      />
                      <Legend />
                      <Line 
                        type="monotone" 
                        dataKey="value" 
                        stroke="#00d4ff" 
                        dot={false}
                        strokeWidth={2}
                        name={selectedDeviceForChart.device_type}
                      />
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              </div>
            ) : (
              <div className="flex items-center justify-center h-96">
                <p className="text-gray-400">No data available for this period</p>
              </div>
            )}

            <div className="flex gap-3 pt-6">
              <button
                onClick={() => {
                  setShowChartModal(false)
                  setSelectedDeviceForChart(null)
                  setChartData([])
                  setChartStats({ average: 0, min: 0, max: 0 })
                }}
                className="flex-1 px-4 py-2 bg-gray-700 text-white rounded-lg hover:bg-gray-600 transition-all"
              >
                Close
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Add Device Modal - for Users */}
      {!isAdmin && (
        <AddDeviceModal
          isOpen={showAddDeviceModal}
          onClose={() => setShowAddDeviceModal(false)}
          onAdd={handleAddDevice}
          isLoading={addingDevice}
        />
      )}

      {/* Edit Alert Thresholds Modal */}
      {!isAdmin && (
        <EditAlertThresholdsModal
          isOpen={showAlertThresholdsModal}
          onClose={() => setShowAlertThresholdsModal(false)}
          onUpdate={handleSaveAlertThresholds}
          device={selectedDeviceForAlert}
          isLoading={savingAlerts}
        />
      )}
    </div>
  )
}
