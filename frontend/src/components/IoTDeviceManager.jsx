import { useEffect, useMemo, useRef, useState } from 'react'
import {
  AlertCircle,
  Bot,
  Droplets,
  Edit3,
  Fan,
  Home,
  Mail,
  Plus,
  Send,
  Settings,
  Thermometer,
  Trash2,
  Wifi,
  X,
} from 'lucide-react'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'
import { useDevices } from '../context/DeviceContext'
import { useAuth } from '../context/AuthContext'
import AddDeviceModal from './AddDeviceModal'
import SensorAlertThresholdModal from './SensorAlertThresholdModal'
import SensorEditModal from './SensorEditModal'
import SensorWifiModal from './SensorWifiModal'
import SensorAiContextModal from './SensorAiContextModal'
import api from '../api'
import { formatVNTime } from '../utils/vnTime'

const getSensorId = (sensor) => sensor?.sensor_id || sensor?.source
const fmt = (value, suffix = '') => value === null || value === undefined ? '--' : `${Number(value).toFixed(1)}${suffix}`
const onlyTime = (value) => value ? formatVNTime(value).split(' ').pop() : '--'
const toNumberOrNull = (value) => {
  if (value === null || value === undefined || value === '') return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function isOutOfRange(value, minValue, maxValue) {
  const current = toNumberOrNull(value)
  const min = toNumberOrNull(minValue)
  const max = toNumberOrNull(maxValue)
  if (current === null || min === null || max === null) return false
  return current < min || current > max
}

function formatThreshold(minValue, maxValue, unit) {
  const min = toNumberOrNull(minValue)
  const max = toNumberOrNull(maxValue)
  if (min === null || max === null) return null
  return `${min.toFixed(1)} - ${max.toFixed(1)}${unit}`
}

function numericValues(rows, key) {
  return rows.map((row) => Number(row[key])).filter((value) => Number.isFinite(value))
}

function stat(rows, key, mode) {
  const values = numericValues(rows, key)
  if (values.length === 0) return null
  if (mode === 'min') return Math.min(...values)
  if (mode === 'max') return Math.max(...values)
  return values.reduce((sum, value) => sum + value, 0) / values.length
}

export default function IoTDeviceManager() {
  const { sensors, fetchSensors, createSensor, deleteSensor, loading } = useDevices()
  const { user } = useAuth()
  const [showAddModal, setShowAddModal] = useState(false)
  const [showGlobalSetting, setShowGlobalSetting] = useState(false)
  const [adding, setAdding] = useState(false)
  const [latestMap, setLatestMap] = useState({})
  const [selectedSensor, setSelectedSensor] = useState(null)
  const [history, setHistory] = useState([])
  const [forecast, setForecast] = useState([])
  const [openSettingId, setOpenSettingId] = useState(null)
  const [deviceStates, setDeviceStates] = useState({})
  const [toast, setToast] = useState(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [alertSensor, setAlertSensor] = useState(null)
  const [editSensor, setEditSensor] = useState(null)
  const [wifiSensor, setWifiSensor] = useState(null)
  const [aiSensor, setAiSensor] = useState(null)
  const [modalSaving, setModalSaving] = useState(false)
  const wsRef = useRef(null)

  const selectedSensorId = getSensorId(selectedSensor)

  const showToast = (message, tone = 'info') => {
    setToast({ message, tone })
    window.setTimeout(() => setToast(null), 4000)
  }

  const loadLatest = async (sensorList = sensors) => {
    if (!sensorList?.length) return
    const pairs = await Promise.all(sensorList.map(async (sensor) => {
      const sensorId = getSensorId(sensor)
      try {
        const res = await api.get(`/api/sensors/${sensorId}/latest`)
        return [sensorId, res.data]
      } catch {
        return [sensorId, sensor.latest_reading || null]
      }
    }))
    setLatestMap((prev) => ({ ...prev, ...Object.fromEntries(pairs) }))
  }

  const loadDetails = async (sensor, silent = false) => {
    if (!sensor) return
    if (!silent) setDetailLoading(true)
    const sensorId = getSensorId(sensor)
    try {
      const [historyRes, forecastRes] = await Promise.all([
        api.get(`/api/sensors/${sensorId}/history`, { params: { minutes: 120 } }).catch(() => ({ data: { readings: [] } })),
        api.get(`/api/sensors/${sensorId}/forecast`).catch(() => ({ data: { forecasts: [] } })),
      ])
      setHistory(historyRes.data?.readings || [])
      setForecast(forecastRes.data?.forecasts || [])
    } finally {
      if (!silent) setDetailLoading(false)
    }
  }

  useEffect(() => {
    loadLatest()
  }, [sensors])

  useEffect(() => {
    const interval = window.setInterval(() => {
      if (document.visibilityState === 'hidden') return
      loadLatest()
      if (selectedSensor) loadDetails(selectedSensor, true)
    }, 3000)
    return () => window.clearInterval(interval)
  }, [sensors, selectedSensor])

  useEffect(() => {
    const connect = () => {
      const useSameOriginApi = String(import.meta.env.VITE_USE_SAME_ORIGIN_API || 'false').toLowerCase() === 'true'
      const coreServerUrl = import.meta.env.VITE_CORE_SERVER_IP || import.meta.env.VITE_SERVER_IP || 'localhost'
      const corePort = import.meta.env.VITE_CORE_SERVER_PORT || import.meta.env.VITE_SERVER_PORT || '8000'
      const token = localStorage.getItem('access_token') || ''
      const wsProtocol = window.location.protocol === 'https:' ? 'wss' : 'ws'
      const wsHost = useSameOriginApi ? window.location.host : `${coreServerUrl}:${corePort}`
      const wsUrl = `${wsProtocol}://${wsHost}/api/ws/frontend_sensor_${Date.now()}?token=${encodeURIComponent(token)}`
      wsRef.current = new WebSocket(wsUrl)
      wsRef.current.onmessage = (event) => {
        try {
          const msg = JSON.parse(event.data)
          if (msg.type === 'sensor_reading' && msg.sensor_id) {
            setLatestMap((prev) => ({ ...prev, [msg.sensor_id]: { ...prev[msg.sensor_id], ...msg, timestamp: msg.timestamp || msg.event_ts } }))
            if (selectedSensorId === msg.sensor_id) loadDetails(selectedSensor, true)
          }
          if (msg.type === 'iot_metric') {
            setLatestMap((prev) => {
              const current = prev[msg.source] || { sensor_id: msg.source }
              return { ...prev, [msg.source]: { ...current, [msg.metric_type]: msg.value, timestamp: msg.timestamp } }
            })
          }
        } catch (err) {
          console.error('Failed to parse websocket payload:', err)
        }
      }
      wsRef.current.onclose = () => window.setTimeout(connect, 3000)
    }
    connect()
    return () => wsRef.current?.close()
  }, [selectedSensorId])

  const handleAdd = async (payload) => {
    setAdding(true)
    try {
      await createSensor(payload)
      showToast('Đã thêm thiết bị IoT.')
    } finally {
      setAdding(false)
    }
  }

  const handleDelete = async (sensor) => {
    const sensorId = getSensorId(sensor)
    if (!window.confirm(`Delete sensor ${sensorId}?`)) return
    await deleteSensor(sensorId)
    if (selectedSensorId === sensorId) setSelectedSensor(null)
    showToast('Đã xóa thiết bị.')
  }

  const openDetails = async (sensor) => {
    setSelectedSensor(sensor)
    await loadDetails(sensor)
  }

  const syncVirtualSensor = async (sensor) => {
    const sensorId = getSensorId(sensor)
    try {
      const res = await api.post(`/api/sensors/${sensorId}/sync-meteostat`, null, { params: { hours: 24 } })
      if (res.data?.latest_reading) {
        setLatestMap((prev) => ({ ...prev, [sensorId]: res.data.latest_reading }))
      }
      showToast(res.data?.message || 'Đã đồng bộ Virtual IoT.')
      await fetchSensors()
    } catch (err) {
      showToast(err.response?.data?.detail || 'Không đồng bộ được Virtual IoT.', 'error')
    }
  }

  const applySensorPatch = async (sensor, payload, successMessage = 'Saved') => {
    const sensorId = getSensorId(sensor)
    const res = await api.patch(`/api/sensors/${sensorId}`, payload)
    showToast(successMessage)
    await fetchSensors()
    if (selectedSensorId === sensorId) {
      setSelectedSensor(res.data)
      await loadDetails(res.data, true)
    }
    return res.data
  }

  const openSettingModal = (kind, sensor) => {
    setOpenSettingId(null)
    if (kind === 'alerts') setAlertSensor(sensor)
    if (kind === 'edit') setEditSensor(sensor)
    if (kind === 'wifi') setWifiSensor(sensor)
    if (kind === 'ai') setAiSensor(sensor)
  }

  const handleSaveAlertThresholds = async (payload) => {
    if (!alertSensor) return
    setModalSaving(true)
    try {
      await applySensorPatch(alertSensor, payload, 'Đã lưu ngưỡng cảnh báo')
      setAlertSensor(null)
    } catch (err) {
      showToast(err.response?.data?.detail || 'Không lưu được ngưỡng cảnh báo', 'error')
      throw err
    } finally {
      setModalSaving(false)
    }
  }

  const handleSaveDeviceEdit = async (payload) => {
    if (!editSensor) return
    setModalSaving(true)
    try {
      await applySensorPatch(editSensor, payload, 'Device updated')
      setEditSensor(null)
    } catch (err) {
      showToast(err.response?.data?.detail || 'Could not update device', 'error')
      throw err
    } finally {
      setModalSaving(false)
    }
  }

  const handleSaveAiContext = async (payload) => {
    if (!aiSensor) return
    setModalSaving(true)
    try {
      await applySensorPatch(aiSensor, payload, 'Saved AI context')
      setAiSensor(null)
    } catch (err) {
      showToast(err.response?.data?.detail || 'Could not save AI context', 'error')
      throw err
    } finally {
      setModalSaving(false)
    }
  }

  const setPhysicalDeviceState = async (sensor, key, value) => {
    const sensorId = getSensorId(sensor)
    if (sensor.source_type === 'virtual_meteostat') {
      showToast('Virtual sensor cannot receive hardware commands.', 'warning')
      return
    }

    const payload = key === 'mist' ? { fog: Boolean(value) } : { [key]: Boolean(value) }
    try {
      await api.post(`/api/devices/${sensorId}/manual-command`, payload)
      setDeviceStates((prev) => ({
        ...prev,
        [sensorId]: { ...(prev[sensorId] || {}), [key]: value },
      }))
      showToast(`${value ? 'Bật' : 'Tắt'} ${key === 'fan' ? 'quạt' : 'phun sương'} cho ${sensor.name}.`)
    } catch (err) {
      showToast(err.response?.data?.detail || 'Could not send manual command', 'error')
    }
  }
  const chartData = useMemo(() => {
    const actual = (history || []).map((row) => ({
      time: onlyTime(row.timestamp || row.event_ts),
      timestamp: row.timestamp || row.event_ts,
      temperature: row.temperature,
      humidity: row.humidity,
      forecast_temperature: null,
      forecast_humidity: null,
    }))
    const predicted = (forecast || []).slice(0, 12).map((row) => ({
      time: onlyTime(row.forecast_ts || row.timestamp || row.event_ts),
      timestamp: row.forecast_ts || row.timestamp || row.event_ts,
      temperature: null,
      humidity: null,
      forecast_temperature: row.temperature ?? row.predicted_temperature ?? (row.target === 'temperature' ? row.predicted_value : null),
      forecast_humidity: row.humidity ?? row.predicted_humidity ?? (row.target === 'humidity' ? row.predicted_value : null),
    }))
    return [...actual, ...predicted].sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp))
  }, [history, forecast])

  return (
    <div className="min-h-screen bg-dark-900 p-8">
      <div className="flex items-start justify-between gap-4 mb-10">
        <div>
          <h1 className="text-4xl font-bold text-white mb-2">IoT Devices</h1>
          <p className="text-gray-400">Manage your IoT sensors and devices</p>
        </div>
        <div className="flex gap-3">
          <button onClick={() => setShowGlobalSetting(true)} className="inline-flex items-center gap-2 px-5 py-3 bg-indigo-500/20 border border-indigo-400/40 text-indigo-200 rounded-lg hover:border-indigo-300">
            <Settings className="w-5 h-5" /> Setting
          </button>
          {user?.role !== 'admin' && (
            <button onClick={() => setShowAddModal(true)} className="inline-flex items-center gap-2 px-5 py-3 bg-neon-cyan/20 border border-neon-cyan/40 text-neon-cyan rounded-lg hover:border-neon-cyan">
              <Plus className="w-5 h-5" /> Add Device
            </button>
          )}
        </div>
      </div>

      {toast && (
        <div className={`mb-5 rounded-lg border px-4 py-3 text-sm ${toast.tone === 'error' ? 'border-red-400/40 bg-red-500/10 text-red-200' : toast.tone === 'warning' ? 'border-yellow-400/40 bg-yellow-500/10 text-yellow-200' : 'border-neon-cyan/30 bg-neon-cyan/10 text-neon-cyan'}`}>
          {toast.message}
        </div>
      )}

      {loading ? (
        <div className="text-gray-400">Loading devices...</div>
      ) : sensors.length === 0 ? (
        <EmptyState onAdd={() => setShowAddModal(true)} />
      ) : (
        <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-5">
          {sensors.map((sensor) => {
            const sensorId = getSensorId(sensor)
            const latest = latestMap[sensorId] || sensor.latest_reading || {}
            const isVirtual = sensor.source_type === 'virtual_meteostat'
            const state = deviceStates[sensorId] || {}
            const tempThreshold = formatThreshold(sensor.temperature_min_threshold, sensor.temperature_max_threshold, '°C')
            const humidityThreshold = formatThreshold(sensor.humidity_min_threshold, sensor.humidity_max_threshold, '%')
            const hasThresholds = Boolean(tempThreshold || humidityThreshold)
            const tempOutOfRange = isOutOfRange(latest.temperature, sensor.temperature_min_threshold, sensor.temperature_max_threshold)
            const humidityOutOfRange = isOutOfRange(latest.humidity, sensor.humidity_min_threshold, sensor.humidity_max_threshold)
            const hasAlertViolation = Boolean(sensor.alert_enabled) && (tempOutOfRange || humidityOutOfRange)
            return (
              <div key={sensorId} className={`relative rounded-xl border p-5 transition-all ${
                hasAlertViolation
                  ? 'bg-red-950/20 border-red-400/70 shadow-[0_0_0_1px_rgba(248,113,113,0.4)]'
                  : 'bg-dark-800 border-gray-800 hover:border-neon-cyan/40'
              }`}>
                <button type="button" onClick={() => openDetails(sensor)} className="block w-full text-left">
                  <div className="flex items-start justify-between gap-3 mb-4">
                    <div>
                      <h3 className="text-lg font-bold text-white">{sensor.name}</h3>
                      <p className="text-xs text-gray-500 font-mono mt-2">Source: {sensorId}</p>
                      <p className="text-xs text-gray-500">Created by: {user?.username || 'user'}</p>
                    </div>
                    <div className="flex flex-col items-end gap-2">
                      <span className="px-3 py-1 rounded bg-orange-500/20 text-orange-300 text-xs">temperature</span>
                      <span className="px-3 py-1 rounded bg-sky-500/20 text-sky-200 text-xs">humidity</span>
                    </div>
                  </div>

                  <div className="flex items-center gap-2 text-gray-400 mb-4">
                    <Home className="w-4 h-4" />
                    <span>{sensor.location || sensor.location_province || 'nha'}</span>
                  </div>
                  <div className="mb-4">
                    <span className="inline-block px-3 py-1 rounded border border-neon-cyan/30 bg-neon-cyan/10 text-neon-cyan text-sm">
                      {isVirtual ? 'Virtual IoT' : sensor.environment_type === 'outdoor' ? 'Ngoài trời' : 'Trong nhà'}
                    </span>
                  </div>

                  {Boolean(sensor.alert_enabled) && hasThresholds && (
                    <div className={`mb-4 rounded-lg border px-3 py-2 text-xs ${
                      hasAlertViolation
                        ? 'border-red-400/50 bg-red-500/10 text-red-200'
                        : 'border-cyan-400/30 bg-cyan-500/10 text-cyan-100'
                    }`}>
                      <p className="font-semibold">{hasAlertViolation ? 'Đang vượt ngưỡng cảnh báo' : 'Ngưỡng cảnh báo đang bật'}</p>
                      <div className="mt-1 flex flex-wrap gap-2">
                        {tempThreshold && <span className="rounded bg-black/25 px-2 py-1">Nhiệt độ: {tempThreshold}</span>}
                        {humidityThreshold && <span className="rounded bg-black/25 px-2 py-1">Độ ẩm: {humidityThreshold}</span>}
                      </div>
                    </div>
                  )}

                  <div className="rounded-lg border border-neon-cyan/20 bg-dark-900 p-4 mb-4">
                    <p className="text-gray-400 text-sm mb-3">Real-time Value</p>
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <div className="flex items-baseline gap-2">
                          <span className="text-3xl font-bold text-neon-cyan">{fmt(latest.temperature)}</span>
                          <span className="text-gray-300">°C</span>
                        </div>
                        <p className="text-xs text-orange-200 mt-1">Temperature</p>
                      </div>
                      <div>
                        <div className="flex items-baseline gap-2">
                          <span className="text-3xl font-bold text-sky-300">{fmt(latest.humidity)}</span>
                          <span className="text-gray-300">%</span>
                        </div>
                        <p className="text-xs text-sky-200 mt-1">Humidity</p>
                      </div>
                    </div>
                    <p className="text-gray-500 text-xs mt-3">Updated: {onlyTime(latest.timestamp || latest.event_ts)}</p>
                  </div>
                </button>

                <div className={`mb-4 flex items-center gap-2 ${hasAlertViolation ? 'text-red-300' : 'text-neon-green'}`}>
                  <span className={`w-3 h-3 rounded-full animate-pulse ${hasAlertViolation ? 'bg-red-400' : 'bg-neon-green'}`} />
                  <span>{hasAlertViolation ? 'Alert' : 'Active'}</span>
                </div>

                <div className="grid grid-cols-2 gap-3">
                  <button onClick={() => setOpenSettingId(openSettingId === sensorId ? null : sensorId)} className="inline-flex items-center justify-center gap-2 px-4 py-2.5 bg-neon-cyan/20 border border-neon-cyan/30 text-neon-cyan rounded hover:border-neon-cyan">
                    <Settings className="w-5 h-5" /> Setting
                  </button>
                  {user?.role !== 'admin' && (
                    <button onClick={() => handleDelete(sensor)} className="inline-flex items-center justify-center gap-2 px-4 py-2.5 bg-red-500/20 border border-red-400/30 text-red-300 rounded hover:border-red-300">
                      <Trash2 className="w-5 h-5" /> Delete
                    </button>
                  )}
                </div>

                {openSettingId === sensorId && (
                  <SensorSettingMenu
                    sensor={sensor}
                    state={state}
                    onVirtualSync={() => syncVirtualSensor(sensor)}
                    onOpenAlerts={() => openSettingModal('alerts', sensor)}
                    onOpenEdit={() => openSettingModal('edit', sensor)}
                    onOpenWifi={() => openSettingModal('wifi', sensor)}
                    onOpenAiContext={() => openSettingModal('ai', sensor)}
                    onToggleFan={(value) => setPhysicalDeviceState(sensor, 'fan', value)}
                    onToggleMist={(value) => setPhysicalDeviceState(sensor, 'mist', value)}
                  />
                )}
              </div>
            )
          })}
        </div>
      )}

      {selectedSensor && (
        <SensorChartModal
          sensor={selectedSensor}
          latest={latestMap[selectedSensorId] || selectedSensor.latest_reading || {}}
          history={history}
          forecast={forecast}
          chartData={chartData}
          loading={detailLoading}
          onClose={() => setSelectedSensor(null)}
        />
      )}

      {showGlobalSetting && <NotificationSettingsModal onClose={() => setShowGlobalSetting(false)} onMessage={showToast} />}
      <AddDeviceModal isOpen={showAddModal} onClose={() => setShowAddModal(false)} onAdd={handleAdd} isLoading={adding} />
      <SensorAlertThresholdModal
        isOpen={Boolean(alertSensor)}
        sensor={alertSensor}
        isLoading={modalSaving}
        onClose={() => setAlertSensor(null)}
        onSave={handleSaveAlertThresholds}
      />
      <SensorEditModal
        isOpen={Boolean(editSensor)}
        sensor={editSensor}
        isLoading={modalSaving}
        onClose={() => setEditSensor(null)}
        onSave={handleSaveDeviceEdit}
      />
      <SensorWifiModal
        isOpen={Boolean(wifiSensor)}
        sensor={wifiSensor}
        onClose={() => setWifiSensor(null)}
        onMessage={showToast}
      />
      <SensorAiContextModal
        isOpen={Boolean(aiSensor)}
        sensor={aiSensor}
        isLoading={modalSaving}
        onClose={() => setAiSensor(null)}
        onSave={handleSaveAiContext}
        onMessage={showToast}
      />
    </div>
  )
}

function SensorSettingMenu({
  sensor,
  state,
  onVirtualSync,
  onOpenAlerts,
  onOpenEdit,
  onOpenWifi,
  onOpenAiContext,
  onToggleFan,
  onToggleMist,
}) {
  const isVirtual = sensor.source_type === 'virtual_meteostat'
  return (
    <div className="absolute left-6 right-6 top-[calc(100%-5.5rem)] z-20 rounded-xl border border-neon-cyan/30 bg-dark-900 p-5 shadow-2xl">
      <div className="space-y-4 text-lg">
        <button onClick={onOpenAlerts} className="flex items-center gap-3 text-yellow-300 hover:text-yellow-200">
          <AlertCircle className="w-5 h-5" /> Alerts
        </button>
        <button onClick={onOpenEdit} className="flex items-center gap-3 text-blue-200 hover:text-blue-100">
          <Edit3 className="w-5 h-5" /> Edit
        </button>
        <button onClick={onOpenWifi} className="flex items-center gap-3 text-sky-300 hover:text-sky-200">
          <Wifi className="w-5 h-5" /> WiFi
        </button>
        <button onClick={onOpenAiContext} className="flex items-center gap-3 text-neon-cyan hover:text-cyan-200">
          <Bot className="w-5 h-5" /> AI Context
        </button>
      </div>

      <div className="border-t border-gray-700 my-5" />
      <p className="text-orange-300 text-sm tracking-wide mb-4">TEST THIẾT BỊ</p>
      {isVirtual && (
        <button onClick={onVirtualSync} className="w-full mb-4 px-4 py-3 rounded-lg border border-sky-400/30 bg-sky-500/10 text-sky-200 hover:border-sky-300">
          Sync dữ liệu Virtual IoT
        </button>
      )}
      <div className="grid grid-cols-2 gap-3 mb-3">
        <button onClick={() => onToggleFan(true)} className={`rounded-lg border px-4 py-3 ${state.fan ? 'border-green-300 bg-green-500/20 text-green-100' : 'border-green-400/30 bg-green-500/10 text-green-200'}`}>
          <Fan className="w-5 h-5 mx-auto mb-1" /> Bật quạt
        </button>
        <button onClick={() => onToggleFan(false)} className="rounded-lg border border-red-400/30 bg-red-500/10 px-4 py-3 text-red-200">
          Tắt quạt
        </button>
      </div>
      <div className="grid grid-cols-2 gap-3">
        <button onClick={() => onToggleMist(true)} className={`rounded-lg border px-4 py-3 ${state.mist ? 'border-sky-300 bg-sky-500/20 text-sky-100' : 'border-sky-400/30 bg-sky-500/10 text-sky-200'}`}>
          <Droplets className="w-5 h-5 mx-auto mb-1" /> Bật phun
        </button>
        <button onClick={() => onToggleMist(false)} className="rounded-lg border border-red-400/30 bg-red-500/10 px-4 py-3 text-red-200">
          Tắt phun
        </button>
      </div>
    </div>
  )
}

function SensorChartModal({ sensor, latest, history, forecast, chartData, loading, onClose }) {
  const tempAvg = stat(history, 'temperature', 'avg')
  const tempMin = stat(history, 'temperature', 'min')
  const tempMax = stat(history, 'temperature', 'max')
  const humAvg = stat(history, 'humidity', 'avg')
  const humMin = stat(history, 'humidity', 'min')
  const humMax = stat(history, 'humidity', 'max')

  return (
    <div className="fixed inset-0 bg-black/80 z-50 p-6 overflow-y-auto">
      <div className="bg-dark-800 border border-neon-cyan/30 rounded-xl p-8 max-w-7xl mx-auto">
        <div className="flex justify-between items-start mb-8">
          <div>
            <h2 className="text-3xl font-bold text-white mb-2">{sensor.name}</h2>
            <p className="text-gray-400">Last 2 hours of temperature and humidity data</p>
            <p className="text-yellow-300 text-sm mt-2">Includes short forecast when Databricks forecast rows are available</p>
            <p className="text-gray-500 text-sm mt-2">Forecast points: {forecast.length}</p>
            <p className="text-gray-500 text-sm">Updated: {latest.timestamp || latest.event_ts ? formatVNTime(latest.timestamp || latest.event_ts, true) : '--'}</p>
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-white">
            <X className="w-8 h-8" />
          </button>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-5 mb-8">
          <StatBox label="Average" temp={tempAvg} humidity={humAvg} color="cyan" />
          <StatBox label="Minimum" temp={tempMin} humidity={humMin} color="green" />
          <StatBox label="Maximum" temp={tempMax} humidity={humMax} color="orange" />
        </div>

        <div className="rounded-lg border border-gray-700 bg-dark-900/40 p-5">
          {loading ? (
            <div className="h-[440px] flex items-center justify-center text-gray-400">Loading chart...</div>
          ) : chartData.length === 0 ? (
            <div className="h-[440px] flex items-center justify-center text-gray-400">No realtime data yet.</div>
          ) : (
            <ResponsiveContainer width="100%" height={440}>
              <LineChart data={chartData}>
                <CartesianGrid strokeDasharray="4 4" stroke="#2a365d" />
                <XAxis dataKey="time" stroke="#9CA3AF" tick={{ fontSize: 12 }} />
                <YAxis yAxisId="temp" stroke="#9CA3AF" tick={{ fontSize: 12 }} label={{ value: '°C', angle: -90, position: 'insideLeft', fill: '#9CA3AF' }} />
                <YAxis yAxisId="hum" orientation="right" stroke="#9CA3AF" tick={{ fontSize: 12 }} label={{ value: '%', angle: 90, position: 'insideRight', fill: '#9CA3AF' }} />
                <Tooltip contentStyle={{ backgroundColor: '#111827', border: '1px solid #00f0ff', borderRadius: 8 }} />
                <Legend />
                <Line yAxisId="temp" type="monotone" dataKey="temperature" stroke="#00f0ff" strokeWidth={3} dot={false} name="temperature (actual)" />
                <Line yAxisId="hum" type="monotone" dataKey="humidity" stroke="#38bdf8" strokeWidth={3} dot={false} name="humidity (actual)" />
                <Line yAxisId="temp" type="monotone" dataKey="forecast_temperature" stroke="#ffd400" strokeWidth={3} dot={false} strokeDasharray="6 5" name="temperature forecast" />
                <Line yAxisId="hum" type="monotone" dataKey="forecast_humidity" stroke="#ff7ab6" strokeWidth={3} dot={false} strokeDasharray="6 5" name="humidity forecast" />
              </LineChart>
            </ResponsiveContainer>
          )}
        </div>

        <button onClick={onClose} className="w-full mt-8 px-5 py-3 bg-gray-600 text-white rounded-lg hover:bg-gray-500">Close</button>
      </div>
    </div>
  )
}

function StatBox({ label, temp, humidity, color }) {
  const border = color === 'green' ? 'border-green-400/40' : color === 'orange' ? 'border-orange-400/40' : 'border-neon-cyan/40'
  const text = color === 'green' ? 'text-neon-green' : color === 'orange' ? 'text-neon-orange' : 'text-neon-cyan'
  return (
    <div className={`rounded-lg border ${border} bg-dark-900/40 p-5`}>
      <p className="text-gray-400 mb-3">{label}</p>
      <div className="grid grid-cols-2 gap-3">
        <div>
          <p className={`text-3xl font-bold ${text}`}>{fmt(temp, ' °C')}</p>
          <p className="text-xs text-gray-500 mt-1">Temperature</p>
        </div>
        <div>
          <p className="text-3xl font-bold text-sky-300">{fmt(humidity, '%')}</p>
          <p className="text-xs text-gray-500 mt-1">Humidity</p>
        </div>
      </div>
    </div>
  )
}

function EmptyState({ onAdd }) {
  return (
    <div className="rounded-xl border border-dashed border-gray-700 bg-dark-800/50 p-10 text-center">
      <Thermometer className="w-10 h-10 text-neon-cyan mx-auto mb-3" />
      <h3 className="text-white text-xl font-bold mb-2">No IoT devices yet</h3>
      <p className="text-gray-400 mb-5">Add a physical ESP32 sensor or a virtual IoT sensor.</p>
      <button onClick={onAdd} className="inline-flex items-center gap-2 px-5 py-3 bg-neon-cyan/20 border border-neon-cyan/40 text-neon-cyan rounded-lg hover:border-neon-cyan">
        <Plus className="w-5 h-5" /> Add Device
      </button>
    </div>
  )
}

function NotificationSettingsModal({ onClose, onMessage }) {
  const [telegramValue, setTelegramValue] = useState('')
  const [emailValue, setEmailValue] = useState('')
  const [targets, setTargets] = useState([])
  const [loadingTargets, setLoadingTargets] = useState(false)
  const [busyId, setBusyId] = useState(null)

  const telegramTargets = targets.filter((target) => target.target_type === 'telegram')
  const emailTargets = targets.filter((target) => target.target_type === 'email')

  const loadTargets = async () => {
    setLoadingTargets(true)
    try {
      const res = await api.get('/api/auth/notifications/targets')
      setTargets(res.data?.targets || [])
    } catch (err) {
      onMessage?.(err.response?.data?.detail || 'Could not load notification targets.', 'error')
    } finally {
      setLoadingTargets(false)
    }
  }

  useEffect(() => {
    loadTargets()
  }, [])

  const addTarget = async (targetType) => {
    const value = targetType === 'telegram' ? telegramValue.trim() : emailValue.trim()
    if (!value) {
      onMessage?.(targetType === 'telegram' ? 'Please enter Telegram Chat ID.' : 'Please enter email.', 'warning')
      return
    }
    try {
      await api.post('/api/auth/notifications/targets', {
        target_type: targetType,
        target_value: value,
      })
      if (targetType === 'telegram') setTelegramValue('')
      else setEmailValue('')
      await loadTargets()
      onMessage?.(targetType === 'telegram' ? 'Telegram Chat ID added.' : 'Email added.')
    } catch (err) {
      onMessage?.(err.response?.data?.detail || 'Could not add notification target.', 'error')
    }
  }

  const toggleTarget = async (target) => {
    setBusyId(target.id)
    try {
      await api.patch(`/api/auth/notifications/targets/${target.id}`, {
        is_enabled: !target.is_enabled,
      })
      await loadTargets()
    } catch (err) {
      onMessage?.(err.response?.data?.detail || 'Could not update target.', 'error')
    } finally {
      setBusyId(null)
    }
  }

  const deleteTarget = async (target) => {
    setBusyId(target.id)
    try {
      await api.delete(`/api/auth/notifications/targets/${target.id}`)
      await loadTargets()
      onMessage?.('Notification target deleted.')
    } catch (err) {
      onMessage?.(err.response?.data?.detail || 'Could not delete target.', 'error')
    } finally {
      setBusyId(null)
    }
  }

  const testTarget = async (target) => {
    setBusyId(target.id)
    try {
      await api.post(`/api/auth/notifications/targets/${target.id}/test`)
      onMessage?.('Test notification sent.')
    } catch (err) {
      onMessage?.(err.response?.data?.detail || 'Could not send test notification.', 'error')
    } finally {
      setBusyId(null)
    }
  }

  return (
    <div className="fixed inset-0 bg-black/70 z-50 p-6 overflow-y-auto">
      <div className="bg-dark-800 border border-indigo-400/30 rounded-xl p-6 max-w-7xl mx-auto">
        <div className="flex items-center justify-between mb-8">
          <h2 className="text-3xl font-bold text-white">Notification Settings</h2>
          <button onClick={onClose} className="text-gray-400 hover:text-white"><X className="w-7 h-7" /></button>
        </div>

        {loadingTargets && <p className="text-gray-400 mb-4">Loading notification targets...</p>}

        <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
          <section className="rounded-xl border border-neon-cyan/30 bg-dark-900/50 p-5">
            <div className="flex flex-wrap items-center justify-between gap-3 mb-5">
              <div className="flex items-center gap-2">
                <Send className="w-5 h-5 text-neon-cyan" />
                <h3 className="text-2xl font-bold text-neon-cyan">Telegram</h3>
              </div>
              <button
                onClick={() => window.open('https://t.me/userinfobot', '_blank', 'noopener,noreferrer')}
                className="px-4 py-2 rounded-lg border border-neon-cyan/40 bg-neon-cyan/10 text-neon-cyan hover:border-neon-cyan"
              >
                Huong dan tim Chat ID
              </button>
            </div>

            <input
              value={telegramValue}
              onChange={(e) => setTelegramValue(e.target.value)}
              placeholder="Enter Telegram Chat ID"
              className="w-full bg-dark-800 border border-gray-600 rounded-lg px-4 py-4 text-white text-lg focus:outline-none focus:border-neon-cyan"
            />
            <button
              onClick={() => addTarget('telegram')}
              className="w-full mt-4 px-4 py-4 rounded-lg border border-neon-cyan/40 bg-neon-cyan/20 text-neon-cyan text-xl hover:border-neon-cyan"
            >
              Add Telegram Chat ID
            </button>

            <TargetList
              targets={telegramTargets}
              busyId={busyId}
              onToggle={toggleTarget}
              onDelete={deleteTarget}
              onTest={testTarget}
              hideTest
            />
          </section>

          <section className="rounded-xl border border-green-400/30 bg-dark-900/50 p-5">
            <div className="flex items-center gap-2 mb-6">
              <Mail className="w-5 h-5 text-green-300" />
              <h3 className="text-2xl font-bold text-green-300">Email</h3>
            </div>
            <input
              type="email"
              value={emailValue}
              onChange={(e) => setEmailValue(e.target.value)}
              placeholder="Enter email for alerts"
              className="w-full bg-dark-800 border border-gray-600 rounded-lg px-4 py-4 text-white text-lg focus:outline-none focus:border-green-300"
            />
            <button
              onClick={() => addTarget('email')}
              className="w-full mt-4 px-4 py-4 rounded-lg border border-green-400/40 bg-green-500/20 text-green-200 text-xl hover:border-green-300"
            >
              Add Email
            </button>

            <TargetList
              targets={emailTargets}
              busyId={busyId}
              onToggle={toggleTarget}
              onDelete={deleteTarget}
              onTest={testTarget}
            />
          </section>
        </div>
      </div>
    </div>
  )
}

function TargetList({ targets, busyId, onToggle, onDelete, onTest, hideTest = false }) {
  if (!targets.length) {
    return <p className="mt-5 text-gray-500 text-sm">No targets added yet.</p>
  }

  return (
    <div className="mt-5 space-y-3">
      {targets.map((target) => (
        <div key={target.id} className="flex flex-wrap items-center gap-3 rounded-lg bg-dark-800 px-4 py-3">
          <span className="flex-1 min-w-[220px] text-white text-lg break-all">{target.target_value}</span>
          <label className="inline-flex items-center gap-2 text-gray-200">
            <input
              type="checkbox"
              checked={Boolean(target.is_enabled)}
              disabled={busyId === target.id}
              onChange={() => onToggle(target)}
              className="w-5 h-5 accent-neon-cyan"
            />
            Enable
          </label>
          {!hideTest && (
            <button disabled={busyId === target.id} onClick={() => onTest(target)} className="text-neon-cyan hover:text-cyan-200 disabled:opacity-50">
              Test
            </button>
          )}
          <button disabled={busyId === target.id} onClick={() => onDelete(target)} className="text-red-300 hover:text-red-200 disabled:opacity-50">
            Delete
          </button>
        </div>
      ))}
    </div>
  )
}

