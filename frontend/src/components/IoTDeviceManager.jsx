import { useEffect, useMemo, useRef, useState } from 'react'
import { Plus, RefreshCcw, Trash2, Thermometer, Droplets, Cloud, Cpu, Database, Award } from 'lucide-react'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'
import { useDevices } from '../context/DeviceContext'
import { useAuth } from '../context/AuthContext'
import AddDeviceModal from './AddDeviceModal'
import api from '../api'
import { formatVNTime } from '../utils/vnTime'

const getSensorId = (sensor) => sensor?.sensor_id || sensor?.source
const fmt = (value, suffix = '') => value === null || value === undefined ? '--' : `${Number(value).toFixed(1)}${suffix}`

export default function IoTDeviceManager() {
  const { sensors, fetchSensors, createSensor, deleteSensor, loading } = useDevices()
  const { user } = useAuth()
  const [showAddModal, setShowAddModal] = useState(false)
  const [adding, setAdding] = useState(false)
  const [latestMap, setLatestMap] = useState({})
  const [selectedSensor, setSelectedSensor] = useState(null)
  const [history, setHistory] = useState([])
  const [forecast, setForecast] = useState([])
  const [leaderboard, setLeaderboard] = useState([])
  const [dbxStatus, setDbxStatus] = useState(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const wsRef = useRef(null)
  const sensorsRef = useRef([])

  useEffect(() => { sensorsRef.current = sensors || [] }, [sensors])

  const selectedSensorId = getSensorId(selectedSensor)

  useEffect(() => {
    api.get('/api/sensors/databricks/status')
      .then((res) => setDbxStatus(res.data))
      .catch(() => setDbxStatus({ configured: false, enabled: false }))
  }, [])

  useEffect(() => {
    const loadLatest = async () => {
      const pairs = await Promise.all((sensors || []).map(async (sensor) => {
        const sensorId = getSensorId(sensor)
        try {
          const res = await api.get(`/api/sensors/${sensorId}/latest`)
          return [sensorId, res.data]
        } catch {
          return [sensorId, sensor.latest_reading || null]
        }
      }))
      setLatestMap(Object.fromEntries(pairs))
    }
    if (sensors?.length) loadLatest()
  }, [sensors])

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
            setLatestMap((prev) => ({ ...prev, [msg.sensor_id]: { ...prev[msg.sensor_id], ...msg, event_ts: msg.timestamp } }))
          }
          if (msg.type === 'iot_metric') {
            setLatestMap((prev) => {
              const current = prev[msg.source] || { sensor_id: msg.source }
              return {
                ...prev,
                [msg.source]: {
                  ...current,
                  [msg.metric_type]: msg.value,
                  event_ts: msg.timestamp,
                  timestamp: msg.timestamp,
                },
              }
            })
          }
        } catch (err) {
          console.error('Failed to parse websocket payload:', err)
        }
      }
      wsRef.current.onclose = () => setTimeout(connect, 3000)
    }
    connect()
    return () => wsRef.current?.close()
  }, [])

  const handleAdd = async (payload) => {
    setAdding(true)
    try { await createSensor(payload) } finally { setAdding(false) }
  }

  const handleDelete = async (sensor) => {
    const sensorId = getSensorId(sensor)
    if (!window.confirm(`Delete sensor ${sensorId}?`)) return
    await deleteSensor(sensorId)
    if (selectedSensorId === sensorId) setSelectedSensor(null)
  }

  const openDetails = async (sensor) => {
    setSelectedSensor(sensor)
    setDetailLoading(true)
    const sensorId = getSensorId(sensor)
    try {
      const [historyRes, forecastRes, leaderboardRes] = await Promise.all([
        api.get(`/api/sensors/${sensorId}/history`, { params: { minutes: 24 * 60 } }).catch(() => ({ data: { readings: [] } })),
        api.get(`/api/sensors/${sensorId}/forecast`).catch(() => ({ data: { forecasts: [] } })),
        api.get(`/api/sensors/${sensorId}/model-leaderboard`).catch(() => ({ data: { models: [] } })),
      ])
      setHistory(historyRes.data?.readings || [])
      setForecast(forecastRes.data?.forecasts || [])
      setLeaderboard(leaderboardRes.data?.models || [])
    } finally {
      setDetailLoading(false)
    }
  }

  const chartData = useMemo(() => {
    const actual = (history || []).map((row) => ({
      time: formatVNTime(row.timestamp || row.event_ts),
      timestamp: row.timestamp || row.event_ts,
      temperature: row.temperature,
      humidity: row.humidity,
      forecast_temperature: null,
      forecast_humidity: null,
    }))
    const predicted = (forecast || []).map((row) => ({
      time: formatVNTime(row.forecast_ts || row.timestamp || row.event_ts),
      timestamp: row.forecast_ts || row.timestamp || row.event_ts,
      temperature: null,
      humidity: null,
      forecast_temperature: row.temperature ?? row.predicted_temperature ?? (row.target === 'temperature' ? row.predicted_value : null),
      forecast_humidity: row.humidity ?? row.predicted_humidity ?? (row.target === 'humidity' ? row.predicted_value : null),
    }))
    return [...actual, ...predicted].sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp))
  }, [history, forecast])

  const bestModels = leaderboard.filter((row) => row.is_best === true || row.is_best === 1 || row.is_best === 'true')

  return (
    <div className="min-h-screen bg-dark-900 p-8">
      <div className="flex items-start justify-between gap-4 mb-8">
        <div>
          <h1 className="text-4xl font-bold text-white mb-2">Sensor Lakehouse Dashboard</h1>
          <p className="text-gray-400">One sensor card contains both temperature and humidity. Databricks owns model training and forecast results.</p>
          <div className="flex flex-wrap gap-2 mt-4 text-xs">
            <span className={`px-3 py-1 rounded-full border ${dbxStatus?.configured ? 'border-green-400/40 text-green-300 bg-green-500/10' : 'border-yellow-400/40 text-yellow-300 bg-yellow-500/10'}`}>
              Databricks: {dbxStatus?.configured ? 'Configured' : 'Not configured'}
            </span>
            <span className="px-3 py-1 rounded-full border border-blue-400/30 text-blue-200 bg-blue-500/10">Bronze: {dbxStatus?.bronze_table || 'bronze_sensor_readings'}</span>
          </div>
        </div>
        <div className="flex gap-3">
          <button onClick={fetchSensors} className="inline-flex items-center gap-2 px-4 py-2 bg-dark-800 border border-gray-700 text-gray-200 rounded-lg hover:border-neon-cyan/50">
            <RefreshCcw className="w-4 h-4" /> Refresh
          </button>
          {user?.role !== 'admin' && (
            <button onClick={() => setShowAddModal(true)} className="inline-flex items-center gap-2 px-4 py-2 bg-neon-cyan/20 border border-neon-cyan/40 text-neon-cyan rounded-lg hover:border-neon-cyan">
              <Plus className="w-4 h-4" /> Add Sensor
            </button>
          )}
        </div>
      </div>

      {loading ? (
        <div className="text-gray-400">Loading sensors...</div>
      ) : sensors.length === 0 ? (
        <div className="border border-dashed border-gray-700 rounded-xl p-10 text-center text-gray-400">No sensors yet.</div>
      ) : (
        <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
          {sensors.map((sensor) => {
            const sensorId = getSensorId(sensor)
            const latest = latestMap[sensorId] || sensor.latest_reading || {}
            const isVirtual = sensor.source_type === 'virtual_meteostat'
            return (
              <div key={sensorId} className="bg-dark-800 border border-gray-700 rounded-xl p-6 hover:border-neon-cyan/50 transition-all">
                <div className="flex items-start justify-between gap-4 mb-5">
                  <div>
                    <div className="flex items-center gap-2 mb-2">
                      {isVirtual ? <Cloud className="w-5 h-5 text-sky-300" /> : <Cpu className="w-5 h-5 text-neon-green" />}
                      <h3 className="text-xl font-bold text-white">{sensor.name}</h3>
                    </div>
                    <p className="text-xs text-gray-500 font-mono">{sensorId}</p>
                  </div>
                  <div className="flex gap-2">
                    <span className="px-2 py-1 rounded bg-dark-900 border border-gray-700 text-xs text-gray-300">{sensor.environment_type}</span>
                    <span className="px-2 py-1 rounded bg-dark-900 border border-gray-700 text-xs text-gray-300">{isVirtual ? 'Virtual Meteostat' : 'Physical IoT'}</span>
                  </div>
                </div>

                <div className="grid grid-cols-2 gap-4 mb-5">
                  <div className="rounded-lg border border-orange-400/20 bg-orange-500/10 p-4">
                    <div className="flex items-center gap-2 text-orange-200 text-sm mb-2"><Thermometer className="w-4 h-4" /> Temperature</div>
                    <p className="text-3xl font-bold text-white">{fmt(latest.temperature, ' C')}</p>
                  </div>
                  <div className="rounded-lg border border-sky-400/20 bg-sky-500/10 p-4">
                    <div className="flex items-center gap-2 text-sky-200 text-sm mb-2"><Droplets className="w-4 h-4" /> Humidity</div>
                    <p className="text-3xl font-bold text-white">{fmt(latest.humidity, '%')}</p>
                  </div>
                </div>

                <div className="text-sm text-gray-400 space-y-1 mb-5">
                  <p>Province: <span className="text-gray-200">{sensor.location_province || sensor.location_query || '--'}</span></p>
                  <p>Updated: <span className="text-gray-200">{latest.timestamp || latest.event_ts ? formatVNTime(latest.timestamp || latest.event_ts, true) : '--'}</span></p>
                  <p>Databricks write: <span className="text-gray-200">{latest.databricks_status || 'waiting'}</span></p>
                </div>

                <div className="flex gap-2">
                  <button onClick={() => openDetails(sensor)} className="flex-1 px-4 py-2 bg-neon-cyan/20 text-neon-cyan border border-neon-cyan/40 rounded-lg hover:border-neon-cyan">Analytics</button>
                  {user?.role !== 'admin' && <button onClick={() => handleDelete(sensor)} className="px-4 py-2 bg-red-500/10 text-red-300 border border-red-400/30 rounded-lg hover:border-red-300"><Trash2 className="w-4 h-4" /></button>}
                </div>
              </div>
            )
          })}
        </div>
      )}

      {selectedSensor && (
        <div className="fixed inset-0 bg-black/80 z-50 p-6 overflow-y-auto">
          <div className="bg-dark-800 border border-neon-cyan/20 rounded-xl p-6 max-w-6xl mx-auto">
            <div className="flex justify-between items-start mb-6">
              <div>
                <h2 className="text-2xl font-bold text-white">{selectedSensor.name}</h2>
                <p className="text-sm text-gray-400 font-mono">{selectedSensorId}</p>
              </div>
              <button onClick={() => setSelectedSensor(null)} className="text-gray-400 hover:text-white">Close</button>
            </div>

            {detailLoading ? <div className="text-gray-400">Loading analytics...</div> : (
              <div className="space-y-6">
                <div className="bg-dark-900/60 rounded-lg border border-gray-700 p-4">
                  <ResponsiveContainer width="100%" height={360}>
                    <LineChart data={chartData}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                      <XAxis dataKey="time" stroke="#9CA3AF" tick={{ fontSize: 11 }} />
                      <YAxis stroke="#9CA3AF" tick={{ fontSize: 11 }} />
                      <Tooltip contentStyle={{ backgroundColor: '#111827', border: '1px solid #00d4ff', borderRadius: 8 }} />
                      <Legend />
                      <Line type="monotone" dataKey="temperature" stroke="#fb923c" dot={false} name="Temperature actual" />
                      <Line type="monotone" dataKey="humidity" stroke="#38bdf8" dot={false} name="Humidity actual" />
                      <Line type="monotone" dataKey="forecast_temperature" stroke="#facc15" dot={false} strokeDasharray="6 5" name="Temperature forecast" />
                      <Line type="monotone" dataKey="forecast_humidity" stroke="#a78bfa" dot={false} strokeDasharray="6 5" name="Humidity forecast" />
                    </LineChart>
                  </ResponsiveContainer>
                </div>

                <div className="bg-dark-900/60 rounded-lg border border-gray-700 p-4">
                  <div className="flex items-center gap-2 mb-4 text-white font-semibold"><Award className="w-5 h-5 text-yellow-300" /> Databricks Model Leaderboard</div>
                  {leaderboard.length === 0 ? (
                    <p className="text-gray-400 text-sm">No model results yet. Run the Databricks training notebook/job to populate model_evaluation_results.</p>
                  ) : (
                    <div className="overflow-x-auto">
                      <table className="w-full text-sm">
                        <thead className="text-gray-400 border-b border-gray-700">
                          <tr><th className="text-left py-2">Target</th><th className="text-left py-2">Model</th><th className="text-left py-2">MAE</th><th className="text-left py-2">RMSE</th><th className="text-left py-2">Training Time</th><th className="text-left py-2">Best</th></tr>
                        </thead>
                        <tbody>
                          {leaderboard.map((row, index) => (
                            <tr key={index} className="border-b border-gray-800 text-gray-200">
                              <td className="py-2">{row.target}</td>
                              <td className="py-2">{row.model_name || row.model}</td>
                              <td className="py-2">{row.mae ?? '--'}</td>
                              <td className="py-2">{row.rmse ?? '--'}</td>
                              <td className="py-2">{row.training_time_seconds ?? '--'}s</td>
                              <td className="py-2">{(row.is_best === true || row.is_best === 1 || row.is_best === 'true') ? 'Best' : ''}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}
                  {bestModels.length > 0 && <p className="text-xs text-yellow-200 mt-3">Best model is selected independently per target by Databricks evaluation metrics.</p>}
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      <AddDeviceModal isOpen={showAddModal} onClose={() => setShowAddModal(false)} onAdd={handleAdd} isLoading={adding} />
    </div>
  )
}
