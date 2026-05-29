import { useEffect, useMemo, useState } from 'react'
import { CalendarDays, Plus, Server, Thermometer, TrendingUp } from 'lucide-react'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'
import { useDevices } from '../context/DeviceContext'
import api from '../api'
import { formatVNTime } from '../utils/vnTime'

const getSensorId = (sensor) => sensor?.sensor_id || sensor?.source
const today = () => new Date().toISOString().slice(0, 10)
const fmt = (value, digits = 1) => value === null || value === undefined || Number.isNaN(Number(value)) ? '--' : Number(value).toFixed(digits)
const onlyTime = (value) => value ? formatVNTime(value).split(' ').pop()?.slice(0, 5) : '--'

export default function UserDashboard() {
  const { sensors, selectedSensorId, setSelectedSensorId } = useDevices()
  const [latestMap, setLatestMap] = useState({})
  const [history, setHistory] = useState([])
  const [forecast, setForecast] = useState([])
  const [fromDate, setFromDate] = useState(today())
  const [toDate, setToDate] = useState(today())
  const [error, setError] = useState('')

  const activeSensorId = selectedSensorId || getSensorId(sensors?.[0])
  const activeSensor = sensors.find((sensor) => getSensorId(sensor) === activeSensorId)
  const latest = latestMap[activeSensorId] || activeSensor?.latest_reading || {}

  const loadLatest = async () => {
    if (!sensors?.length) return
    const pairs = await Promise.all(sensors.map(async (sensor) => {
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

  const loadChart = async () => {
    if (!activeSensorId) return
    try {
      const [historyRes, forecastRes] = await Promise.all([
        api.get(`/api/sensors/${activeSensorId}/history`, { params: { minutes: 24 * 60 } }).catch(() => ({ data: { readings: [] } })),
        api.get(`/api/sensors/${activeSensorId}/forecast`).catch(() => ({ data: { forecasts: [] } })),
      ])
      setHistory(historyRes.data?.readings || [])
      setForecast(forecastRes.data?.forecasts || [])
      setError('')
    } catch (err) {
      setError(err.response?.data?.detail || 'Không tải được dữ liệu dashboard.')
    }
  }

  useEffect(() => {
    if (sensors.length && !selectedSensorId) setSelectedSensorId(getSensorId(sensors[0]))
  }, [sensors, selectedSensorId])

  useEffect(() => {
    loadLatest()
  }, [sensors])

  useEffect(() => {
    loadChart()
  }, [activeSensorId, fromDate, toDate])

  useEffect(() => {
    const interval = window.setInterval(() => {
      if (document.visibilityState === 'hidden') return
      loadLatest()
      loadChart()
    }, 3000)
    return () => window.clearInterval(interval)
  }, [sensors, activeSensorId, fromDate, toDate])

  const chartData = useMemo(() => {
    const from = new Date(`${fromDate}T00:00:00`)
    const to = new Date(`${toDate}T23:59:59`)
    const actual = (history || [])
      .filter((row) => {
        const stamp = new Date(row.timestamp || row.event_ts)
        return stamp >= from && stamp <= to
      })
      .map((row) => ({
        timestamp: row.timestamp || row.event_ts,
        time: onlyTime(row.timestamp || row.event_ts),
        temperature: row.temperature,
        humidity: row.humidity,
        forecast_temperature: null,
        forecast_humidity: null,
      }))
    const predicted = (forecast || []).map((row) => ({
      timestamp: row.forecast_ts || row.timestamp || row.event_ts,
      time: onlyTime(row.forecast_ts || row.timestamp || row.event_ts),
      temperature: null,
      humidity: null,
      forecast_temperature: row.temperature ?? row.predicted_temperature ?? (row.target === 'temperature' ? row.predicted_value : null),
      forecast_humidity: row.humidity ?? row.predicted_humidity ?? (row.target === 'humidity' ? row.predicted_value : null),
    }))
    return [...actual, ...predicted].sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp))
  }, [history, forecast, fromDate, toDate])

  const forecastSnapshot = forecast.find((row) => row.target === 'temperature') || forecast[0]
  const forecastTemp = forecastSnapshot?.temperature ?? forecastSnapshot?.predicted_temperature ?? forecastSnapshot?.predicted_value
  const confidence = forecastSnapshot?.confidence ?? forecastSnapshot?.confidence_score

  return (
    <div className="min-h-screen bg-dark-900 p-8">
      <div className="mb-8">
        <h1 className="text-4xl font-bold text-white mb-2">Dashboard</h1>
        <p className="text-gray-400">Theo dõi nhanh thiết bị, dữ liệu thực tế và phần dự báo trong cùng một màn hình.</p>
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-6 mb-8">
        <SummaryCard icon={Thermometer} title="IoT Devices" value={sensors.length} action="+ Add Device" />
        <SummaryCard icon={Server} title="Active Sensors" value={sensors.filter((sensor) => sensor.is_active !== false).length} detail="Chỉ tính những sensor còn hoạt động." />
        <SummaryCard icon={TrendingUp} title="Forecast Snapshot" value={fmt(forecastTemp, 4)} detail={`Mức tin cậy: ${confidence ? `${Math.round(Number(confidence) * 100)}%` : '--'}`} yellow />
      </div>

      <div className="rounded-xl border border-neon-cyan/20 bg-dark-800 p-6">
        <div className="flex items-center gap-2 mb-3">
          <TrendingUp className="w-5 h-5 text-neon-cyan" />
          <h2 className="text-2xl font-bold text-white">Forecast View</h2>
        </div>
        <p className="text-gray-400 text-sm mb-6">
          Chọn 1 ngày để xem dữ liệu theo giờ. Biểu đồ hiện 2 chỉ số: nhiệt độ và độ ẩm.
        </p>

        <div className="grid grid-cols-1 xl:grid-cols-3 gap-5 mb-5">
          <label>
            <span className="block text-sm text-gray-300 mb-2">Sensor</span>
            <select value={activeSensorId || ''} onChange={(e) => setSelectedSensorId(e.target.value)} className="w-full bg-dark-900 border border-gray-700 rounded-lg px-4 py-3 text-white focus:outline-none focus:border-neon-cyan">
              {sensors.map((sensor) => (
                <option key={getSensorId(sensor)} value={getSensorId(sensor)}>
                  {sensor.name} (temperature + humidity)
                </option>
              ))}
            </select>
          </label>
          <label>
            <span className="block text-sm text-gray-300 mb-2">From Date</span>
            <div className="relative">
              <CalendarDays className="w-4 h-4 text-gray-400 absolute left-3 top-1/2 -translate-y-1/2" />
              <input type="date" value={fromDate} onChange={(e) => setFromDate(e.target.value)} className="w-full bg-dark-900 border border-gray-700 rounded-lg pl-10 pr-4 py-3 text-white focus:outline-none focus:border-neon-cyan" />
            </div>
          </label>
          <label>
            <span className="block text-sm text-gray-300 mb-2">To Date</span>
            <div className="relative">
              <CalendarDays className="w-4 h-4 text-gray-400 absolute left-3 top-1/2 -translate-y-1/2" />
              <input type="date" value={toDate} onChange={(e) => setToDate(e.target.value)} className="w-full bg-dark-900 border border-gray-700 rounded-lg pl-10 pr-4 py-3 text-white focus:outline-none focus:border-neon-cyan" />
            </div>
          </label>
        </div>

        {error && (
          <div className="mb-4 rounded-lg border border-yellow-400/30 bg-yellow-500/10 px-4 py-3 text-yellow-200 text-sm">
            {error}
          </div>
        )}

        <div className="rounded-xl bg-dark-900/70 border border-gray-700 p-5">
          <div className="flex items-start justify-between mb-4">
            <div>
              <h3 className="text-white font-semibold">Chart Output</h3>
              <p className="text-gray-500 text-sm">Recorded là dữ liệu thật, Forecast là kết quả Databricks nếu đã chạy notebook.</p>
            </div>
            <div className="text-sm text-gray-400">
              Latest: <span className="text-neon-cyan">{fmt(latest.temperature)}°C</span> / <span className="text-sky-300">{fmt(latest.humidity)}%</span>
            </div>
          </div>

          {chartData.length === 0 ? (
            <div className="h-[420px] flex items-center justify-center text-gray-400">Chưa có dữ liệu realtime cho sensor này.</div>
          ) : (
            <ResponsiveContainer width="100%" height={420}>
              <LineChart data={chartData}>
                <CartesianGrid strokeDasharray="4 4" stroke="#202a4a" />
                <XAxis dataKey="time" stroke="#9CA3AF" tick={{ fontSize: 11 }} />
                <YAxis yAxisId="temp" stroke="#9CA3AF" tick={{ fontSize: 11 }} />
                <YAxis yAxisId="hum" orientation="right" stroke="#9CA3AF" tick={{ fontSize: 11 }} />
                <Tooltip contentStyle={{ backgroundColor: '#111827', border: '1px solid #00f0ff', borderRadius: 8 }} />
                <Legend />
                <Line yAxisId="temp" type="monotone" dataKey="temperature" stroke="#ff6680" strokeWidth={3} dot={false} name="temperature recorded" />
                <Line yAxisId="hum" type="monotone" dataKey="humidity" stroke="#38bdf8" strokeWidth={3} dot={false} name="humidity recorded" />
                <Line yAxisId="temp" type="monotone" dataKey="forecast_temperature" stroke="#ffd400" strokeWidth={3} strokeDasharray="6 5" dot={false} name="temperature forecast" />
                <Line yAxisId="hum" type="monotone" dataKey="forecast_humidity" stroke="#ff7ab6" strokeWidth={3} strokeDasharray="6 5" dot={false} name="humidity forecast" />
              </LineChart>
            </ResponsiveContainer>
          )}
        </div>
      </div>
    </div>
  )
}

function SummaryCard({ icon: Icon, title, value, detail, action, yellow = false }) {
  return (
    <div className="rounded-xl border border-neon-cyan/20 bg-dark-800 p-6">
      <div className="flex items-center justify-between mb-5">
        <h3 className="text-white font-bold">{title}</h3>
        <Icon className={`w-5 h-5 ${yellow ? 'text-neon-green' : 'text-neon-cyan'}`} />
      </div>
      <p className={`text-5xl font-bold ${yellow ? 'text-neon-yellow' : 'text-neon-cyan'}`}>{value}</p>
      {detail && <p className="text-gray-400 text-sm mt-3">{detail}</p>}
      {action && (
        <button className="w-full mt-4 inline-flex items-center justify-center gap-2 rounded-lg bg-neon-cyan/20 border border-neon-cyan/30 py-2 text-neon-cyan">
          <Plus className="w-4 h-4" /> {action.replace('+ ', '')}
        </button>
      )}
    </div>
  )
}
