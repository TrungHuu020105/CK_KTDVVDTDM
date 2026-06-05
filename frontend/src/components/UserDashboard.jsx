import { useEffect, useMemo, useState } from 'react'
import { CalendarDays, Download, Plus, Server, Thermometer, TrendingUp } from 'lucide-react'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'
import { useDevices } from '../context/DeviceContext'
import api from '../api'
import { formatVNDate, formatVNDateTime, formatVNTime } from '../utils/vnTime'

const getSensorId = (sensor) => sensor?.sensor_id || sensor?.source
const today = () => new Date().toISOString().slice(0, 10)
const fmt = (value, digits = 1) => value === null || value === undefined || Number.isNaN(Number(value)) ? '--' : Number(value).toFixed(digits)
const onlyTime = (value) => value ? formatVNTime(value).slice(0, 5) : '--'
const LATEST_REFRESH_MS = 3000
const HISTORY_REFRESH_MS = 15000
const FORECAST_REFRESH_MS = 5 * 60 * 1000

const startOfDay = (value) => new Date(`${value}T00:00:00`)
const endOfDay = (value) => new Date(`${value}T23:59:59`)
const dayDiffInclusive = (fromDate, toDate) => Math.max(1, Math.round((endOfDay(toDate) - startOfDay(fromDate)) / 86400000) + 1)
const hourKey = (value) => value ? formatVNTime(value).slice(0, 2) : ''
const average = (values) => {
  const valid = values.filter((value) => value !== null && value !== undefined && !Number.isNaN(Number(value))).map(Number)
  if (!valid.length) return null
  return valid.reduce((sum, value) => sum + value, 0) / valid.length
}

const addForecastBridge = (rows) => {
  const bridged = (rows || []).map((row) => ({ ...row }))

  const firstForecastIndex = bridged.findIndex(
    (row) => row.forecast_temperature !== null || row.forecast_humidity !== null
  )
  if (firstForecastIndex <= 0) return bridged

  for (let index = firstForecastIndex - 1; index >= 0; index -= 1) {
    const row = bridged[index]
    const hasActual = row.temperature !== null || row.humidity !== null
    if (!hasActual) continue

    if (row.temperature !== null && row.forecast_temperature === null) {
      row.forecast_temperature = row.temperature
      row.forecast_temperature_bridge = true
    }
    if (row.humidity !== null && row.forecast_humidity === null) {
      row.forecast_humidity = row.humidity
      row.forecast_humidity_bridge = true
    }
    break
  }

  return bridged
}

const listDateKeys = (fromDate, toDate) => {
  const keys = []
  const cursor = startOfDay(fromDate)
  const end = startOfDay(toDate)
  while (cursor <= end) {
    keys.push(cursor.toISOString().slice(0, 10))
    cursor.setDate(cursor.getDate() + 1)
  }
  return keys
}

export default function UserDashboard() {
  const { sensors, selectedSensorId, setSelectedSensorId } = useDevices()
  const [latestMap, setLatestMap] = useState({})
  const [history, setHistory] = useState([])
  const [forecast, setForecast] = useState([])
  const [fromDate, setFromDate] = useState(today())
  const [toDate, setToDate] = useState(today())
  const [error, setError] = useState('')
  const [exporting, setExporting] = useState(false)

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

  const loadHistory = async () => {
    if (!activeSensorId) return
    try {
      const minutes = Math.max(24 * 60, dayDiffInclusive(fromDate, toDate) * 24 * 60)
      const historyRes = await api.get(`/api/sensors/${activeSensorId}/history`, { params: { minutes } }).catch(() => ({ data: { readings: [] } }))
      setHistory(historyRes.data?.readings || [])
      setError('')
    } catch (err) {
      setError(err.response?.data?.detail || 'Khong tai duoc du lieu dashboard.')
    }
  }

  const loadForecast = async () => {
    if (!activeSensorId) return
    try {
      const forecastRes = await api.get(`/api/sensors/${activeSensorId}/forecast`).catch(() => ({ data: { forecasts: [] } }))
      setForecast(forecastRes.data?.forecasts || [])
      setError('')
    } catch (err) {
      setError(err.response?.data?.detail || 'Khong tai duoc du lieu dashboard.')
    }
  }

  const loadChart = async () => {
    await Promise.all([loadHistory(), loadForecast()])
  }

  useEffect(() => {
    if (sensors.length && !selectedSensorId) setSelectedSensorId(getSensorId(sensors[0]))
  }, [sensors, selectedSensorId, setSelectedSensorId])

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
    }, LATEST_REFRESH_MS)
    return () => window.clearInterval(interval)
  }, [sensors])

  useEffect(() => {
    const interval = window.setInterval(() => {
      if (document.visibilityState === 'hidden') return
      loadHistory()
    }, HISTORY_REFRESH_MS)
    return () => window.clearInterval(interval)
  }, [activeSensorId, fromDate, toDate])

  useEffect(() => {
    const interval = window.setInterval(() => {
      if (document.visibilityState === 'hidden') return
      loadForecast()
    }, FORECAST_REFRESH_MS)
    return () => window.clearInterval(interval)
  }, [activeSensorId, fromDate, toDate])

  const chartData = useMemo(() => {
    const normalizedFrom = fromDate <= toDate ? fromDate : toDate
    const normalizedTo = fromDate <= toDate ? toDate : fromDate
    const from = startOfDay(normalizedFrom)
    const to = endOfDay(normalizedTo)
    const singleDay = normalizedFrom === normalizedTo
    const now = new Date()
    const rawActual = (history || []).filter((row) => {
      const stamp = new Date(row.timestamp || row.event_ts)
      return stamp >= from && stamp <= to
    })
    const latestActualTs = rawActual.length
      ? rawActual
        .map((row) => new Date(row.timestamp || row.event_ts))
        .sort((a, b) => a - b)
        .at(-1)
      : null
    const futureCutoffTs = latestActualTs
      ? new Date(Math.max(latestActualTs.getTime(), now.getTime()))
      : now

    const actual = rawActual
      .map((row) => {
        const timestamp = row.timestamp || row.event_ts
        return {
          timestamp,
          label: singleDay ? onlyTime(timestamp) : formatVNDate(timestamp),
          tooltipLabel: formatVNDateTime(timestamp, false),
          temperature: row.temperature,
          humidity: row.humidity,
          forecast_temperature: null,
          forecast_humidity: null,
          forecast_temperature_bridge: false,
          forecast_humidity_bridge: false,
        }
      })

    const predicted = (forecast || [])
      .filter((row) => {
        const stamp = new Date(row.forecast_ts || row.timestamp || row.event_ts)
        if (stamp < from || stamp > to) return false
        if (stamp <= futureCutoffTs) return false
        return true
      })
      .map((row) => {
        const timestamp = row.forecast_ts || row.timestamp || row.event_ts
        return {
          timestamp,
          label: singleDay ? onlyTime(timestamp) : formatVNDate(timestamp),
          tooltipLabel: formatVNDateTime(timestamp, false),
          temperature: null,
          humidity: null,
          forecast_temperature: row.temperature ?? row.predicted_temperature ?? (row.target === 'temperature' ? row.predicted_value : null),
          forecast_humidity: row.humidity ?? row.predicted_humidity ?? (row.target === 'humidity' ? row.predicted_value : null),
          forecast_temperature_bridge: false,
          forecast_humidity_bridge: false,
        }
      })

    if (singleDay) {
      const actualByHour = new Map()
      const forecastByHour = new Map()

      for (const row of actual) {
        const key = hourKey(row.timestamp)
        if (!actualByHour.has(key)) actualByHour.set(key, [])
        actualByHour.get(key).push(row)
      }

      for (const row of predicted) {
        const key = hourKey(row.timestamp)
        if (!forecastByHour.has(key)) forecastByHour.set(key, [])
        forecastByHour.get(key).push(row)
      }

      const hours = Array.from(new Set([
        ...actualByHour.keys(),
        ...forecastByHour.keys(),
      ])).sort((a, b) => Number(a) - Number(b))

      return addForecastBridge(hours.map((hour) => {
        const actualRows = actualByHour.get(hour) || []
        const forecastRows = forecastByHour.get(hour) || []
        return {
          timestamp: `${normalizedFrom}T${hour}:00:00`,
          label: `${hour}:00`,
          tooltipLabel: `${normalizedFrom} ${hour}:00`,
          temperature: average(actualRows.map((row) => row.temperature)),
          humidity: average(actualRows.map((row) => row.humidity)),
          forecast_temperature: average(forecastRows.map((row) => row.forecast_temperature)),
          forecast_humidity: average(forecastRows.map((row) => row.forecast_humidity)),
          forecast_temperature_bridge: false,
          forecast_humidity_bridge: false,
        }
      }))
    }

    if (!singleDay) {
      const actualByDate = new Map()
      const forecastByDate = new Map()

      for (const row of actual) {
        const dateKey = formatVNDate(row.timestamp)
        if (!actualByDate.has(dateKey)) actualByDate.set(dateKey, [])
        actualByDate.get(dateKey).push(row)
      }

      for (const row of predicted) {
        const dateKey = formatVNDate(row.timestamp)
        if (!forecastByDate.has(dateKey)) forecastByDate.set(dateKey, [])
        forecastByDate.get(dateKey).push(row)
      }

      return addForecastBridge(listDateKeys(normalizedFrom, normalizedTo).map((dateKey) => {
        const actualRows = actualByDate.get(dateKey) || []
        const forecastRows = forecastByDate.get(dateKey) || []
        return {
          timestamp: `${dateKey}T00:00:00`,
          label: dateKey,
          tooltipLabel: dateKey,
          temperature: average(actualRows.map((row) => row.temperature)),
          humidity: average(actualRows.map((row) => row.humidity)),
          forecast_temperature: average(forecastRows.map((row) => row.forecast_temperature)),
          forecast_humidity: average(forecastRows.map((row) => row.forecast_humidity)),
          forecast_temperature_bridge: false,
          forecast_humidity_bridge: false,
        }
      }))
    }

    return addForecastBridge([...actual, ...predicted].sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp)))
  }, [history, forecast, fromDate, toDate])
  const hasForecastPoints = chartData.some((row) => row.forecast_temperature !== null || row.forecast_humidity !== null)
  const forecastPointCount = chartData.filter((row) => row.forecast_temperature !== null || row.forecast_humidity !== null).length
  const showForecastDots = forecastPointCount <= 2

  const forecastSnapshot = forecast.find((row) => row.temperature !== null && row.temperature !== undefined) || forecast[0]
  const forecastTemp = forecastSnapshot?.temperature ?? forecastSnapshot?.predicted_temperature ?? forecastSnapshot?.predicted_value
  const confidence = forecastSnapshot?.confidence ?? forecastSnapshot?.confidence_score

  const downloadCsv = async () => {
    if (!activeSensorId) return
    const normalizedFrom = fromDate <= toDate ? fromDate : toDate
    const normalizedTo = fromDate <= toDate ? toDate : fromDate
    setExporting(true)
    try {
      const res = await api.get(`/api/sensors/${activeSensorId}/history/export`, {
        params: {
          from_date: normalizedFrom,
          to_date: normalizedTo,
        },
        responseType: 'blob',
      })
      const blob = new Blob([res.data], { type: 'text/csv;charset=utf-8;' })
      const url = window.URL.createObjectURL(blob)
      const link = document.createElement('a')
      const safeSensorId = String(activeSensorId).replace(/[^a-zA-Z0-9_-]+/g, '_')
      link.href = url
      link.download = `${safeSensorId}_${normalizedFrom}_${normalizedTo}.csv`
      document.body.appendChild(link)
      link.click()
      link.remove()
      window.URL.revokeObjectURL(url)
    } catch (err) {
      setError(err.response?.data?.detail || 'Khong tai duoc file CSV cho sensor nay.')
    } finally {
      setExporting(false)
    }
  }

  return (
    <div className="min-h-screen bg-dark-900 p-8">
      <div className="mb-8">
        <h1 className="text-4xl font-bold text-white mb-2">Dashboard</h1>
        <p className="text-gray-400">Theo doi nhanh thiet bi, du lieu thuc te va phan du bao trong cung mot man hinh.</p>
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-6 mb-8">
        <SummaryCard icon={Thermometer} title="IoT Devices" value={sensors.length} action="+ Add Device" />
        <SummaryCard icon={Server} title="Active Sensors" value={sensors.filter((sensor) => sensor.is_active !== false).length} detail="Chi tinh nhung sensor con hoat dong." />
        <SummaryCard icon={TrendingUp} title="Forecast Snapshot" value={fmt(forecastTemp, 4)} detail={`Muc tin cay: ${confidence ? `${Math.round(Number(confidence) * 100)}%` : '--'}`} yellow />
      </div>

      <div className="rounded-xl border border-neon-cyan/20 bg-dark-800 p-6">
        <div className="flex items-center gap-2 mb-3">
          <TrendingUp className="w-5 h-5 text-neon-cyan" />
          <h2 className="text-2xl font-bold text-white">Forecast View</h2>
        </div>
        <p className="text-gray-400 text-sm mb-6">
          Chon 1 ngay de xem du lieu theo gio. Bieu do hien 2 chi so: nhiet do va do am.
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

        <div className="mb-5 flex justify-end">
          <button
            type="button"
            onClick={downloadCsv}
            disabled={!activeSensorId || exporting}
            className="inline-flex items-center gap-2 rounded-lg border border-emerald-400/40 bg-emerald-500/10 px-4 py-3 text-emerald-200 transition hover:border-emerald-300 disabled:opacity-60"
          >
            <Download className="w-4 h-4" />
            {exporting ? 'Dang tai CSV...' : 'Tai CSV cua sensor'}
          </button>
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
              <p className="text-gray-500 text-sm">Recorded la du lieu that, Forecast la ket qua Databricks. Forecast chi hien thi tu thoi diem hien tai tro di va duoc noi mem tu moc actual cuoi cung.</p>
              {!hasForecastPoints && (
                <p className="text-amber-300 text-xs mt-2">Khung ngay dang chon hien khong con moc forecast nao o tuong lai.</p>
              )}
              {hasForecastPoints && showForecastDots && (
                <p className="text-cyan-300 text-xs mt-2">Khung ngay dang chon chi con {forecastPointCount} moc forecast o tuong lai, nen dashboard hien cham du bao thay vi duong dai.</p>
              )}
            </div>
            <div className="text-sm text-gray-400">
              Latest: <span className="text-neon-cyan">{fmt(latest.temperature)}C</span> / <span className="text-sky-300">{fmt(latest.humidity)}%</span>
            </div>
          </div>

          {chartData.length === 0 ? (
            <div className="h-[420px] flex items-center justify-center text-gray-400">Chua co du lieu realtime cho sensor nay.</div>
          ) : (
            <ResponsiveContainer width="100%" height={420}>
              <LineChart data={chartData}>
                <CartesianGrid strokeDasharray="4 4" stroke="#202a4a" />
                <XAxis dataKey="label" stroke="#9CA3AF" tick={{ fontSize: 11 }} minTickGap={20} />
                <YAxis yAxisId="temp" stroke="#9CA3AF" tick={{ fontSize: 11 }} />
                <YAxis yAxisId="hum" orientation="right" stroke="#9CA3AF" tick={{ fontSize: 11 }} />
                <Tooltip
                  content={({ active, payload }) => {
                    if (!active || !payload?.length) return null
                    const row = payload[0]?.payload || {}
                    const tooltipRows = payload.filter((entry) => {
                      if (entry.dataKey === 'forecast_temperature' && row.forecast_temperature_bridge) return false
                      if (entry.dataKey === 'forecast_humidity' && row.forecast_humidity_bridge) return false
                      return entry.value !== null && entry.value !== undefined
                    })
                    if (!tooltipRows.length) return null

                    return (
                      <div className="rounded-lg border border-neon-cyan bg-gray-900 px-4 py-3 shadow-lg">
                        <p className="mb-3 text-white">{row.tooltipLabel || '--'}</p>
                        <div className="space-y-2">
                          {tooltipRows.map((entry) => (
                            <p key={entry.dataKey} style={{ color: entry.color }}>
                              {entry.name} : {fmt(entry.value, 2)}
                            </p>
                          ))}
                        </div>
                      </div>
                    )
                  }}
                  contentStyle={{ backgroundColor: '#111827', border: '1px solid #00f0ff', borderRadius: 8 }}
                />
                <Legend />
                <Line yAxisId="temp" type="monotone" dataKey="temperature" stroke="#ff6680" strokeWidth={3} dot={false} strokeLinecap="round" strokeLinejoin="round" name="temperature recorded" />
                <Line yAxisId="hum" type="monotone" dataKey="humidity" stroke="#38bdf8" strokeWidth={3} dot={false} strokeLinecap="round" strokeLinejoin="round" name="humidity recorded" />
                <Line yAxisId="temp" type="monotone" dataKey="forecast_temperature" stroke="#ffd400" strokeWidth={3} strokeDasharray="6 5" strokeLinecap="round" strokeLinejoin="round" dot={showForecastDots ? { r: 4, fill: '#ffd400', stroke: '#ffd400' } : false} activeDot={{ r: 6 }} connectNulls name="temperature forecast" />
                <Line yAxisId="hum" type="monotone" dataKey="forecast_humidity" stroke="#ff7ab6" strokeWidth={3} strokeDasharray="6 5" strokeLinecap="round" strokeLinejoin="round" dot={showForecastDots ? { r: 4, fill: '#ff7ab6', stroke: '#ff7ab6' } : false} activeDot={{ r: 6 }} connectNulls name="humidity forecast" />
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
