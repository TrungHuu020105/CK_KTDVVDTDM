import React, { useCallback, useEffect, useMemo, useState } from 'react'
import axios from 'axios'
import DatabricksChart from './DatabricksChart'
import '../styles/Analytics.css'

const API_BASE_URL = 'http://localhost:8000/api/analytics'

const getVietnamDateString = (date = new Date()) => {
  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Asia/Ho_Chi_Minh',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(date)

  const day = parts.find((item) => item.type === 'day')?.value
  const month = parts.find((item) => item.type === 'month')?.value
  const year = parts.find((item) => item.type === 'year')?.value

  return `${year}-${month}-${day}`
}

const getVietnamHourLabel = (value) =>
  `${new Intl.DateTimeFormat('en-GB', {
    hour: '2-digit',
    hour12: false,
    timeZone: 'Asia/Ho_Chi_Minh',
  }).format(new Date(value))}:00`

const getVietnamDayKey = (value) => getVietnamDateString(new Date(value))

const formatMetricLabel = (value) =>
  String(value || '')
    .split('_')
    .map((item) => item.charAt(0).toUpperCase() + item.slice(1))
    .join(' ')

const formatDisplayDate = (value) => {
  if (!value) {
    return '--'
  }

  const [year, month, day] = value.split('-')
  return `${day}/${month}/${year}`
}

export default function Analytics() {
  const [sensors, setSensors] = useState([])
  const [selectedSeries, setSelectedSeries] = useState('')
  const [fromDate, setFromDate] = useState(() => getVietnamDateString())
  const [toDate, setToDate] = useState(() => getVietnamDateString())
  const [measurements, setMeasurements] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [chartData, setChartData] = useState([])

  const sensorOptions = useMemo(
    () =>
      sensors.map((sensor) => ({
        ...sensor,
        key: `${sensor.sensor_id}|${sensor.metric_type}`,
        label: `${sensor.sensor_id} (${formatMetricLabel(sensor.metric_type)})`,
      })),
    [sensors],
  )

  const selectedOption = useMemo(
    () => sensorOptions.find((item) => item.key === selectedSeries) || null,
    [sensorOptions, selectedSeries],
  )

  const selectedSensor = selectedOption?.sensor_id || ''
  const selectedMetric = selectedOption?.metric_type || ''
  const selectedUnit = selectedOption?.unit || ''
  const selectedMetricLabel = selectedMetric ? formatMetricLabel(selectedMetric) : ''
  const dateRangeLabel =
    fromDate === toDate
      ? formatDisplayDate(fromDate)
      : `${formatDisplayDate(fromDate)} - ${formatDisplayDate(toDate)}`

  const processHourlyData = useCallback((data) => {
    const grouped = {}

    data.forEach((record) => {
      const hourKey = getVietnamHourLabel(record.event_ts)

      if (!grouped[hourKey]) {
        grouped[hourKey] = []
      }

      grouped[hourKey].push(Number(record.metric_value))
    })

    const nextChartData = Object.keys(grouped)
      .sort((a, b) => Number(a.slice(0, 2)) - Number(b.slice(0, 2)))
      .map((hour) => ({
        time: hour,
        value: grouped[hour].reduce((acc, item) => acc + item, 0) / grouped[hour].length,
        count: grouped[hour].length,
      }))

    setChartData(nextChartData)
  }, [])

  const processDailyData = useCallback((data) => {
    const grouped = {}

    data.forEach((record) => {
      const dayKey = getVietnamDayKey(record.event_ts)

      if (!grouped[dayKey]) {
        grouped[dayKey] = []
      }

      grouped[dayKey].push(Number(record.metric_value))
    })

    const nextChartData = Object.keys(grouped)
      .sort()
      .map((day) => ({
        time: day,
        value: grouped[day].reduce((acc, item) => acc + item, 0) / grouped[day].length,
        count: grouped[day].length,
      }))

    setChartData(nextChartData)
  }, [])

  const processChartData = useCallback(
    (data, from, to) => {
      if (!data || data.length === 0) {
        setChartData([])
        return
      }

      // Nếu chọn cùng 1 ngày thì hiển thị dữ liệu của ngày đó theo từng giờ.
      if (from === to) {
        processHourlyData(data)
        return
      }

      processDailyData(data)
    },
    [processDailyData, processHourlyData],
  )

  const fetchMeasurements = useCallback(
    async ({ sensorId, metricType, from, to }) => {
      if (!sensorId || !metricType || !from || !to) {
        return
      }

      if (from > to) {
        setError('Từ ngày không được lớn hơn đến ngày')
        setMeasurements([])
        setChartData([])
        return
      }

      setLoading(true)
      setError('')

      try {
        const response = await axios.get(`${API_BASE_URL}/measurements`, {
          params: {
            sensor_id: sensorId,
            metric_type: metricType,
            from_date: from,
            to_date: to,
            limit: 10000,
          },
        })

        if (response.data.status === 'ok') {
          const rows = response.data.data || []
          setMeasurements(rows)
          processChartData(rows, from, to)
        } else {
          setError(response.data.message || 'Lỗi lấy dữ liệu')
          setMeasurements([])
          setChartData([])
        }
      } catch (err) {
        console.error('Error fetching measurements:', err)
        setError('Lỗi kết nối server')
        setMeasurements([])
        setChartData([])
      } finally {
        setLoading(false)
      }
    },
    [processChartData],
  )

  const connectedDeviceCount = useMemo(
    () => new Set(sensors.map((sensor) => sensor.sensor_id)).size,
    [sensors],
  )

  const metricStats = useMemo(() => {
    const values = measurements
      .map((record) => Number(record.metric_value))
      .filter((value) => Number.isFinite(value))

    if (values.length === 0) {
      return null
    }

    const min = Math.min(...values)
    const max = Math.max(...values)
    const avg = values.reduce((acc, value) => acc + value, 0) / values.length

    return {
      min,
      max,
      avg,
    }
  }, [measurements])

  useEffect(() => {
    const fetchSensors = async () => {
      try {
        const response = await axios.get(`${API_BASE_URL}/sensors`)
        if (response.data.status === 'ok') {
          const data = response.data.data || []
          setSensors(data)

          if (data.length > 0) {
            setSelectedSeries(`${data[0].sensor_id}|${data[0].metric_type}`)
          }
        }
      } catch (err) {
        console.error('Error fetching sensors:', err)
        setError('Không thể lấy danh sách cảm biến')
      }
    }

    fetchSensors()
  }, [])

  useEffect(() => {
    if (!selectedSensor || !selectedMetric || !fromDate || !toDate) {
      return
    }

    const timer = setTimeout(() => {
      fetchMeasurements({
        sensorId: selectedSensor,
        metricType: selectedMetric,
        from: fromDate,
        to: toDate,
      })
    }, 180)

    return () => clearTimeout(timer)
  }, [selectedSensor, selectedMetric, fromDate, toDate, fetchMeasurements])

  const setTodayRange = () => {
    const today = getVietnamDateString()
    setFromDate(today)
    setToDate(today)
  }

  return (
    <div className="analytics-page">
      <header className="analytics-title-block">
        <h2>Dashboard</h2>
        <p>Overview of your IoT devices and analytics</p>
      </header>

      <section className="overview-grid" aria-label="Overview cards">
        <article className="overview-card devices-card">
          <div className="overview-card-head compact">
            <div>
              <p className="overview-kicker">Devices</p>
              <h3>IoT Devices</h3>
            </div>
            <span className="overview-icon pulse" aria-hidden="true"></span>
          </div>

          <div className="devices-summary">
            <p className="overview-value">{connectedDeviceCount}</p>
            <p className="overview-caption">{connectedDeviceCount} devices connected</p>
          </div>

          <button type="button" className="overview-button">
            + Manage Devices
          </button>
        </article>

        <article className="overview-card metrics-card">
          <div className="overview-card-head compact metrics-head">
            <div>
              <p className="overview-kicker">Overview</p>
              <h3>Min / Max / Average</h3>
            </div>

            <span className="metric-chip">
              {selectedMetricLabel ? `${selectedMetricLabel} (${selectedUnit})` : 'No metric'}
            </span>
          </div>

          {metricStats ? (
            <div className="metrics-grid">
              <article className="metric-tile min">
                <p>Min</p>
                <strong>
                  {metricStats.min.toFixed(2)} {selectedUnit}
                </strong>
              </article>

              <article className="metric-tile max">
                <p>Max</p>
                <strong>
                  {metricStats.max.toFixed(2)} {selectedUnit}
                </strong>
              </article>

              <article className="metric-tile avg">
                <p>Average</p>
                <strong>
                  {metricStats.avg.toFixed(2)} {selectedUnit}
                </strong>
              </article>
            </div>
          ) : (
            <p className="overview-caption metrics-empty">
              Chưa có dữ liệu cho {selectedMetricLabel || 'metric được chọn'}
            </p>
          )}
        </article>
      </section>

      <section className="analysis-card" aria-label="Sensor Data Analysis">
        <div className="analysis-head">
          <div>
            <h3>Sensor Data Analysis</h3>
            <p className="analysis-subtitle">
              {selectedSensor || 'No sensor'} • {dateRangeLabel}
            </p>
          </div>

          <div className="analysis-meta">
            <span className="meta-chip">{selectedMetricLabel || 'Select metric'}</span>
            <button type="button" className="quick-today" onClick={setTodayRange}>
              Today
            </button>
          </div>
        </div>

        <div className="analysis-filters">
          <label className="filter-field">
            <span>Select Sensor</span>
            <select
              value={selectedSeries}
              onChange={(event) => setSelectedSeries(event.target.value)}
            >
              <option value="">-- Select sensor --</option>
              {sensorOptions.map((option) => (
                <option key={option.key} value={option.key}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <label className="filter-field">
            <span>From Date</span>
            <input
              type="date"
              value={fromDate}
              onChange={(event) => setFromDate(event.target.value)}
            />
          </label>

          <label className="filter-field">
            <span>To Date</span>
            <input
              type="date"
              value={toDate}
              onChange={(event) => setToDate(event.target.value)}
            />
          </label>
        </div>

        {error ? <div className="analysis-error">{error}</div> : null}

        {loading ? <div className="analysis-loading">Đang tải dữ liệu...</div> : null}

        {!loading && chartData.length > 0 ? (
          <DatabricksChart data={chartData} metric={selectedMetric} />
        ) : null}

        {!loading && !error && chartData.length === 0 ? (
          <div className="analysis-empty">No data found for this date range.</div>
        ) : null}
      </section>
    </div>
  )
}
