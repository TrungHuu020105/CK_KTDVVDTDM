import React, { useEffect, useMemo, useRef, useState } from 'react'
import Analytics from './Analytics'
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

const BASE_DEVICE_CONFIG = [
  {
    id: 'sensor-1-temperature',
    title: 'Temperature',
    source: 'sensor_1',
    location: 'Living_Room',
    dataKey: 'temperature',
    unit: '°C',
    tag: 'temperature',
    decimals: 2,
    isCustom: false,
  },
  {
    id: 'sensor-2-humidity',
    title: 'Humidity',
    source: 'sensor_2',
    location: 'Living_Room',
    dataKey: 'humidity',
    unit: '%',
    tag: 'humidity',
    decimals: 2,
    isCustom: false,
  },
  {
    id: 'sensor-3-soil-moisture',
    title: 'Soil Moisture',
    source: 'sensor_3',
    location: 'Garden',
    dataKey: 'soil_moisture',
    unit: '%',
    tag: 'soil_moisture',
    decimals: 2,
    isCustom: false,
  },
  {
    id: 'sensor-4-light-intensity',
    title: 'Light Intensity',
    source: 'sensor_4',
    location: 'Outdoor',
    dataKey: 'light_intensity',
    unit: 'lux',
    tag: 'light_intensity',
    decimals: 0,
    isCustom: false,
  },
  {
    id: 'sensor-5-pressure',
    title: 'Pressure',
    source: 'sensor_5',
    location: 'Outdoor',
    dataKey: 'pressure',
    unit: 'hPa',
    tag: 'pressure',
    decimals: 2,
    isCustom: false,
  },
]

const NAV_ITEMS = ['My Dashboard', 'IoT Devices', 'Alerts']
const METRIC_OPTIONS = [
  { key: 'temperature', label: 'Temperature', unit: '°C', decimals: 2 },
  { key: 'humidity', label: 'Humidity', unit: '%', decimals: 2 },
  { key: 'soil_moisture', label: 'Soil Moisture', unit: '%', decimals: 2 },
  { key: 'light_intensity', label: 'Light Intensity', unit: 'lux', decimals: 0 },
  { key: 'pressure', label: 'Pressure', unit: 'hPa', decimals: 2 },
]
const STALE_AFTER_MS = 20000

const createInitialState = (devices) =>
  devices.reduce((accumulator, device) => {
    accumulator[device.id] = {
      value: null,
      lastUpdate: 0,
      active: false,
    }
    return accumulator
  }, {})

const normalizeText = (value = '') =>
  value
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')

const toFiniteNumber = (value) => {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

const getDefaultUnit = (metric) => {
  const option = METRIC_OPTIONS.find((item) => item.key === metric)
  return option ? option.unit : ''
}

const getDefaultDecimals = (metric) => {
  const option = METRIC_OPTIONS.find((item) => item.key === metric)
  return option ? option.decimals : 2
}

const getWebSocketUrl = () => {
  const configuredUrl = import.meta.env.VITE_WS_URL?.trim()
  if (configuredUrl) {
    return configuredUrl
  }

  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  const backendPort = import.meta.env.VITE_BACKEND_PORT || '8000'
  return `${protocol}//${window.location.hostname}:${backendPort}/ws`
}

const getAnalyticsApiBase = () => {
  const configuredUrl = import.meta.env.VITE_ANALYTICS_API_URL?.trim()
  if (configuredUrl) {
    return configuredUrl.replace(/\/+$/, '')
  }

  const protocol = window.location.protocol
  const backendPort = import.meta.env.VITE_BACKEND_PORT || '8000'
  return `${protocol}//${window.location.hostname}:${backendPort}/api/analytics`
}

const padTwoDigits = (value) => String(value).padStart(2, '0')

const parseTimestampParts = (value) => {
  if (typeof value !== 'string') {
    return null
  }

  const match = value
    .trim()
    .match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?/)

  if (!match) {
    return null
  }

  return {
    year: Number(match[1]),
    month: Number(match[2]),
    day: Number(match[3]),
    hour: Number(match[4]),
    minute: Number(match[5]),
    second: Number(match[6] || '0'),
  }
}

const toWallClockDate = (value) => {
  const parts = parseTimestampParts(value)
  if (parts) {
    return new Date(
      parts.year,
      parts.month - 1,
      parts.day,
      parts.hour,
      parts.minute,
      parts.second,
      0,
    )
  }

  const parsed = new Date(value)
  return Number.isFinite(parsed.getTime()) ? parsed : null
}

const toMinuteKey = (date) =>
  `${date.getFullYear()}-${padTwoDigits(date.getMonth() + 1)}-${padTwoDigits(date.getDate())}T${padTwoDigits(date.getHours())}:${padTwoDigits(date.getMinutes())}:00`

const toSortableTime = (value) => {
  const parsed = toWallClockDate(value)
  if (!parsed) {
    return Number.NEGATIVE_INFINITY
  }
  return parsed.getTime()
}

const formatMinuteLabel = (value) =>
  (() => {
    const parts = parseTimestampParts(value)
    if (parts) {
      return `${padTwoDigits(parts.hour)}:${padTwoDigits(parts.minute)}`
    }

    const parsed = toWallClockDate(value)
    if (!parsed) {
      return '--:--'
    }

    return `${padTwoDigits(parsed.getHours())}:${padTwoDigits(parsed.getMinutes())}`
  })()

const aggregateRowsToMinutely = (rows, lookbackMinutes = 120) => {
  const cutoffMs = Date.now() - lookbackMinutes * 60 * 1000
  const buckets = new Map()

  rows.forEach((row) => {
    const rawTs = row?.event_ts
    const ts = toWallClockDate(rawTs)
    if (!ts) {
      return
    }

    const tsMs = ts.getTime()
    const metricValue = Number(row?.metric_value)

    if (!Number.isFinite(tsMs) || tsMs < cutoffMs || !Number.isFinite(metricValue)) {
      return
    }

    ts.setSeconds(0, 0)
    const key = toMinuteKey(ts)

    const existing = buckets.get(key) || {
      minute_ts: key,
      sum: 0,
      count: 0,
      unit: row?.unit || '',
    }

    existing.sum += metricValue
    existing.count += 1

    buckets.set(key, existing)
  })

  return Array.from(buckets.values())
    .map((item) => ({
      minute_ts: item.minute_ts,
      avg_value: item.count > 0 ? item.sum / item.count : null,
      sample_count: item.count,
      unit: item.unit,
    }))
    .filter((item) => Number.isFinite(item.avg_value))
    .sort((a, b) => toSortableTime(a.minute_ts) - toSortableTime(b.minute_ts))
}

const buildAutoCardsFromPayload = (payload, devices) => {
  const sensorIdRaw = String(payload.sensor_id || '').trim()
  const sensorIdKey = normalizeText(sensorIdRaw)
  const location = String(payload.location || 'Unknown').trim() || 'Unknown'

  if (!sensorIdKey) {
    return {
      newCards: [],
      targetIds: [],
    }
  }

  const newCards = []
  const targetIds = []

  METRIC_OPTIONS.forEach((metric) => {
    const metricValue = toFiniteNumber(payload[metric.key])
    if (metricValue === null) {
      return
    }

    const existingCard = devices.find((device) => {
      if (device.dataKey !== metric.key) {
        return false
      }

      const sourceKey = normalizeText(device.source)
      return sourceKey && sourceKey === sensorIdKey
    })

    if (existingCard) {
      targetIds.push(existingCard.id)
      return
    }

    const autoId = `auto-${sensorIdKey}-${metric.key}`
    targetIds.push(autoId)

    const hasAutoCard = devices.some((device) => device.id === autoId)
    if (hasAutoCard) {
      return
    }

    newCards.push({
      id: autoId,
      title: `${sensorIdRaw} ${metric.label}`,
      source: sensorIdRaw,
      location,
      dataKey: metric.key,
      unit: metric.unit,
      tag: metric.key,
      decimals: metric.decimals,
      isCustom: false,
      isAuto: true,
    })
  })

  return {
    newCards,
    targetIds,
  }
}

const findTargetCardIds = (payload, devices) => {
  const targets = new Set()
  const sensorKey = normalizeText(payload.sensor_id)
  const locationKey = normalizeText(payload.location)

  devices.forEach((device) => {
    const metricValue = toFiniteNumber(payload[device.dataKey])
    if (metricValue === null) {
      return
    }

    const sourceKey = normalizeText(device.source)
    const deviceLocationKey = normalizeText(device.location)

    if (sourceKey) {
      if (sourceKey === sensorKey) {
        targets.add(device.id)
      }
      return
    }

    if (deviceLocationKey && deviceLocationKey === locationKey) {
      targets.add(device.id)
    }
  })

  return Array.from(targets)
}

const formatValue = (value, decimals = 2) => {
  if (value === null) {
    return 'N/A'
  }

  if (!Number.isFinite(value)) {
    return 'N/A'
  }

  return value.toFixed(decimals)
}

const Dashboard = () => {
  const [customDevices, setCustomDevices] = useState([])
  const [autoDevices, setAutoDevices] = useState([])
  const [activeNavItem, setActiveNavItem] = useState('IoT Devices')
  const allDevices = useMemo(
    () => [...BASE_DEVICE_CONFIG, ...customDevices, ...autoDevices],
    [customDevices, autoDevices],
  )
  const [deviceState, setDeviceState] = useState(() => createInitialState(BASE_DEVICE_CONFIG))
  const [isConnected, setIsConnected] = useState(false)
  const [messageCount, setMessageCount] = useState(0)
  const [showAddForm, setShowAddForm] = useState(false)
  const [historyModal, setHistoryModal] = useState({ open: false, device: null })
  const [historyData, setHistoryData] = useState([])
  const [historyLoading, setHistoryLoading] = useState(false)
  const [historyError, setHistoryError] = useState('')
  const [newDevice, setNewDevice] = useState({
    title: '',
    source: '',
    location: '',
    dataKey: 'temperature',
    unit: getDefaultUnit('temperature'),
  })
  const wsRef = useRef(null)
  const reconnectTimerRef = useRef(null)
  const allDevicesRef = useRef(allDevices)
  const socketUrl = useMemo(() => getWebSocketUrl(), [])
  const analyticsApiBase = useMemo(() => getAnalyticsApiBase(), [])

  useEffect(() => {
    allDevicesRef.current = allDevices
  }, [allDevices])

  const resetNewDevice = () => {
    setNewDevice({
      title: '',
      source: '',
      location: '',
      dataKey: 'temperature',
      unit: getDefaultUnit('temperature'),
    })
  }

  const handleNewDeviceFieldChange = (event) => {
    const { name, value } = event.target

    if (name === 'dataKey') {
      setNewDevice((previousState) => ({
        ...previousState,
        dataKey: value,
        unit: getDefaultUnit(value),
      }))
      return
    }

    setNewDevice((previousState) => ({
      ...previousState,
      [name]: value,
    }))
  }

  const handleAddDevice = (event) => {
    event.preventDefault()

    const title = newDevice.title.trim()
    const location = newDevice.location.trim()
    const source = newDevice.source.trim()

    if (!title) {
      return
    }

    const deviceId = `${normalizeText(title) || 'device'}-${Date.now()}`
    const addedDevice = {
      id: deviceId,
      title,
      source,
      location: location || 'Unknown',
      dataKey: newDevice.dataKey,
      unit: newDevice.unit.trim() || getDefaultUnit(newDevice.dataKey),
      tag: newDevice.dataKey,
      decimals: getDefaultDecimals(newDevice.dataKey),
      isCustom: true,
    }

    setCustomDevices((previousDevices) => [...previousDevices, addedDevice])
    setDeviceState((previousState) => ({
      ...previousState,
      [deviceId]: {
        value: null,
        lastUpdate: 0,
        active: false,
      },
    }))

    setShowAddForm(false)
    resetNewDevice()
  }

  const handleDeleteCustomDevice = (deviceId) => {
    setCustomDevices((previousDevices) =>
      previousDevices.filter((device) => device.id !== deviceId),
    )

    setDeviceState((previousState) => {
      const nextState = { ...previousState }
      delete nextState[deviceId]
      return nextState
    })
  }

  useEffect(() => {
    setDeviceState((previousState) => {
      let changed = false
      const nextState = { ...previousState }

      allDevices.forEach((device) => {
        if (nextState[device.id]) {
          return
        }

        nextState[device.id] = {
          value: null,
          lastUpdate: 0,
          active: false,
        }
        changed = true
      })

      return changed ? nextState : previousState
    })
  }, [allDevices])

  useEffect(() => {
    let stillMounted = true

    const connect = () => {
      try {
        wsRef.current = new WebSocket(socketUrl)

        wsRef.current.onopen = () => {
          if (stillMounted) {
            setIsConnected(true)
          }
        }

        wsRef.current.onmessage = (event) => {
          try {
            const payload = JSON.parse(event.data)
            const currentDevices = allDevicesRef.current
            const inferred = buildAutoCardsFromPayload(payload, currentDevices)
            const targetCardIds = Array.from(
              new Set([...findTargetCardIds(payload, currentDevices), ...inferred.targetIds]),
            )

            if (inferred.newCards.length > 0) {
              setAutoDevices((previousDevices) => {
                const existingIds = new Set(previousDevices.map((device) => device.id))
                const cardsToAdd = inferred.newCards.filter((device) => !existingIds.has(device.id))

                if (cardsToAdd.length === 0) {
                  return previousDevices
                }

                return [...previousDevices, ...cardsToAdd]
              })
            }

            setMessageCount((count) => count + 1)

            if (targetCardIds.length === 0) {
              return
            }

            setDeviceState((previousState) => {
              const nextState = { ...previousState }
              const now = Date.now()
              const deviceLookup = [...allDevicesRef.current, ...inferred.newCards]

              targetCardIds.forEach((cardId) => {
                const cardConfig = deviceLookup.find((device) => device.id === cardId)
                if (!cardConfig) {
                  return
                }

                const numericValue = toFiniteNumber(payload[cardConfig.dataKey])
                if (numericValue === null) {
                  return
                }

                if (!nextState[cardId]) {
                  nextState[cardId] = {
                    value: null,
                    lastUpdate: 0,
                    active: false,
                  }
                }

                nextState[cardId] = {
                  value: numericValue,
                  lastUpdate: now,
                  active: true,
                }
              })

              return nextState
            })
          } catch (error) {
            console.error('WebSocket payload error:', error)
          }
        }

        wsRef.current.onerror = () => {
          if (stillMounted) {
            setIsConnected(false)
          }
        }

        wsRef.current.onclose = () => {
          if (!stillMounted) {
            return
          }

          setIsConnected(false)
          reconnectTimerRef.current = setTimeout(connect, 3000)
        }
      } catch (error) {
        console.error('WebSocket connection error:', error)
        setIsConnected(false)
        reconnectTimerRef.current = setTimeout(connect, 3000)
      }
    }

    connect()

    return () => {
      stillMounted = false
      if (reconnectTimerRef.current) {
        clearTimeout(reconnectTimerRef.current)
      }
      if (wsRef.current) {
        wsRef.current.close()
      }
    }
  }, [socketUrl])

  useEffect(() => {
    const staleCheck = setInterval(() => {
      const now = Date.now()

      setDeviceState((previousState) => {
        let changed = false
        const nextState = { ...previousState }

        allDevices.forEach((device) => {
          const entry = previousState[device.id]
          if (!entry) {
            return
          }

          if (!entry.active) {
            return
          }

          if (now - entry.lastUpdate > STALE_AFTER_MS) {
            nextState[device.id] = {
              ...entry,
              active: false,
            }
            changed = true
          }
        })

        return changed ? nextState : previousState
      })
    }, 3000)

    return () => {
      clearInterval(staleCheck)
    }
  }, [allDevices])

  const visibleDevices = useMemo(
    () => allDevices.filter((device) => (deviceState[device.id]?.lastUpdate || 0) > 0),
    [allDevices, deviceState],
  )

  const activeDeviceCount = useMemo(
    () => visibleDevices.filter((device) => deviceState[device.id]?.active).length,
    [visibleDevices, deviceState],
  )

  const inactiveDeviceCount = visibleDevices.length - activeDeviceCount

  const historyStats = useMemo(() => {
    if (!historyData.length) {
      return null
    }

    const values = historyData
      .map((point) => Number(point.value))
      .filter((value) => Number.isFinite(value))

    if (!values.length) {
      return null
    }

    const min = Math.min(...values)
    const max = Math.max(...values)
    const avg = values.reduce((sum, value) => sum + value, 0) / values.length

    return { min, max, avg }
  }, [historyData])

  const closeHistoryModal = () => {
    setHistoryModal({ open: false, device: null })
    setHistoryData([])
    setHistoryError('')
    setHistoryLoading(false)
  }

  const openHistoryModal = async (device) => {
    setHistoryModal({ open: true, device })
    setHistoryData([])
    setHistoryError('')
    setHistoryLoading(true)

    if (!device?.source || !device?.dataKey) {
      setHistoryError('Thiết bị chưa có sensor hoặc metric hợp lệ để truy vấn lịch sử')
      setHistoryLoading(false)
      return
    }

    try {
      const lookbackMinutes = '120'
      const params = new URLSearchParams({
        sensor_id: device.source,
        metric_type: device.dataKey,
        lookback_minutes: lookbackMinutes,
      })

      let sourceRows = []

      // Ưu tiên endpoint mới. Nếu backend đang chạy bản cũ (404) thì fallback.
      const recentResponse = await fetch(
        `${analyticsApiBase}/recent-minutely?${params.toString()}`,
      )

      if (recentResponse.ok) {
        const payload = await recentResponse.json()
        if (payload.status !== 'ok') {
          throw new Error(payload.message || 'Không thể lấy dữ liệu lịch sử')
        }
        sourceRows = payload.data || []
      } else {
        const fallbackParams = new URLSearchParams({
          sensor_id: device.source,
          metric_type: device.dataKey,
          limit: '10000',
        })

        const fallbackResponse = await fetch(
          `${analyticsApiBase}/measurements?${fallbackParams.toString()}`,
        )

        if (!fallbackResponse.ok) {
          throw new Error(`HTTP ${fallbackResponse.status}`)
        }

        const fallbackPayload = await fallbackResponse.json()
        if (fallbackPayload.status !== 'ok') {
          throw new Error(fallbackPayload.message || 'Không thể lấy dữ liệu fallback')
        }

        sourceRows = aggregateRowsToMinutely(fallbackPayload.data || [], Number(lookbackMinutes))
      }

      const normalized = sourceRows
        .map((item) => {
          const value = Number(item.avg_value)
          if (!Number.isFinite(value)) {
            return null
          }

          return {
            time: formatMinuteLabel(item.minute_ts),
            value,
            sampleCount: Number(item.sample_count) || 0,
            rawMinuteTs: item.minute_ts,
          }
        })
        .filter(Boolean)
        .sort((a, b) => toSortableTime(a.rawMinuteTs) - toSortableTime(b.rawMinuteTs))

      if (normalized.length === 0) {
        setHistoryError('Không có dữ liệu trong 2 giờ gần nhất trên Databricks')
      }

      setHistoryData(normalized)
    } catch (error) {
      console.error('History chart error:', error)
      setHistoryError('Không thể tải chart lịch sử từ Databricks')
    } finally {
      setHistoryLoading(false)
    }
  }

  const HistoryTooltip = ({ active, payload }) => {
    if (!active || !payload || payload.length === 0) {
      return null
    }

    const point = payload[0].payload
    return (
      <div className="history-tooltip">
        <p>{point.time}</p>
        <p>
          Avg: {point.value.toFixed(historyModal.device?.decimals || 2)} {historyModal.device?.unit}
        </p>
        <p>Samples: {point.sampleCount}</p>
      </div>
    )
  }

  return (
    <div className="dashboard-shell">
      <aside className="sidebar">
        <div className="brand-wrap">
          <div className="brand-icon" aria-hidden="true">
            <svg viewBox="0 0 24 24" role="img">
              <path d="M4 18h16" />
              <path d="M7 15V9" />
              <path d="M12 15V6" />
              <path d="M17 15v-4" />
            </svg>
          </div>
          <div>
            <h1>MetricsPulse</h1>
            <p>PRO V2.0</p>
          </div>
        </div>

        <div className={`backend-badge ${isConnected ? 'online' : 'offline'}`}>
          <span className="badge-dot" aria-hidden="true"></span>
          {isConnected ? 'Backend Online' : 'Backend Offline'}
        </div>

        <nav className="nav-list" aria-label="Main navigation">
          {NAV_ITEMS.map((item) => (
            <button
              type="button"
              key={item}
              className={`nav-item ${item === activeNavItem ? 'active' : ''}`}
              onClick={() => {
                setActiveNavItem(item)
                if (item !== 'IoT Devices') {
                  setShowAddForm(false)
                }
              }}
            >
              <span className="nav-ring" aria-hidden="true"></span>
              {item}
            </button>
          ))}
        </nav>

        <div className="sidebar-footer">
          <p>Logged in as</p>
          <strong>user</strong>
          <small>Version 2.0.0</small>
        </div>
      </aside>

      <main className="workspace">
        <div className="workspace-header">
          <p className="workspace-subtitle">
            {activeNavItem === 'My Dashboard' && 'Analyze historical data from Databricks'}
            {activeNavItem === 'IoT Devices' && 'Manage your IoT sensors and devices'}
            {activeNavItem === 'Alerts' && 'Monitor device status and activity'}
          </p>
          {activeNavItem === 'IoT Devices' ? (
            <button
              type="button"
              className="add-device-button"
              onClick={() => {
                setShowAddForm((previousState) => !previousState)
              }}
            >
              + Add Device
            </button>
          ) : null}
        </div>

        {activeNavItem === 'My Dashboard' ? <Analytics /> : null}

        {activeNavItem === 'Alerts' ? (
          <div className="no-device-data">
            <h3>System Status</h3>
            <p>Total devices with data: {visibleDevices.length}</p>
            <p>Active devices: {activeDeviceCount}</p>
            <p>Inactive devices: {inactiveDeviceCount}</p>
          </div>
        ) : null}

        {activeNavItem === 'IoT Devices' ? (
          <>
            {showAddForm ? (
              <form className="add-device-form" onSubmit={handleAddDevice}>
                <label className="add-field">
                  Device name
                  <input
                    type="text"
                    name="title"
                    placeholder="e.g. Garage Temperature"
                    value={newDevice.title}
                    onChange={handleNewDeviceFieldChange}
                    required
                  />
                </label>

                <label className="add-field">
                  Sensor ID
                  <input
                    type="text"
                    name="source"
                    placeholder="e.g. sensor_2"
                    value={newDevice.source}
                    onChange={handleNewDeviceFieldChange}
                  />
                </label>

                <label className="add-field">
                  Location
                  <input
                    type="text"
                    name="location"
                    placeholder="e.g. Garage"
                    value={newDevice.location}
                    onChange={handleNewDeviceFieldChange}
                  />
                </label>

                <label className="add-field">
                  Metric
                  <select
                    name="dataKey"
                    value={newDevice.dataKey}
                    onChange={handleNewDeviceFieldChange}
                  >
                    {METRIC_OPTIONS.map((option) => (
                      <option key={option.key} value={option.key}>
                        {option.label}
                      </option>
                    ))}
                  </select>
                </label>

                <label className="add-field">
                  Unit
                  <input
                    type="text"
                    name="unit"
                    placeholder="Unit"
                    value={newDevice.unit}
                    onChange={handleNewDeviceFieldChange}
                  />
                </label>

                <div className="add-form-actions">
                  <button
                    type="button"
                    className="cancel-device-button"
                    onClick={() => {
                      setShowAddForm(false)
                      resetNewDevice()
                    }}
                  >
                    Cancel
                  </button>
                  <button type="submit" className="save-device-button">
                    Save Device
                  </button>
                </div>
              </form>
            ) : null}

            {visibleDevices.length === 0 ? (
              <div className="no-device-data">
                <h3>Chưa có thiết bị nào gửi dữ liệu</h3>
                <p>
                  Dashboard sẽ tự động hiện device-card khi backend nhận được dữ liệu từ thiết bị.
                </p>
              </div>
            ) : null}

            <section className="device-grid" aria-label="Device cards">
              {visibleDevices.map((device) => {
                const state =
                  deviceState[device.id] || {
                    value: null,
                    active: false,
                  }
                const value = formatValue(state.value, device.decimals)

                return (
                  <article
                    className="device-card clickable"
                    key={device.id}
                    role="button"
                    tabIndex={0}
                    onClick={() => {
                      openHistoryModal(device)
                    }}
                    onKeyDown={(event) => {
                      if (event.key === 'Enter' || event.key === ' ') {
                        event.preventDefault()
                        openHistoryModal(device)
                      }
                    }}
                  >
                    <div className="card-top">
                      <h2>{device.title}</h2>
                      <span className="top-chip">{device.tag}</span>
                    </div>

                    <p className="source-label">Source:</p>
                    <p className="source-meta">{device.source}</p>
                    <p className="source-value">
                      <span className="source-home" aria-hidden="true"></span>
                      {device.location}
                    </p>

                    <div className="value-box">
                      <p>Real-time Value</p>
                      <div className="value-line">
                        <strong>{value}</strong>
                        {device.unit ? <span>{device.unit}</span> : null}
                      </div>
                    </div>

                    <p className={`card-state ${state.active ? 'active' : 'inactive'}`}>
                      <span className="state-dot" aria-hidden="true"></span>
                      {state.active ? 'Active' : 'Inactive'}
                    </p>

                    <p className="history-hint">Click to view 2-hour Databricks chart</p>

                    <div className="card-actions">
                      <button
                        type="button"
                        className="action-button alerts"
                        onClick={(event) => {
                          event.stopPropagation()
                        }}
                      >
                        Alerts
                      </button>
                      <button
                        type="button"
                        className="action-button edit"
                        disabled
                        onClick={(event) => {
                          event.stopPropagation()
                        }}
                      >
                        Edit
                      </button>
                      <button
                        type="button"
                        className="action-button delete"
                        onClick={(event) => {
                          event.stopPropagation()
                          if (device.isCustom) {
                            handleDeleteCustomDevice(device.id)
                          }
                        }}
                        disabled={!device.isCustom}
                      >
                        Delete
                      </button>
                    </div>
                  </article>
                )
              })}
            </section>
          </>
        ) : null}

        {historyModal.open ? (
          <div
            className="history-modal-backdrop"
            onClick={() => {
              closeHistoryModal()
            }}
          >
            <section
              className="history-modal"
              role="dialog"
              aria-modal="true"
              aria-label="Device history chart"
              onClick={(event) => {
                event.stopPropagation()
              }}
            >
              <div className="history-modal-head">
                <div>
                  <h3>{historyModal.device?.title} • 2 giờ gần nhất</h3>
                  <p>
                    {historyModal.device?.source} • {historyModal.device?.dataKey}
                  </p>
                </div>
                <button
                  type="button"
                  className="history-close-btn"
                  onClick={() => {
                    closeHistoryModal()
                  }}
                >
                  ×
                </button>
              </div>

              {historyLoading ? (
                <div className="history-loading">Đang tải dữ liệu từ Databricks...</div>
              ) : null}

              {!historyLoading && historyError ? (
                <div className="history-error">{historyError}</div>
              ) : null}

              {!historyLoading && !historyError && historyData.length > 0 ? (
                <div className="history-chart-wrap">
                  <ResponsiveContainer width="100%" height={320}>
                    <LineChart data={historyData} margin={{ top: 12, right: 16, left: 0, bottom: 8 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="rgba(112, 138, 198, 0.22)" />
                      <XAxis
                        dataKey="time"
                        tick={{ fontSize: 11, fill: '#94a8d8' }}
                        axisLine={{ stroke: 'rgba(112, 138, 198, 0.35)' }}
                        tickLine={{ stroke: 'rgba(112, 138, 198, 0.35)' }}
                        interval={9}
                      />
                      <YAxis
                        tick={{ fontSize: 12, fill: '#94a8d8' }}
                        axisLine={{ stroke: 'rgba(112, 138, 198, 0.35)' }}
                        tickLine={{ stroke: 'rgba(112, 138, 198, 0.35)' }}
                      />
                      <Tooltip content={<HistoryTooltip />} />
                      <Line
                        type="monotone"
                        dataKey="value"
                        stroke="#19dbff"
                        strokeWidth={2.2}
                        dot={{ r: 1.8, fill: '#19dbff' }}
                        activeDot={{ r: 4 }}
                        isAnimationActive={true}
                        animationDuration={450}
                        name="Avg per minute"
                      />
                    </LineChart>
                  </ResponsiveContainer>

                  {historyStats ? (
                    <div className="history-stats-grid">
                      <article className="history-stat-card">
                        <p>Min</p>
                        <strong>
                          {historyStats.min.toFixed(historyModal.device?.decimals || 2)}{' '}
                          {historyModal.device?.unit}
                        </strong>
                      </article>

                      <article className="history-stat-card">
                        <p>Max</p>
                        <strong>
                          {historyStats.max.toFixed(historyModal.device?.decimals || 2)}{' '}
                          {historyModal.device?.unit}
                        </strong>
                      </article>

                      <article className="history-stat-card">
                        <p>Avg</p>
                        <strong>
                          {historyStats.avg.toFixed(historyModal.device?.decimals || 2)}{' '}
                          {historyModal.device?.unit}
                        </strong>
                      </article>
                    </div>
                  ) : null}

                  <p className="history-note">
                    Mỗi điểm là trung bình của 1 phút trong 2 giờ gần nhất.
                  </p>
                </div>
              ) : null}
            </section>
          </div>
        ) : null}

        <footer className="workspace-footer">
          <span>{isConnected ? 'Connected' : 'Reconnecting...'}</span>
          <span>Messages: {messageCount}</span>
          <span>{activeNavItem === 'IoT Devices' ? `Socket: ${socketUrl}` : `View: ${activeNavItem}`}</span>
        </footer>
      </main>
    </div>
  )
}

export default Dashboard
