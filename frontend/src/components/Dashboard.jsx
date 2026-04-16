import React, { useEffect, useMemo, useRef, useState } from 'react'

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
  const allDevices = useMemo(
    () => [...BASE_DEVICE_CONFIG, ...customDevices, ...autoDevices],
    [customDevices, autoDevices],
  )
  const [deviceState, setDeviceState] = useState(() => createInitialState(BASE_DEVICE_CONFIG))
  const [isConnected, setIsConnected] = useState(false)
  const [messageCount, setMessageCount] = useState(0)
  const [showAddForm, setShowAddForm] = useState(false)
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
              className={`nav-item ${item === 'IoT Devices' ? 'active' : ''}`}
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
          <p className="workspace-subtitle">Manage your IoT sensors and devices</p>
          <button
            type="button"
            className="add-device-button"
            onClick={() => {
              setShowAddForm((previousState) => !previousState)
            }}
          >
            + Add Device
          </button>
        </div>

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
              <article className="device-card" key={device.id}>
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

                <div className="card-actions">
                  <button type="button" className="action-button alerts">
                    Alerts
                  </button>
                  <button type="button" className="action-button edit" disabled>
                    Edit
                  </button>
                  <button
                    type="button"
                    className="action-button delete"
                    onClick={() => {
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

        <footer className="workspace-footer">
          <span>{isConnected ? 'Connected' : 'Reconnecting...'}</span>
          <span>Messages: {messageCount}</span>
          <span>Socket: {socketUrl}</span>
        </footer>
      </main>
    </div>
  )
}

export default Dashboard
