import { createContext, useContext, useState, useEffect } from 'react'
import api from '../api'

const DeviceContext = createContext()
const DEVICE_CACHE_KEY = 'iotDevicesCacheV1'

const readCachedDevices = () => {
  try {
    const raw = localStorage.getItem(DEVICE_CACHE_KEY)
    if (!raw) return []
    const parsed = JSON.parse(raw)
    return Array.isArray(parsed) ? parsed : []
  } catch {
    return []
  }
}

const writeCachedDevices = (devices) => {
  try {
    localStorage.setItem(DEVICE_CACHE_KEY, JSON.stringify(devices || []))
  } catch {
    // Ignore cache write errors.
  }
}

export function DeviceProvider({ children }) {
  const [iotDevices, setIotDevices] = useState(() => readCachedDevices())
  const [selectedIoTDevice, setSelectedIoTDevice] = useState(null)

  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  useEffect(() => {
    fetchIoTDevices()
  }, [])

  const fetchIoTDevices = async () => {
    try {
      setLoading(true)
      console.log('[DeviceContext] Fetching devices from Databricks...')

      // Backend /api/iot-devices is Databricks-backed now.
      const response = await api.get('/api/iot-devices')
      const devices = response.data?.devices || []

      setIotDevices(devices)
      writeCachedDevices(devices)

      if (devices.length > 0) {
        const hasSelected = devices.some((d) => String(d.id) === String(selectedIoTDevice))
        if (!selectedIoTDevice || !hasSelected) {
          setSelectedIoTDevice(devices[0].id)
        }
      }

      setError(null)
    } catch (err) {
      console.error('Failed to fetch IoT devices:', err)
      setError('Failed to load devices from Databricks')
    } finally {
      setLoading(false)
    }
  }

  // Compatibility aliases so existing user-facing components keep working.
  const fetchAllIoTDevices = fetchIoTDevices

  const createIoTDevice = async (deviceData) => {
    try {
      const response = await api.post('/api/iot-devices', deviceData)
      setIotDevices((prev) => {
        const bySource = new Map(prev.map((d) => [d.source || d.id, d]))
        bySource.set(response.data.source || response.data.id, response.data)
        const next = Array.from(bySource.values())
        writeCachedDevices(next)
        return next
      })
      return response.data
    } catch (err) {
      throw err
    }
  }

  const updateIoTDevice = async (deviceId, updates) => {
    try {
      const response = await api.put(`/api/iot-devices/${deviceId}`, updates)
      setIotDevices((prev) => {
        const next = prev.map((d) => (String(d.id) === String(deviceId) ? response.data : d))
        writeCachedDevices(next)
        return next
      })
      return response.data
    } catch (err) {
      throw err
    }
  }

  const deleteIoTDevice = async (deviceId) => {
    try {
      await api.delete(`/api/iot-devices/${deviceId}`)
      setIotDevices((prev) => {
        const next = prev.filter((d) => String(d.id) !== String(deviceId))
        writeCachedDevices(next)
        if (String(selectedIoTDevice) === String(deviceId)) {
          setSelectedIoTDevice(next.length > 0 ? next[0].id : null)
        }
        return next
      })
    } catch (err) {
      throw err
    }
  }

  const value = {
    iotDevices,
    allIoTDevices: iotDevices,
    selectedIoTDevice,
    setSelectedIoTDevice,
    createIoTDevice,
    updateIoTDevice,
    deleteIoTDevice,
    fetchIoTDevices,
    fetchAllIoTDevices,

    loading,
    error,
  }

  return (
    <DeviceContext.Provider value={value}>
      {children}
    </DeviceContext.Provider>
  )
}

export function useDevices() {
  const context = useContext(DeviceContext)
  if (!context) {
    throw new Error('useDevices must be used within DeviceProvider')
  }
  return context
}
