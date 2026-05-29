import { createContext, useContext, useState, useEffect } from 'react'
import { useAuth } from './AuthContext'
import api from '../api'

const DeviceContext = createContext()

export function DeviceProvider({ children }) {
  const { user, token } = useAuth()
  const isDev = import.meta.env.DEV
  const [sensors, setSensors] = useState([])
  const [selectedSensorId, setSelectedSensorId] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  const fetchSensors = async () => {
    if (!token) return
    try {
      setLoading(true)
      const response = await api.get('/api/sensors')
      const nextSensors = response.data?.sensors || []
      setSensors(nextSensors)
      if (nextSensors.length > 0 && !selectedSensorId) {
        setSelectedSensorId(nextSensors[0].sensor_id || nextSensors[0].source)
      }
      setError(null)
    } catch (err) {
      console.error('Failed to fetch sensors:', err)
      setError(err.message || 'Failed to fetch sensors')
      setSensors([])
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    if (user && token) {
      if (isDev) console.log('DeviceContext: loading sensor-level devices for', user.role)
      fetchSensors()
    } else {
      setSensors([])
      setSelectedSensorId(null)
    }
  }, [user, token])

  const createSensor = async (sensorData) => {
    const response = await api.post('/api/sensors', sensorData)
    await fetchSensors()
    return response.data
  }

  const updateSensor = async (sensorId, updates) => {
    const response = await api.patch(`/api/sensors/${sensorId}`, updates)
    await fetchSensors()
    return response.data
  }

  const deleteSensor = async (sensorId) => {
    await api.delete(`/api/sensors/${sensorId}`)
    setSensors((prev) => prev.filter((sensor) => (sensor.sensor_id || sensor.source) !== sensorId))
    if (selectedSensorId === sensorId) setSelectedSensorId(null)
  }

  const value = {
    sensors,
    iotDevices: sensors,
    allIoTDevices: sensors,
    selectedSensorId,
    setSelectedSensorId,
    selectedIoTDevice: selectedSensorId,
    setSelectedIoTDevice: setSelectedSensorId,
    fetchSensors,
    fetchIoTDevices: fetchSensors,
    fetchAllIoTDevices: fetchSensors,
    createSensor,
    createIoTDevice: createSensor,
    updateSensor,
    updateIoTDevice: updateSensor,
    deleteSensor,
    deleteIoTDevice: deleteSensor,
    loading,
    error,
  }

  return <DeviceContext.Provider value={value}>{children}</DeviceContext.Provider>
}

export function useDevices() {
  const context = useContext(DeviceContext)
  if (!context) throw new Error('useDevices must be used within DeviceProvider')
  return context
}
