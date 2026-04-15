import { useState } from 'react'
import { X, Plus } from 'lucide-react'

// Map sensor codes to device types
const SENSOR_TYPE_MAP = {
  'sensor_1': 'temperature',
  'sensor_2': 'humidity',
  'sensor_3': 'soil_moisture',
  'sensor_4': 'light_intensity',
  'sensor_5': 'pressure',
  // Custom sensor detection
  'temp': 'temperature', 'temperature': 'temperature', 'temp_': 'temperature',
  'humid': 'humidity', 'humidity': 'humidity', 'humid_': 'humidity',
  'moisture': 'soil_moisture', 'soil': 'soil_moisture', 'moisture_': 'soil_moisture',
  'light': 'light_intensity', 'lux': 'light_intensity', 'light_': 'light_intensity',
  'pressure': 'pressure', 'press': 'pressure', 'pressure_': 'pressure',
}

const AVAILABLE_SENSORS = [
  { code: 'sensor_1', name: 'Sensor 1 - Temperature', type: 'temperature', unit: '°C' },
  { code: 'sensor_2', name: 'Sensor 2 - Humidity', type: 'humidity', unit: '%' },
  { code: 'sensor_3', name: 'Sensor 3 - Soil Moisture', type: 'soil_moisture', unit: '%' },
  { code: 'sensor_4', name: 'Sensor 4 - Light Intensity', type: 'light_intensity', unit: 'lux' },
  { code: 'sensor_5', name: 'Sensor 5 - Pressure', type: 'pressure', unit: 'hPa' },
]

const getDeviceTypeFromCode = (code) => {
  if (!code) return 'temperature'
  
  const lower = code.toLowerCase()
  
  // Check predefined
  if (SENSOR_TYPE_MAP[lower]) return SENSOR_TYPE_MAP[lower]
  
  // Check if code contains keywords
  for (const [key, type] of Object.entries(SENSOR_TYPE_MAP)) {
    if (lower.includes(key)) return type
  }
  
  // Default
  return 'temperature'
}

export default function AddDeviceModal({ isOpen, onClose, onAdd, isLoading }) {
  const [formData, setFormData] = useState({
    name: '',
    location: '',
    sensorCode: ''
  })
  const [useCustomCode, setUseCustomCode] = useState(false)
  const [error, setError] = useState('')

  const handleChange = (e) => {
    const { name, value } = e.target
    setFormData(prev => ({
      ...prev,
      [name]: value
    }))
    setError('')
  }

  const handleSubmit = async (e) => {
    e.preventDefault()
    
    // Validation
    if (!formData.name.trim()) {
      setError('Device name is required')
      return
    }
    if (!formData.sensorCode.trim()) {
      setError('Sensor code is required')
      return
    }

    // Validate sensor code format
    if (!/^[a-zA-Z0-9_-]+$/.test(formData.sensorCode)) {
      setError('Sensor code can only contain letters, numbers, hyphens, and underscores')
      return
    }

    const deviceType = getDeviceTypeFromCode(formData.sensorCode)
    
    try {
      await onAdd({
        name: formData.name.trim(),
        location: formData.location.trim(),
        device_type: deviceType,
        source: formData.sensorCode.toLowerCase().trim()
      })
      
      // Reset form
      setFormData({
        name: '',
        location: '',
        sensorCode: ''
      })
      setUseCustomCode(false)
      setError('')
    } catch (err) {
      setError(err.message || 'Failed to add device')
    }
  }

  if (!isOpen) return null

  const detectedType = getDeviceTypeFromCode(formData.sensorCode)
  const typeNames = {
    temperature: '🌡️ Temperature',
    humidity: '💧 Humidity',
    soil_moisture: '🌱 Soil Moisture',
    light_intensity: '💡 Light Intensity',
    pressure: '🔵 Pressure'
  }

  return (
    <div className="fixed inset-0 bg-black/50 backdrop-blur-sm flex items-center justify-center z-50">
      <div className="bg-dark-800 border border-neon-cyan/30 rounded-xl p-8 max-w-md w-full mx-4">
        {/* Header */}
        <div className="flex justify-between items-center mb-6">
          <h2 className="text-2xl font-bold text-white flex items-center gap-2">
            <Plus className="w-6 h-6 text-neon-cyan" />
            Add IoT Device
          </h2>
          <button
            onClick={onClose}
            className="text-gray-400 hover:text-white transition-colors"
          >
            <X className="w-6 h-6" />
          </button>
        </div>

        {/* Form */}
        <form onSubmit={handleSubmit} className="space-y-4">
          {/* Device Name */}
          <div>
            <label className="block text-sm font-medium text-gray-300 mb-2">
              Device Name *
            </label>
            <input
              type="text"
              name="name"
              value={formData.name}
              onChange={handleChange}
              placeholder="e.g., Living Room Temperature"
              className="w-full bg-dark-900 border border-gray-700 rounded-lg px-4 py-2 text-white placeholder-gray-500 focus:outline-none focus:border-neon-cyan/60 transition-colors"
            />
          </div>

          {/* Location */}
          <div>
            <label className="block text-sm font-medium text-gray-300 mb-2">
              Location
            </label>
            <input
              type="text"
              name="location"
              value={formData.location}
              onChange={handleChange}
              placeholder="e.g., Living Room, Garden, etc."
              className="w-full bg-dark-900 border border-gray-700 rounded-lg px-4 py-2 text-white placeholder-gray-500 focus:outline-none focus:border-neon-cyan/60 transition-colors"
            />
          </div>

          {/* Sensor Code - Choose or Custom */}
          <div>
            <label className="block text-sm font-medium text-gray-300 mb-2">
              Sensor Code * {useCustomCode ? '(Custom)' : '(Predefined)'}
            </label>
            
            {!useCustomCode ? (
              <>
                <select
                  name="sensorCode"
                  value={formData.sensorCode}
                  onChange={handleChange}
                  className="w-full bg-dark-900 border border-gray-700 rounded-lg px-4 py-2 text-white focus:outline-none focus:border-neon-cyan/60 transition-colors"
                >
                  <option value="">Select a sensor...</option>
                  {AVAILABLE_SENSORS.map(sensor => (
                    <option key={sensor.code} value={sensor.code}>
                      {sensor.name} ({sensor.unit})
                    </option>
                  ))}
                </select>
                <button
                  type="button"
                  onClick={() => {
                    setUseCustomCode(true)
                    setFormData(prev => ({ ...prev, sensorCode: '' }))
                  }}
                  className="mt-2 text-xs text-neon-cyan hover:underline"
                >
                  → Use custom sensor code
                </button>
              </>
            ) : (
              <>
                <input
                  type="text"
                  name="sensorCode"
                  value={formData.sensorCode}
                  onChange={handleChange}
                  placeholder="e.g., bedroom_temp, garage_humidity, etc."
                  className="w-full bg-dark-900 border border-gray-700 rounded-lg px-4 py-2 text-white placeholder-gray-500 focus:outline-none focus:border-neon-cyan/60 transition-colors"
                />
                <button
                  type="button"
                  onClick={() => {
                    setUseCustomCode(false)
                    setFormData(prev => ({ ...prev, sensorCode: '' }))
                  }}
                  className="mt-2 text-xs text-gray-400 hover:text-gray-300"
                >
                  ← Use predefined sensors
                </button>
              </>
            )}
          </div>

          {/* Auto-detected Type */}
          {formData.sensorCode && (
            <div className="bg-neon-cyan/10 border border-neon-cyan/30 rounded-lg p-3">
              <p className="text-xs text-gray-400">Type will be:</p>
              <p className="text-sm font-medium text-neon-cyan">{typeNames[detectedType] || detectedType}</p>
            </div>
          )}

          {/* Error Message */}
          {error && (
            <div className="bg-red-500/10 border border-red-500/30 rounded-lg p-3">
              <p className="text-red-400 text-sm">{error}</p>
            </div>
          )}

          {/* Info */}
          <div className="bg-neon-cyan/5 border border-neon-cyan/20 rounded-lg p-3">
            <p className="text-xs text-gray-400">
              💡 Each sensor code must be unique. Sensor type will be auto-detected from the code.
            </p>
          </div>

          {/* Buttons */}
          <div className="flex gap-3 pt-4">
            <button
              type="button"
              onClick={onClose}
              className="flex-1 px-4 py-2 bg-gray-700 text-white rounded-lg hover:bg-gray-600 transition-all"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={isLoading}
              className="flex-1 px-4 py-2 bg-neon-cyan/20 text-neon-cyan border border-neon-cyan/40 rounded-lg hover:border-neon-cyan transition-all disabled:opacity-50"
            >
              {isLoading ? 'Adding...' : 'Add Device'}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}
