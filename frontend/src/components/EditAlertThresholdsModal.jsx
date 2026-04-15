import { useState, useEffect } from 'react'
import { X, AlertCircle } from 'lucide-react'

const SENSOR_DEFAULTS = {
  temperature: { min: 15, max: 35 },
  humidity: { min: 30, max: 90 },
  soil_moisture: { min: 0, max: 100 },
  light_intensity: { min: 200, max: 900 },
  pressure: { min: 900, max: 1100 },
}

export default function EditAlertThresholdsModal({ isOpen, onClose, onUpdate, device, isLoading }) {
  const [formData, setFormData] = useState({
    alert_enabled: false,
    lower_threshold: null,
    upper_threshold: null
  })
  const [error, setError] = useState('')

  useEffect(() => {
    if (device) {
      setFormData({
        alert_enabled: device.alert_enabled || false,
        lower_threshold: device.lower_threshold || null,
        upper_threshold: device.upper_threshold || null
      })
    }
  }, [device])

  const handleChange = (e) => {
    const { name, value, type, checked } = e.target
    setFormData(prev => ({
      ...prev,
      [name]: type === 'checkbox' ? checked : value === '' ? null : parseFloat(value)
    }))
    setError('')
  }

  const handleSubmit = async (e) => {
    e.preventDefault()

    // Validation
    if (formData.alert_enabled) {
      // At least one threshold must be set
      if ((formData.lower_threshold === null || formData.lower_threshold === '') && 
          (formData.upper_threshold === null || formData.upper_threshold === '')) {
        setError('Please set at least one threshold (lower or upper)')
        return
      }
      // Upper should be higher than lower (if both set)
      if (formData.lower_threshold !== null && formData.upper_threshold !== null &&
          formData.upper_threshold <= formData.lower_threshold) {
        setError('Upper threshold must be higher than lower threshold')
        return
      }
    }

    try {
      await onUpdate(formData)
      setError('')
    } catch (err) {
      setError(err.message || 'Failed to save alert thresholds')
    }
  }

  if (!isOpen || !device) return null

  const defaults = SENSOR_DEFAULTS[device?.device_type]
  const unitMap = {
    temperature: '°C',
    humidity: '%',
    soil_moisture: '%',
    light_intensity: 'lux',
    pressure: 'hPa'
  }
  const unit = unitMap[device?.device_type] || ''

  return (
    <div className="fixed inset-0 bg-black/50 backdrop-blur-sm flex items-center justify-center z-50">
      <div className="bg-dark-800 border border-neon-cyan/30 rounded-xl p-8 max-w-md w-full mx-4">
        {/* Header */}
        <div className="flex justify-between items-center mb-6">
          <div>
            <h2 className="text-2xl font-bold text-white flex items-center gap-2">
              <AlertCircle className="w-6 h-6 text-neon-cyan" />
              Alert Thresholds
            </h2>
            <p className="text-sm text-gray-400 mt-1">{device.name}</p>
          </div>
          <button
            onClick={onClose}
            className="text-gray-400 hover:text-white transition-colors"
          >
            <X className="w-6 h-6" />
          </button>
        </div>

        {/* Form */}
        <form onSubmit={handleSubmit} className="space-y-4">
          {/* Enable Alerts */}
          <div>
            <label className="flex items-center gap-3 cursor-pointer">
              <input
                type="checkbox"
                name="alert_enabled"
                checked={formData.alert_enabled}
                onChange={handleChange}
                className="w-5 h-5 rounded border-gray-600 text-neon-cyan focus:ring-neon-cyan/30 bg-dark-900"
              />
              <span className="text-white font-medium">Enable alerts for this sensor</span>
            </label>
            <p className="text-xs text-gray-500 ml-8 mt-1">Get notifications when values exceed thresholds</p>
          </div>

          {/* Lower Threshold */}
          <div>
            <label className="block text-sm font-medium text-gray-300 mb-2">
              ⬇️ Lower Threshold {unit && `[${unit}]`}
            </label>
            <input
              type="number"
              name="lower_threshold"
              value={formData.lower_threshold === null ? '' : formData.lower_threshold}
              onChange={handleChange}
              placeholder={defaults ? `e.g., ${defaults.min}` : 'Optional - alert when value drops below this'}
              step="0.1"
              className="w-full bg-dark-900 border border-gray-700 rounded-lg px-4 py-2 text-white placeholder-gray-600 focus:outline-none focus:border-neon-cyan/60 transition-colors"
            />
            <p className="text-xs text-gray-500 mt-1">Alert triggered when value {'<'} lower threshold</p>
          </div>

          {/* Upper Threshold */}
          <div>
            <label className="block text-sm font-medium text-gray-300 mb-2">
              ⬆️ Upper Threshold {unit && `[${unit}]`}
            </label>
            <input
              type="number"
              name="upper_threshold"
              value={formData.upper_threshold === null ? '' : formData.upper_threshold}
              onChange={handleChange}
              placeholder={defaults ? `e.g., ${defaults.max}` : 'Optional - alert when value exceeds this'}
              step="0.1"
              className="w-full bg-dark-900 border border-gray-700 rounded-lg px-4 py-2 text-white placeholder-gray-600 focus:outline-none focus:border-neon-cyan/60 transition-colors"
            />
            <p className="text-xs text-gray-500 mt-1">Alert triggered when value {'>'} upper threshold</p>
          </div>

          {/* Error Message */}
          {error && (
            <div className="bg-red-500/10 border border-red-500/30 rounded-lg p-3">
              <p className="text-red-400 text-sm">{error}</p>
            </div>
          )}

          {/* Info Box */}
          {formData.alert_enabled && (
            <div className="bg-neon-cyan/5 border border-neon-cyan/20 rounded-lg p-3">
              <p className="text-xs text-gray-400">
                💡 Alert behavior:
              </p>
              <ul className="text-xs text-gray-500 mt-2 space-y-1 ml-4">
                <li>• Alert if value {'<'} lower_threshold</li>
                <li>• Alert if value {'>'} upper_threshold</li>
                <li>• Safe range: lower_threshold to upper_threshold</li>
              </ul>
            </div>
          )}

          {/* Buttons */}
          <div className="flex gap-3 pt-4">
            <button
              type="button"
              onClick={onClose}
              className="flex-1 px-4 py-2 bg-gray-700/50 text-gray-300 rounded-lg hover:bg-gray-700 transition-colors text-sm font-medium"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={isLoading}
              className="flex-1 px-4 py-2 bg-neon-cyan/20 text-neon-cyan border border-neon-cyan/40 rounded-lg hover:border-neon-cyan hover:bg-neon-cyan/30 transition-colors text-sm font-medium disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {isLoading ? 'Saving...' : 'Save Thresholds'}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}

