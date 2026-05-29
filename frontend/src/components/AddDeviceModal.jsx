import { useState } from 'react'
import { X, Plus, Cloud, Cpu } from 'lucide-react'

const VN_PROVINCES = [
  'An Giang', 'Ba Ria - Vung Tau', 'Bac Giang', 'Bac Kan', 'Bac Lieu', 'Bac Ninh', 'Ben Tre',
  'Binh Dinh', 'Binh Duong', 'Binh Phuoc', 'Binh Thuan', 'Ca Mau', 'Can Tho', 'Cao Bang',
  'Da Nang', 'Dak Lak', 'Dak Nong', 'Dien Bien', 'Dong Nai', 'Dong Thap', 'Gia Lai', 'Ha Giang',
  'Ha Nam', 'Ha Noi', 'Ha Tinh', 'Hai Duong', 'Hai Phong', 'Hau Giang', 'Hoa Binh', 'Hung Yen',
  'Hue', 'Khanh Hoa', 'Kien Giang', 'Kon Tum', 'Lai Chau', 'Lam Dong', 'Lang Son', 'Lao Cai',
  'Long An', 'Nam Dinh', 'Nghe An', 'Ninh Binh', 'Ninh Thuan', 'Phu Tho', 'Phu Yen',
  'Quang Binh', 'Quang Nam', 'Quang Ngai', 'Quang Ninh', 'Quang Tri', 'Soc Trang', 'Son La',
  'Tay Ninh', 'Thai Binh', 'Thai Nguyen', 'Thanh Hoa', 'Tien Giang', 'Ho Chi Minh City',
  'Tra Vinh', 'Tuyen Quang', 'Vinh Long', 'Vinh Phuc', 'Yen Bai'
]

const normalizeSource = (value) => {
  const raw = (value || '').toLowerCase().trim()
  const match = raw.match(/^sensor[-_]?0*(\d+)$/)
  if (match) return `sensor_${Number(match[1])}`
  return raw
}

export default function AddDeviceModal({ isOpen, onClose, onAdd, isLoading }) {
  const [formData, setFormData] = useState({
    name: '',
    source: '',
    source_type: 'physical_iot',
    environment_type: 'indoor',
    location: '',
    location_province: 'Ho Chi Minh City',
  })
  const [error, setError] = useState('')

  const handleChange = (e) => {
    const { name, value } = e.target
    setFormData((prev) => {
      const next = { ...prev, [name]: value }
      if (name === 'source_type' && value === 'virtual_meteostat') next.environment_type = 'outdoor'
      return next
    })
    setError('')
  }

  const handleSubmit = async (e) => {
    e.preventDefault()
    if (!formData.name.trim()) return setError('Sensor name is required')
    if (!formData.source.trim()) return setError('Sensor ID is required')
    if (!/^[a-zA-Z0-9_-]+$/.test(formData.source)) {
      return setError('Sensor ID can only contain letters, numbers, hyphens, and underscores')
    }
    if (formData.source_type === 'virtual_meteostat' && formData.environment_type !== 'outdoor') {
      return setError('Virtual Meteostat sensors must be outdoor')
    }

    try {
      await onAdd({
        name: formData.name.trim(),
        source: normalizeSource(formData.source),
        source_type: formData.source_type,
        environment_type: formData.environment_type,
        location: formData.location.trim(),
        location_province: formData.location_province,
        location_query: `${formData.location_province}, Vietnam`,
        alert_enabled: false,
      })
      setFormData({
        name: '',
        source: '',
        source_type: 'physical_iot',
        environment_type: 'indoor',
        location: '',
        location_province: 'Ho Chi Minh City',
      })
      setError('')
      onClose()
    } catch (err) {
      setError(err.response?.data?.detail || err.message || 'Failed to add sensor')
    }
  }

  if (!isOpen) return null

  return (
    <div className="fixed inset-0 bg-black/50 backdrop-blur-sm flex items-center justify-center z-50">
      <div className="bg-dark-800 border border-neon-cyan/30 rounded-xl p-8 max-w-xl w-full mx-4">
        <div className="flex justify-between items-center mb-6">
          <h2 className="text-2xl font-bold text-white flex items-center gap-2">
            <Plus className="w-6 h-6 text-neon-cyan" />
            Add Device
          </h2>
          <button type="button" onClick={onClose} className="text-gray-400 hover:text-white transition-colors">
            <X className="w-6 h-6" />
          </button>
        </div>

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-300 mb-2">Device name</label>
            <input name="name" value={formData.name} onChange={handleChange} placeholder="Greenhouse Sensor 01" className="w-full bg-dark-900 border border-gray-700 rounded-lg px-4 py-2 text-white focus:outline-none focus:border-neon-cyan/60" />
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-300 mb-2">Device / Sensor ID</label>
            <input name="source" value={formData.source} onChange={handleChange} placeholder="esp32_devkit_v1 or virtual_meteostat_hcm" className="w-full bg-dark-900 border border-gray-700 rounded-lg px-4 py-2 text-white focus:outline-none focus:border-neon-cyan/60 font-mono" />
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <label className={`border rounded-lg p-4 cursor-pointer ${formData.source_type === 'physical_iot' ? 'border-neon-cyan bg-neon-cyan/10' : 'border-gray-700 bg-dark-900'}`}>
              <input type="radio" name="source_type" value="physical_iot" checked={formData.source_type === 'physical_iot'} onChange={handleChange} className="sr-only" />
              <div className="flex items-center gap-2 text-white"><Cpu className="w-5 h-5" /> Physical ESP32</div>
              <p className="text-xs text-gray-400 mt-2">Realtime MQTT sensor.</p>
            </label>
            <label className={`border rounded-lg p-4 cursor-pointer ${formData.source_type === 'virtual_meteostat' ? 'border-neon-cyan bg-neon-cyan/10' : 'border-gray-700 bg-dark-900'}`}>
              <input type="radio" name="source_type" value="virtual_meteostat" checked={formData.source_type === 'virtual_meteostat'} onChange={handleChange} className="sr-only" />
              <div className="flex items-center gap-2 text-white"><Cloud className="w-5 h-5" /> Virtual Meteostat</div>
              <p className="text-xs text-gray-400 mt-2">Outdoor virtual IoT source.</p>
            </label>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <div>
              <label className="block text-sm font-medium text-gray-300 mb-2">Environment</label>
              <select name="environment_type" value={formData.environment_type} onChange={handleChange} disabled={formData.source_type === 'virtual_meteostat'} className="w-full bg-dark-900 border border-gray-700 rounded-lg px-4 py-2 text-white focus:outline-none focus:border-neon-cyan/60 disabled:opacity-60">
                <option value="indoor">Indoor</option>
                <option value="outdoor">Outdoor</option>
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-300 mb-2">Province / City</label>
              <input list="vn-provinces" name="location_province" value={formData.location_province} onChange={handleChange} className="w-full bg-dark-900 border border-gray-700 rounded-lg px-4 py-2 text-white focus:outline-none focus:border-neon-cyan/60" />
              <datalist id="vn-provinces">{VN_PROVINCES.map((province) => <option key={province} value={province} />)}</datalist>
            </div>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-300 mb-2">Location label</label>
            <input name="location" value={formData.location} onChange={handleChange} placeholder="Nha kinh / Vuon rau / Phong lab" className="w-full bg-dark-900 border border-gray-700 rounded-lg px-4 py-2 text-white focus:outline-none focus:border-neon-cyan/60" />
          </div>

          <div className="rounded-lg border border-blue-400/20 bg-blue-500/10 p-3 text-xs text-blue-100">
            Each sensor stores temperature and humidity together in one sensor-level reading. Databricks receives the same reading as Bronze Lakehouse data.
          </div>

          {error && <div className="bg-red-500/10 border border-red-500/30 rounded-lg p-3"><p className="text-red-400 text-sm">{error}</p></div>}

          <div className="flex gap-3 pt-4">
            <button type="button" onClick={onClose} className="flex-1 px-4 py-2 bg-gray-700 text-white rounded-lg hover:bg-gray-600 transition-all">Cancel</button>
            <button type="submit" disabled={isLoading} className="flex-1 px-4 py-2 bg-neon-cyan/20 text-neon-cyan border border-neon-cyan/40 rounded-lg hover:border-neon-cyan transition-all disabled:opacity-50">
              {isLoading ? 'Adding...' : 'Add Device'}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}
