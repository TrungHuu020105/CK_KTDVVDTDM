import { useEffect, useState } from 'react'
import { Bot, MapPin, Search, X } from 'lucide-react'
import api from '../api'

const PRIORITY_OPTIONS = ['low', 'medium', 'high']

export default function SensorAiContextModal({ isOpen, sensor, isLoading, onClose, onSave, onMessage }) {
  const [formData, setFormData] = useState({
    environment_type: 'indoor',
    task_description: '',
    priority_level: 'medium',
    action_hint: '',
    location_query: '',
    location_province: '',
    latitude: '',
    longitude: '',
  })
  const [geoLoading, setGeoLoading] = useState(false)

  useEffect(() => {
    if (!sensor) return
    setFormData({
      environment_type: sensor.environment_type || 'indoor',
      task_description: sensor.task_description || '',
      priority_level: (sensor.priority_level || 'medium').toLowerCase(),
      action_hint: sensor.action_hint || '',
      location_query: sensor.location_query || sensor.location_province || sensor.location || '',
      location_province: sensor.location_province || '',
      latitude: sensor.latitude ?? '',
      longitude: sensor.longitude ?? '',
    })
  }, [sensor])

  if (!isOpen || !sensor) return null

  const checkLocation = async () => {
    if (!formData.location_query.trim()) {
      onMessage?.('Vui lòng nhập vị trí trước', 'warning')
      return
    }

    setGeoLoading(true)
    try {
      const res = await api.post('/api/sensors/geocode', {
        location_query: formData.location_query.trim(),
      })
      const geo = res.data || {}
      setFormData((prev) => ({
        ...prev,
        latitude: geo.latitude ?? '',
        longitude: geo.longitude ?? '',
        location_province: geo.admin1 || geo.name || prev.location_province,
      }))
      onMessage?.('Kiểm tra vị trí thành công')
    } catch (err) {
      onMessage?.(err.response?.data?.detail || 'Không kiểm tra được vị trí', 'error')
    } finally {
      setGeoLoading(false)
    }
  }

  const submit = async (e) => {
    e.preventDefault()
    await onSave({
      environment_type: formData.environment_type,
      task_description: formData.task_description.trim() || null,
      priority_level: formData.priority_level,
      action_hint: formData.action_hint.trim() || null,
      location_query: formData.location_query.trim() || null,
      location_province: formData.location_province.trim() || null,
      latitude: formData.latitude === '' ? null : Number(formData.latitude),
      longitude: formData.longitude === '' ? null : Number(formData.longitude),
    })
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4">
      <div className="w-full max-w-3xl rounded-2xl border border-cyan-400/30 bg-[#1b2346] p-5 md:p-6">
        <div className="mb-6 flex items-start justify-between gap-3">
          <div>
            <h2 className="flex items-center gap-2 text-2xl font-bold text-white">
              <Bot className="h-7 w-7 text-cyan-300" />
              AI Context
            </h2>
            <p className="mt-1 text-base text-gray-400">{sensor.name}</p>
          </div>
          <button onClick={onClose} className="text-gray-400 transition hover:text-white" aria-label="Close">
            <X className="h-8 w-8" />
          </button>
        </div>

        <form onSubmit={submit} className="space-y-4">
          <div>
            <label className="mb-2 block text-base font-semibold text-gray-200">Môi trường lắp đặt</label>
            <select
              value={formData.environment_type}
              onChange={(e) => setFormData((prev) => ({ ...prev, environment_type: e.target.value }))}
              className="w-full rounded-xl border border-slate-600 bg-[#030f3a] px-4 py-3 text-base text-white outline-none focus:border-cyan-300"
            >
              <option value="indoor">Trong nhà</option>
              <option value="outdoor">Ngoài trời</option>
            </select>
          </div>

          {formData.environment_type === 'outdoor' && (
            <div className="rounded-xl border border-cyan-400/25 bg-cyan-700/10 p-4">
              <label className="mb-2 block text-base font-semibold text-gray-200">Vị trí</label>
              <div className="flex flex-col gap-3 md:flex-row">
                <input
                  value={formData.location_query}
                  onChange={(e) => setFormData((prev) => ({ ...prev, location_query: e.target.value }))}
                  placeholder="Ví dụ: Hóc Môn, Hồ Chí Minh"
                  className="flex-1 rounded-xl border border-slate-600 bg-[#030f3a] px-4 py-3 text-base text-white outline-none focus:border-cyan-300"
                />
                <button
                  type="button"
                  onClick={checkLocation}
                  disabled={geoLoading}
                  className="inline-flex items-center justify-center gap-2 rounded-xl border border-cyan-300/60 bg-cyan-600/30 px-4 py-3 text-sm font-semibold text-cyan-200 disabled:opacity-60"
                >
                  <Search className="h-4 w-4" />
                  {geoLoading ? 'Đang kiểm tra...' : 'Kiểm tra vị trí'}
                </button>
              </div>

              <div className="mt-3 grid grid-cols-1 gap-3 md:grid-cols-3">
                <input
                  value={formData.location_province}
                  onChange={(e) => setFormData((prev) => ({ ...prev, location_province: e.target.value }))}
                  placeholder="Province"
                  className="rounded-xl border border-slate-600 bg-[#030f3a] px-4 py-3 text-base text-white outline-none focus:border-cyan-300"
                />
                <input
                  value={formData.latitude}
                  onChange={(e) => setFormData((prev) => ({ ...prev, latitude: e.target.value }))}
                  placeholder="Latitude"
                  className="rounded-xl border border-slate-600 bg-[#030f3a] px-4 py-3 text-base text-white outline-none focus:border-cyan-300"
                />
                <input
                  value={formData.longitude}
                  onChange={(e) => setFormData((prev) => ({ ...prev, longitude: e.target.value }))}
                  placeholder="Longitude"
                  className="rounded-xl border border-slate-600 bg-[#030f3a] px-4 py-3 text-base text-white outline-none focus:border-cyan-300"
                />
              </div>
            </div>
          )}

          <div>
            <label className="mb-2 block text-base font-semibold text-gray-200">Mô tả nhiệm vụ sensor (tùy chọn)</label>
            <input
              value={formData.task_description}
              onChange={(e) => setFormData((prev) => ({ ...prev, task_description: e.target.value }))}
              placeholder="Ví dụ: theo dõi nhiệt độ phòng khách"
              className="w-full rounded-xl border border-slate-600 bg-[#030f3a] px-4 py-3 text-base text-white outline-none focus:border-cyan-300"
            />
          </div>

          <div>
            <label className="mb-2 block text-base font-semibold text-gray-200">Mức ưu tiên</label>
            <select
              value={formData.priority_level}
              onChange={(e) => setFormData((prev) => ({ ...prev, priority_level: e.target.value }))}
              className="w-full rounded-xl border border-slate-600 bg-[#030f3a] px-4 py-3 text-base text-white outline-none focus:border-cyan-300"
            >
              {PRIORITY_OPTIONS.map((option) => (
                <option key={option} value={option}>{option[0].toUpperCase() + option.slice(1)}</option>
              ))}
            </select>
          </div>

          <div>
            <label className="mb-2 block text-base font-semibold text-gray-200">Gợi ý hành động ban đầu (tùy chọn)</label>
            <input
              value={formData.action_hint}
              onChange={(e) => setFormData((prev) => ({ ...prev, action_hint: e.target.value }))}
              placeholder="Ví dụ: kiểm tra quạt và thông gió"
              className="w-full rounded-xl border border-slate-600 bg-[#030f3a] px-4 py-3 text-base text-white outline-none focus:border-cyan-300"
            />
          </div>

          <div className="grid grid-cols-2 gap-3 pt-1">
            <button type="button" onClick={onClose} className="rounded-xl bg-slate-600/60 py-3 text-base font-semibold text-gray-200 transition hover:bg-slate-500/70">
              Hủy
            </button>
            <button
              type="submit"
              disabled={isLoading}
              className="inline-flex items-center justify-center gap-2 rounded-xl border border-cyan-300/60 bg-cyan-600/30 py-3 text-base font-semibold text-cyan-200 transition hover:bg-cyan-600/40 disabled:opacity-60"
            >
              <MapPin className="h-5 w-5" />
              {isLoading ? 'Đang lưu...' : 'Lưu AI Context'}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}
