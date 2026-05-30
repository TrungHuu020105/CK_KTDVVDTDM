import { useEffect, useState } from 'react'
import { Edit3, X } from 'lucide-react'

export default function SensorEditModal({ isOpen, sensor, isLoading, onClose, onSave }) {
  const [formData, setFormData] = useState({
    name: '',
    location: '',
    location_province: '',
  })
  const [error, setError] = useState('')

  useEffect(() => {
    if (!sensor) return
    setFormData({
      name: sensor.name || '',
      location: sensor.location || '',
      location_province: sensor.location_province || '',
    })
    setError('')
  }, [sensor])

  if (!isOpen || !sensor) return null

  const handleSubmit = async (e) => {
    e.preventDefault()
    if (!formData.name.trim()) {
      setError('Tên thiết bị là bắt buộc')
      return
    }

    try {
      await onSave({
        name: formData.name.trim(),
        location: formData.location.trim() || null,
        location_province: formData.location_province.trim() || null,
      })
      setError('')
    } catch (err) {
      setError(err?.message || 'Không cập nhật được thiết bị')
    }
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4">
      <div className="w-full max-w-2xl rounded-2xl border border-cyan-400/30 bg-[#1b2346] p-5 md:p-7">
        <div className="mb-6 flex items-start justify-between gap-3">
          <h2 className="flex items-center gap-2 text-2xl font-bold text-white">
            <Edit3 className="h-7 w-7 text-cyan-300" />
            Chỉnh sửa thiết bị IoT
          </h2>
          <button onClick={onClose} className="text-gray-400 transition hover:text-white" aria-label="Close">
            <X className="h-8 w-8" />
          </button>
        </div>

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="mb-2 block text-base font-semibold text-gray-200">Tên thiết bị</label>
            <input
              value={formData.name}
              onChange={(e) => setFormData((prev) => ({ ...prev, name: e.target.value }))}
              className="w-full rounded-xl border border-slate-600 bg-[#030f3a] px-4 py-3 text-base text-white outline-none focus:border-cyan-300"
            />
          </div>

          <div>
            <label className="mb-2 block text-base font-semibold text-gray-200">Location</label>
            <input
              value={formData.location}
              onChange={(e) => setFormData((prev) => ({ ...prev, location: e.target.value }))}
              placeholder="Ví dụ: Phòng khách"
              className="w-full rounded-xl border border-slate-600 bg-[#030f3a] px-4 py-3 text-base text-white outline-none focus:border-cyan-300"
            />
          </div>

          <div>
            <label className="mb-2 block text-base font-semibold text-gray-200">Province</label>
            <input
              value={formData.location_province}
              onChange={(e) => setFormData((prev) => ({ ...prev, location_province: e.target.value }))}
              placeholder="Ví dụ: Ho Chi Minh City"
              className="w-full rounded-xl border border-slate-600 bg-[#030f3a] px-4 py-3 text-base text-white outline-none focus:border-cyan-300"
            />
          </div>

          {error && <p className="rounded-xl border border-red-400/40 bg-red-500/10 px-4 py-3 text-sm text-red-200">{error}</p>}

          <div className="grid grid-cols-2 gap-3 pt-1">
            <button type="button" onClick={onClose} className="rounded-xl bg-slate-600/60 py-3 text-base font-semibold text-gray-200 transition hover:bg-slate-500/70">
              Hủy
            </button>
            <button
              type="submit"
              disabled={isLoading}
              className="rounded-xl border border-cyan-300/60 bg-cyan-600/30 py-3 text-base font-semibold text-cyan-200 transition hover:bg-cyan-600/40 disabled:opacity-60"
            >
              {isLoading ? 'Đang cập nhật...' : 'Cập nhật'}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}
