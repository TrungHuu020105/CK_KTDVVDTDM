import { useEffect, useMemo, useState } from 'react'
import { AlertCircle, X } from 'lucide-react'

const METRIC_OPTIONS = [
  { value: 'temperature', label: 'Nhiệt độ', unit: '°C' },
  { value: 'humidity', label: 'Độ ẩm', unit: '%' },
]

const FIELD_MAP = {
  temperature: {
    min: 'temperature_min_threshold',
    max: 'temperature_max_threshold',
  },
  humidity: {
    min: 'humidity_min_threshold',
    max: 'humidity_max_threshold',
  },
}

function pickInitialMetric(sensor) {
  if (
    sensor?.humidity_min_threshold !== null &&
    sensor?.humidity_min_threshold !== undefined &&
    sensor?.humidity_max_threshold !== null &&
    sensor?.humidity_max_threshold !== undefined
  ) {
    return 'humidity'
  }
  return 'temperature'
}

export default function SensorAlertThresholdModal({ isOpen, sensor, isLoading, onClose, onSave }) {
  const [formData, setFormData] = useState({
    alert_enabled: false,
    metric_type: 'temperature',
    temperature_min_threshold: '',
    temperature_max_threshold: '',
    humidity_min_threshold: '',
    humidity_max_threshold: '',
  })
  const [error, setError] = useState('')

  useEffect(() => {
    if (!sensor) return
    setFormData({
      alert_enabled: Boolean(sensor.alert_enabled),
      metric_type: pickInitialMetric(sensor),
      temperature_min_threshold: sensor.temperature_min_threshold ?? '',
      temperature_max_threshold: sensor.temperature_max_threshold ?? '',
      humidity_min_threshold: sensor.humidity_min_threshold ?? '',
      humidity_max_threshold: sensor.humidity_max_threshold ?? '',
    })
    setError('')
  }, [sensor])

  const activeOption = useMemo(
    () => METRIC_OPTIONS.find((option) => option.value === formData.metric_type) || METRIC_OPTIONS[0],
    [formData.metric_type],
  )

  if (!isOpen || !sensor) return null

  const minField = FIELD_MAP[formData.metric_type].min
  const maxField = FIELD_MAP[formData.metric_type].max
  const min = formData[minField] === '' ? null : Number(formData[minField])
  const max = formData[maxField] === '' ? null : Number(formData[maxField])

  const handleSubmit = async (e) => {
    e.preventDefault()

    if (formData.alert_enabled) {
      if (min === null || max === null) {
        setError(`Vui lòng nhập cả ngưỡng dưới và ngưỡng trên cho ${activeOption.label.toLowerCase()}`)
        return
      }
      if (max <= min) {
        setError('Ngưỡng trên phải lớn hơn ngưỡng dưới')
        return
      }
    }

    const payload = {
      alert_enabled: Boolean(formData.alert_enabled),
      [minField]: min,
      [maxField]: max,
    }

    try {
      await onSave(payload)
      setError('')
    } catch (err) {
      setError(err?.message || 'Không lưu được ngưỡng cảnh báo')
    }
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4">
      <div className="w-full max-w-2xl rounded-2xl border border-cyan-400/30 bg-[#1b2346] p-5 md:p-7">
        <div className="mb-6 flex items-start justify-between gap-3">
          <div>
            <h2 className="flex items-center gap-2 text-2xl font-bold text-white">
              <AlertCircle className="h-7 w-7 text-cyan-300" />
              Ngưỡng cảnh báo
            </h2>
            <p className="mt-1 text-base text-gray-400">{sensor.name}</p>
          </div>
          <button onClick={onClose} className="text-gray-400 transition hover:text-white" aria-label="Close">
            <X className="h-8 w-8" />
          </button>
        </div>

        <form onSubmit={handleSubmit} className="space-y-4">
          <label className="flex items-start gap-3">
            <input
              type="checkbox"
              checked={formData.alert_enabled}
              onChange={(e) => setFormData((prev) => ({ ...prev, alert_enabled: e.target.checked }))}
              className="mt-1 h-5 w-5"
            />
            <div>
              <p className="text-lg font-semibold text-white">Bật cảnh báo cho sensor này</p>
              <p className="text-sm text-gray-400">Gửi cảnh báo về Telegram và Email đã cấu hình khi vượt ngưỡng</p>
            </div>
          </label>

          <div>
            <label className="mb-2 block text-base font-semibold text-gray-200">Chọn độ đo</label>
            <select
              value={formData.metric_type}
              onChange={(e) => setFormData((prev) => ({ ...prev, metric_type: e.target.value }))}
              className="w-full rounded-xl border border-slate-600 bg-[#030f3a] px-4 py-3 text-base text-white outline-none focus:border-cyan-300"
            >
              {METRIC_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>{option.label}</option>
              ))}
            </select>
          </div>

          <div>
            <label className="mb-2 block text-base font-semibold text-gray-200">Ngưỡng dưới [{activeOption.unit}]</label>
            <input
              type="number"
              step="0.1"
              value={formData[minField]}
              onChange={(e) => setFormData((prev) => ({ ...prev, [minField]: e.target.value }))}
              placeholder="Tùy chọn"
              className="w-full rounded-xl border border-slate-600 bg-[#030f3a] px-4 py-3 text-base text-white outline-none focus:border-cyan-300"
            />
          </div>

          <div>
            <label className="mb-2 block text-base font-semibold text-gray-200">Ngưỡng trên [{activeOption.unit}]</label>
            <input
              type="number"
              step="0.1"
              value={formData[maxField]}
              onChange={(e) => setFormData((prev) => ({ ...prev, [maxField]: e.target.value }))}
              placeholder="Tùy chọn"
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
              {isLoading ? 'Đang lưu...' : 'Lưu ngưỡng'}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}
