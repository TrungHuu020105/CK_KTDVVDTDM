import { useEffect, useState } from 'react'
import { RefreshCcw, Wifi, X } from 'lucide-react'
import api from '../api'
import { formatVNTime } from '../utils/vnTime'

function normalizeNetworks(raw) {
  if (!Array.isArray(raw)) return []
  return raw
    .map((item) => {
      if (typeof item === 'string') return { ssid: item, rssi: null }
      if (!item || typeof item !== 'object') return null
      return {
        ssid: String(item.ssid || item.SSID || '').trim(),
        rssi: item.rssi ?? item.RSSI ?? null,
      }
    })
    .filter((item) => item && item.ssid)
}

export default function SensorWifiModal({ isOpen, sensor, onClose, onMessage }) {
  const [loading, setLoading] = useState(false)
  const [status, setStatus] = useState(null)
  const [networks, setNetworks] = useState([])
  const [ssid, setSsid] = useState('')
  const [password, setPassword] = useState('')

  const sensorId = sensor?.sensor_id || sensor?.source || ''

  const loadWifiStatus = async () => {
    if (!sensorId) return
    try {
      const res = await api.get(`/api/devices/${sensorId}/wifi-status`)
      setStatus(res.data || null)
    } catch (err) {
      setStatus(null)
      onMessage?.(err.response?.data?.detail || 'Không tải được trạng thái WiFi', 'error')
    }
  }

  const loadWifiList = async () => {
    if (!sensorId) return
    try {
      const res = await api.get(`/api/devices/${sensorId}/wifi-list`)
      setNetworks(normalizeNetworks(res.data?.networks || []))
    } catch (err) {
      onMessage?.(err.response?.data?.detail || 'Không tải được danh sách WiFi', 'error')
    }
  }

  useEffect(() => {
    if (!isOpen || !sensorId) return
    setSsid('')
    setPassword('')
    setNetworks([])
    loadWifiStatus()
    loadWifiList()
  }, [isOpen, sensorId])

  if (!isOpen || !sensor) return null

  const statusText = !status
    ? 'Chưa có trạng thái'
    : status.connected
      ? `ESP32 đang kết nối WiFi: ${status.ssid || '(không rõ SSID)'}`
      : 'ESP32 chưa kết nối WiFi'

  const scanAgain = async () => {
    if (!sensorId) return

    setLoading(true)
    try {
      await api.post(`/api/devices/${sensorId}/scan-wifi`)
      onMessage?.('Đã gửi lệnh quét WiFi, chờ ESP32 phản hồi...')
      setTimeout(() => {
        loadWifiList()
        loadWifiStatus()
      }, 1500)
    } catch (err) {
      onMessage?.(err.response?.data?.detail || 'Quét WiFi thất bại', 'error')
    } finally {
      setLoading(false)
    }
  }

  const saveWifi = async () => {
    if (!ssid.trim()) {
      onMessage?.('Vui lòng nhập tên WiFi (SSID)', 'warning')
      return
    }

    setLoading(true)
    try {
      await api.post(`/api/devices/${sensorId}/wifi-config`, {
        ssid: ssid.trim(),
        password,
      })
      onMessage?.('Đã gửi cấu hình WiFi đến ESP32')
      setTimeout(() => loadWifiStatus(), 1200)
    } catch (err) {
      onMessage?.(err.response?.data?.detail || 'Không lưu được cấu hình WiFi', 'error')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4">
      <div className="w-full max-w-3xl rounded-2xl border border-cyan-400/30 bg-[#1b2346] p-5 md:p-6">
        <div className="mb-6 flex items-start justify-between gap-3">
          <div>
            <h2 className="text-2xl font-bold text-white">Cấu hình WiFi</h2>
            <p className="mt-1 text-base text-gray-400">{sensor.name} ({sensorId})</p>
          </div>
          <button onClick={onClose} className="text-gray-400 transition hover:text-white" aria-label="Close">
            <X className="h-8 w-8" />
          </button>
        </div>

        <div className="rounded-xl border border-cyan-500/30 bg-cyan-700/20 p-4">
          <p className="text-lg font-semibold text-green-200">{statusText}</p>
          <p className="mt-1 text-sm text-gray-300">
            IP: {status?.ip || '--'} • RSSI: {status?.rssi ?? '--'} dBm • WiFi đã lưu: {status?.configured_ssid || '--'}
          </p>
          <p className="mt-1 text-sm text-gray-400">Cập nhật lúc: {status?.received_at ? formatVNTime(status.received_at, true) : '--'}</p>
        </div>

        <div className="mt-6 flex items-center justify-between gap-3">
          <h3 className="text-lg text-gray-200">Danh sách WiFi gần ESP32</h3>
          <button
            onClick={scanAgain}
            disabled={loading}
            className="inline-flex items-center gap-2 rounded-xl border border-cyan-400/50 bg-cyan-600/20 px-4 py-2 text-cyan-200 disabled:opacity-60"
          >
            <RefreshCcw className="h-4 w-4" />
            Quét lại
          </button>
        </div>

        <div className="mt-4 space-y-3">
          <select
            value={ssid}
            onChange={(e) => setSsid(e.target.value)}
            className="w-full rounded-xl border border-slate-600 bg-[#030f3a] px-4 py-3 text-base text-white outline-none focus:border-cyan-300"
          >
            <option value="">Chọn WiFi từ danh sách...</option>
            {networks.map((item) => (
              <option key={`${item.ssid}-${item.rssi ?? 'na'}`} value={item.ssid}>
                {item.ssid}{item.rssi !== null ? ` (RSSI ${item.rssi})` : ''}
              </option>
            ))}
          </select>

          <input
            value={ssid}
            onChange={(e) => setSsid(e.target.value)}
            placeholder="Hoặc nhập SSID thủ công"
            className="w-full rounded-xl border border-slate-600 bg-[#030f3a] px-4 py-3 text-base text-white outline-none focus:border-cyan-300"
          />

          <input
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            placeholder="Mật khẩu WiFi (để trống nếu open network)"
            className="w-full rounded-xl border border-slate-600 bg-[#030f3a] px-4 py-3 text-base text-white outline-none focus:border-cyan-300"
          />

        </div>

        <div className="mt-6 grid grid-cols-2 gap-3">
          <button type="button" onClick={onClose} className="rounded-xl bg-slate-600/60 py-3 text-base font-semibold text-gray-200 transition hover:bg-slate-500/70">
            Hủy
          </button>
          <button
            type="button"
            onClick={saveWifi}
            disabled={loading}
            className="inline-flex items-center justify-center gap-2 rounded-xl border border-cyan-300/60 bg-cyan-600/30 py-3 text-base font-semibold text-cyan-200 transition hover:bg-cyan-600/40 disabled:opacity-60"
          >
            <Wifi className="h-5 w-5" />
            Lưu WiFi
          </button>
        </div>
      </div>
    </div>
  )
}
