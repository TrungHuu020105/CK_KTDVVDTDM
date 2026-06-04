import { useEffect, useMemo, useState } from 'react'
import { Thermometer, Users } from 'lucide-react'
import api from '../api'

export default function AdminIoTOverview() {
  const [rows, setRows] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  useEffect(() => {
    let cancelled = false

    const fetchSummary = async () => {
      if (document.visibilityState === 'hidden') return
      try {
        setLoading(true)
        const response = await api.get('/api/admin/iot-devices/users-summary')
        if (cancelled) return
        setRows(response.data?.users_summary || [])
        setError('')
      } catch (err) {
        if (cancelled) return
        setError(err.response?.data?.detail || 'Could not load IoT devices overview.')
      } finally {
        if (!cancelled) setLoading(false)
      }
    }

    fetchSummary()
    const interval = window.setInterval(fetchSummary, 30000)
    return () => {
      cancelled = true
      window.clearInterval(interval)
    }
  }, [])

  const sortedRows = useMemo(
    () => (
      rows
        .filter((row) => String(row.role || '').toLowerCase() !== 'admin')
        .sort((a, b) => Number(b.device_count || 0) - Number(a.device_count || 0))
    ),
    [rows]
  )

  const visibleTotals = useMemo(
    () => ({
      total_devices: sortedRows.reduce((sum, row) => sum + Number(row.device_count || 0), 0),
      total_users: sortedRows.length,
    }),
    [sortedRows]
  )

  return (
    <div className="p-6 space-y-6">
      <div>
        <h1 className="text-4xl font-bold text-white mb-2">IoT Devices Overview</h1>
        <p className="text-gray-400">Users and their device counts (Admin view - no device details)</p>
      </div>

      {error && (
        <div className="p-4 bg-red-500/10 border border-red-500/30 rounded-lg text-red-300">
          {error}
        </div>
      )}

      <div className="card-border bg-dark-800 overflow-hidden">
        <div className="grid grid-cols-[2fr_3fr_1.4fr] gap-4 px-6 py-4 border-b border-gray-700 text-sm font-semibold text-neon-cyan">
          <span>User</span>
          <span>Email</span>
          <span>Device Count</span>
        </div>

        {loading && sortedRows.length === 0 ? (
          <div className="px-6 py-10 text-gray-400">Loading overview...</div>
        ) : sortedRows.length === 0 ? (
          <div className="px-6 py-10 text-gray-400">No users found.</div>
        ) : (
          sortedRows.map((row) => (
            <div
              key={row.user_id}
              className="grid grid-cols-[2fr_3fr_1.4fr] gap-4 px-6 py-5 border-b border-gray-700/70 last:border-b-0"
            >
              <div className="min-w-0">
                <p className="text-white font-semibold truncate">{row.username || 'Unknown user'}</p>
                <p className="text-xs text-gray-500 mt-1 capitalize">{row.role || 'user'}</p>
              </div>
              <p className="text-gray-300 truncate">{row.email || '--'}</p>
              <div>
                <span className="inline-flex items-center rounded-full bg-neon-cyan/15 px-3 py-1 text-sm text-neon-cyan border border-neon-cyan/20">
                  {Number(row.device_count || 0)} devices
                </span>
              </div>
            </div>
          ))
        )}
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
        <SummaryCard
          icon={Thermometer}
          label="Total Devices"
          value={visibleTotals.total_devices}
          tone="cyan"
        />
        <SummaryCard
          icon={Users}
          label="Total Users"
          value={visibleTotals.total_users}
          tone="green"
        />
      </div>
    </div>
  )
}

function SummaryCard({ icon: Icon, label, value, tone }) {
  const colorClass = tone === 'green' ? 'text-green-300 bg-green-500/10 border-green-500/20' : 'text-neon-cyan bg-neon-cyan/10 border-neon-cyan/20'

  return (
    <div className={`card-border bg-dark-800 p-5 border ${colorClass}`}>
      <div className="flex items-center gap-3 mb-3">
        <Icon className="w-5 h-5" />
        <p className="text-sm uppercase tracking-[0.18em] text-gray-400">{label}</p>
      </div>
      <p className="text-4xl font-bold text-white">{value}</p>
    </div>
  )
}
