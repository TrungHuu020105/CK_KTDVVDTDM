import { useState, useEffect } from 'react'
import { Users, AlertCircle, Thermometer, Cloud, Database } from 'lucide-react'
import api from '../api'

const STAT_STYLES = {
  cyan: 'border-neon-cyan/30 text-neon-cyan bg-neon-cyan/20',
  yellow: 'border-neon-yellow/30 text-neon-yellow bg-neon-yellow/20',
  orange: 'border-neon-orange/30 text-neon-orange bg-neon-orange/20',
  green: 'border-neon-green/30 text-neon-green bg-neon-green/20',
  red: 'border-red-400/30 text-red-300 bg-red-500/20',
}

export default function AdminDashboard() {
  const [stats, setStats] = useState({
    totalUsers: 0,
    pendingUsers: 0,
    totalAlerts: 0,
    totalSensors: 0,
    databricksConfigured: false,
  })
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    fetchStats()
    const interval = setInterval(fetchStats, 30000)
    return () => clearInterval(interval)
  }, [])

  const fetchStats = async () => {
    try {
      setLoading(true)
      const [usersRes, alertsRes, sensorsRes, dbxRes] = await Promise.all([
        api.get('/api/admin/users').catch(() => ({ data: { users: [] } })),
        api.get('/api/alerts').catch(() => ({ data: { alerts: [] } })),
        api.get('/api/sensors').catch(() => ({ data: { sensors: [] } })),
        api.get('/api/sensors/databricks/status').catch(() => ({ data: { configured: false } })),
      ])

      const users = usersRes.data.users || []
      const alerts = alertsRes.data.alerts || []
      const sensors = sensorsRes.data.sensors || []

      setStats({
        totalUsers: users.length,
        pendingUsers: users.filter((u) => !u.is_approved).length,
        totalAlerts: alerts.length,
        totalSensors: sensors.length,
        databricksConfigured: !!dbxRes.data.configured,
      })
    } catch (err) {
      console.error('Failed to fetch cloud architecture stats:', err)
    } finally {
      setLoading(false)
    }
  }

  const StatCard = ({ icon: Icon, label, value, tone }) => {
    const style = STAT_STYLES[tone] || STAT_STYLES.cyan
    return (
      <div className="bg-dark-800 border border-gray-700 rounded-xl p-6 hover:border-neon-cyan/50 transition-all">
        <div className={`w-12 h-12 rounded-lg border flex items-center justify-center mb-4 ${style}`}>
          <Icon className="w-6 h-6" />
        </div>
        <p className="text-gray-400 text-sm mb-2">{label}</p>
        <p className="text-3xl font-bold text-white">{loading ? '-' : value}</p>
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-dark-900 p-8">
      <div className="mb-12">
        <h1 className="text-4xl font-bold text-white mb-2">Cloud Architecture Overview</h1>
        <p className="text-gray-400">IoT services, Databricks Lakehouse, and sensor analytics status</p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        <StatCard icon={Thermometer} label="Sensor-level Devices" value={stats.totalSensors} tone="cyan" />
        <StatCard icon={Users} label="Total Users" value={stats.totalUsers} tone="green" />
        <StatCard icon={AlertCircle} label="Pending Users" value={stats.pendingUsers} tone="yellow" />
        <StatCard icon={AlertCircle} label="Total Alerts" value={stats.totalAlerts} tone="orange" />
        <StatCard icon={Database} label="Databricks SQL" value={stats.databricksConfigured ? 'Configured' : 'Not configured'} tone={stats.databricksConfigured ? 'green' : 'red'} />
        <StatCard icon={Cloud} label="Architecture" value="Lakehouse" tone="cyan" />
      </div>
    </div>
  )
}
