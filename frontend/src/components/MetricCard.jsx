import {
  Cpu, Zap, Activity, TrendingUp, BarChart3, Wifi, AlertCircle, Gauge
} from 'lucide-react'

const iconMap = {
  Cpu,
  Zap,
  Activity,
  TrendingUp,
  BarChart3,
  Wifi,
  AlertCircle,
  Gauge,
}

export default function MetricCard({
  title,
  value = 0,
  unit = '',
  icon = 'Gauge',
  color = 'neon-cyan'
}) {
  const Icon = iconMap[icon] || Gauge

  const colors = {
    'neon-cyan': '#00f0ff',
    'neon-purple': '#c400ff',
    'neon-green': '#00ff88',
    'neon-yellow': '#ffaa00',
  }

  return (
    <div className="card-border card-hover p-6 bg-dark-800 group">
      <div className="flex items-start justify-between mb-4">
        <h3 className="text-gray-400 text-sm font-semibold">{title}</h3>
        <div
          className="p-2 rounded-lg group-hover:bg-opacity-30 transition-all"
          style={{ backgroundColor: colors[color] + '20' }}
        >
          <Icon className="w-4 h-4" style={{ color: colors[color] }} />
        </div>
      </div>

      <div className="flex items-baseline gap-2">
        <span
          className="text-3xl font-bold"
          style={{ color: colors[color] }}
        >
          {typeof value === 'number' ? value.toFixed(2) : value}
        </span>
        {unit && (
          <span className="text-gray-400 text-sm">{unit}</span>
        )}
      </div>

      {/* Trend indicator */}
      <div className="mt-4 flex items-center gap-2 text-xs">
        <div
          className="w-1 h-1 rounded-full"
          style={{ backgroundColor: colors[color] }}
        ></div>
        <span className="text-gray-500">Real-time</span>
      </div>
    </div>
  )
}
