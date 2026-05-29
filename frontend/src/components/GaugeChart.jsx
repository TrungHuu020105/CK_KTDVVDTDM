import { PieChart, Pie, Cell, ResponsiveContainer } from 'recharts'

export default function GaugeChart({ title, value = 0, unit = '%', color = 'neon-cyan', max = 100 }) {
  const normalizedValue = Math.min(Math.max(value, 0), max)
  const percentage = (normalizedValue / max) * 100

  const colors = {
    'neon-cyan': '#00f0ff',
    'neon-purple': '#c400ff',
    'neon-green': '#00ff88',
    'neon-yellow': '#ffaa00',
  }

  const colorValue = colors[color] || colors['neon-cyan']

  return (
    <div className="card-border card-hover p-4 bg-dark-800 relative overflow-hidden group" style={{ minHeight: '200px' }}>
      {/* Background glow effect */}
      <div className={`absolute inset-0 opacity-0 group-hover:opacity-20 transition-opacity bg-gradient-to-br blur-3xl`}
           style={{ background: `linear-gradient(135deg, ${colorValue}, transparent)` }}></div>

      <div className="relative z-10 h-full flex flex-col items-center">
        <p className="text-gray-400 text-xs font-semibold tracking-widest mb-1">{title}</p>

        <div className="flex-1 flex items-center justify-center w-full px-2">
          <svg viewBox="0 0 240 120" className="w-full" style={{ maxHeight: '110px' }}>
            {/* Background arc */}
            <path
              d="M 30 110 A 110 110 0 0 1 210 110"
              fill="none"
              stroke="rgba(100, 100, 150, 0.2)"
              strokeWidth="10"
              strokeLinecap="round"
            />
            {/* Progress arc */}
            <path
              d="M 30 110 A 110 110 0 0 1 210 110"
              fill="none"
              stroke={colorValue}
              strokeWidth="10"
              strokeLinecap="round"
              strokeDasharray={`${(percentage / 100) * 283} 283`}
              filter="drop-shadow(0 0 6px rgba(255,255,255,0.3))"
            />
          </svg>
        </div>

        {/* Value displayed below arc */}
        <div className="text-center py-1">
          <div className="text-3xl font-bold leading-tight" style={{ color: colorValue }}>
            {normalizedValue.toFixed(1)}
          </div>
          <div className="text-gray-400 text-xs">{unit}</div>
        </div>
      </div>
    </div>
  )
}
