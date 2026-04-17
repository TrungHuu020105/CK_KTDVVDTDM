import React from 'react'
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts'

const METRIC_COLORS = {
  temperature: '#00d4ff',
  humidity: '#36b8ff',
  soil_moisture: '#38e1a5',
  light_intensity: '#ffd166',
  pressure: '#9e88ff',
}

const formatMetricLabel = (value) =>
  String(value || '')
    .split('_')
    .map((item) => item.charAt(0).toUpperCase() + item.slice(1))
    .join(' ')

export default function DatabricksChart({ data, metric }) {
  if (!data || data.length === 0) {
    return <div className="chart-empty">Không có dữ liệu để vẽ biểu đồ</div>
  }

  const color = METRIC_COLORS[metric] || '#8884d8'
  const metricLabel = formatMetricLabel(metric)

  const CustomTooltip = ({ active, payload }) => {
    if (active && payload && payload.length) {
      const item = payload[0].payload
      return (
        <div className="custom-tooltip">
          <p className="tooltip-time">{item.time}</p>
          <p className="tooltip-value">
            Value: {item.value.toFixed(2)}
          </p>
        </div>
      )
    }
    return null
  }

  return (
    <div className="chart-wrapper">
      <ResponsiveContainer width="100%" height={400}>
        <LineChart data={data} margin={{ top: 8, right: 12, left: 0, bottom: 8 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="rgba(112, 138, 198, 0.22)" />
          <XAxis
            dataKey="time"
            tick={{ fontSize: 12, fill: '#95a8d7' }}
            axisLine={{ stroke: 'rgba(112, 138, 198, 0.35)' }}
            tickLine={{ stroke: 'rgba(112, 138, 198, 0.35)' }}
          />
          <YAxis
            tick={{ fontSize: 12, fill: '#95a8d7' }}
            axisLine={{ stroke: 'rgba(112, 138, 198, 0.35)' }}
            tickLine={{ stroke: 'rgba(112, 138, 198, 0.35)' }}
          />
          <Tooltip content={<CustomTooltip />} />
          <Legend wrapperStyle={{ color: '#b4c6ee' }} />
          <Line
            type="monotone"
            dataKey="value"
            stroke={color}
            dot={{ fill: color, r: 5 }}
            activeDot={{ r: 7 }}
            isAnimationActive={true}
            animationDuration={500}
            name={metricLabel}
            strokeWidth={2}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}
