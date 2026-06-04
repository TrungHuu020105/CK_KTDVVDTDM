import { useEffect, useMemo, useState } from 'react'
import { Activity, Award, BarChart3, BrainCircuit, Database, MapPinned, RefreshCw, Sparkles } from 'lucide-react'
import { useDevices } from '../context/DeviceContext'
import api from '../api'
import { formatVNDateTime } from '../utils/vnTime'

const getSensorId = (sensor) => sensor?.sensor_id || sensor?.source
const fmt = (value, digits = 3) => value === null || value === undefined || Number.isNaN(Number(value)) ? '--' : Number(value).toFixed(digits)
const intFmt = (value) => value === null || value === undefined || Number.isNaN(Number(value)) ? '--' : Number(value).toLocaleString('vi-VN')
const targetLabel = (value) => value === 'temperature' ? 'Temperature' : value === 'humidity' ? 'Humidity' : value || '--'
const trainingLabel = (value) => value === 'per_location' ? 'Per-location' : value === 'global' ? 'Global' : value || '--'
const familyLabel = (value) => value === 'deep_learning' ? 'Deep Learning' : value === 'spark_mllib' ? 'Spark ML' : value || '--'
const shortTarget = (value) => value === 'temperature' ? 'Temp' : value === 'humidity' ? 'Hum' : value || '--'
const shortModelName = (value) => {
  const text = String(value || '--')
  return text.length > 18 ? `${text.slice(0, 18)}...` : text
}
function SummaryCard({ icon: Icon, title, value, detail, accent = 'cyan' }) {
  const accentClass = accent === 'yellow'
    ? 'border-neon-yellow/30 text-neon-yellow'
    : accent === 'green'
      ? 'border-green-400/30 text-green-300'
      : 'border-neon-cyan/30 text-neon-cyan'

  return (
    <div className="rounded-xl border border-gray-700 bg-dark-900/80 p-5">
      <div className="flex items-center gap-3 mb-3">
        <div className={`rounded-lg border px-3 py-2 ${accentClass}`}>
          <Icon className="w-5 h-5" />
        </div>
        <p className="text-sm text-gray-400">{title}</p>
      </div>
      <p className="text-2xl font-bold text-white mb-1">{value}</p>
      {detail && <p className="text-xs text-gray-500">{detail}</p>}
    </div>
  )
}

function WinnerCard({ title, model, accent = 'cyan' }) {
  const titleClass = accent === 'yellow' ? 'text-neon-yellow' : 'text-neon-cyan'

  return (
    <div className="rounded-xl border border-gray-700 bg-dark-900/80 p-5">
      <div className="flex items-center gap-2 mb-3">
        <Award className={`w-4 h-4 ${titleClass}`} />
        <p className={`text-sm font-semibold ${titleClass}`}>{title}</p>
      </div>
      {model ? (
        <>
          <h3 className="text-xl font-bold text-white">{model.model_name || '--'}</h3>
          <p className="text-sm text-gray-400 mt-1">{familyLabel(model.model_type)} · {trainingLabel(model.training_mode)}</p>
          <div className="grid grid-cols-3 gap-3 mt-4 text-sm">
            <div>
              <p className="text-gray-500">RMSE</p>
              <p className="text-white font-semibold">{fmt(model.rmse)}</p>
            </div>
            <div>
              <p className="text-gray-500">MAE</p>
              <p className="text-white font-semibold">{fmt(model.mae)}</p>
            </div>
            <div>
              <p className="text-gray-500">R2</p>
              <p className="text-white font-semibold">{fmt(model.r2)}</p>
            </div>
          </div>
        </>
      ) : (
        <p className="text-sm text-gray-500">Chua co winner cho chi so nay.</p>
      )}
    </div>
  )
}

function ChartCard({ title, subtitle, children }) {
  return (
    <div className="rounded-xl border border-gray-700 bg-dark-900/80 p-5">
      <h3 className="text-lg font-semibold text-white">{title}</h3>
      <p className="text-sm text-gray-500 mt-1 mb-4">{subtitle}</p>
      {children}
    </div>
  )
}

function MetricLineChart({ rows = [], metrics = [] }) {
  const [hoveredIndex, setHoveredIndex] = useState(null)

  if (!rows.length) {
    return <p className="text-sm text-gray-500">Chua co du lieu model metric de ve chart.</p>
  }

  const width = 760
  const height = 280
  const padding = { top: 20, right: 20, bottom: 60, left: 42 }
  const innerWidth = width - padding.left - padding.right
  const innerHeight = height - padding.top - padding.bottom

  const values = rows.flatMap((row) => metrics.map((metric) => Number(row[metric.key] ?? 0))).filter((value) => Number.isFinite(value))
  const minValue = values.length ? Math.min(...values) : 0
  const maxValue = values.length ? Math.max(...values) : 1
  const range = maxValue - minValue || 1

  const xForIndex = (index) => {
    if (rows.length === 1) return padding.left + innerWidth / 2
    return padding.left + (index / (rows.length - 1)) * innerWidth
  }

  const yForValue = (value) => {
    const normalized = (Number(value ?? 0) - minValue) / range
    return padding.top + innerHeight - normalized * innerHeight
  }

  const linePath = (metricKey) => rows.map((row, index) => {
    const x = xForIndex(index)
    const y = yForValue(row[metricKey])
    return `${index === 0 ? 'M' : 'L'} ${x} ${y}`
  }).join(' ')

  const ticks = Array.from({ length: 4 }, (_, index) => {
    const value = minValue + (range * index) / 3
    const y = yForValue(value)
    return { value, y }
  })
  const activeRow = hoveredIndex === null ? null : rows[hoveredIndex]
  const activeX = hoveredIndex === null ? null : xForIndex(hoveredIndex)
  const hoverBandWidth = rows.length <= 1 ? innerWidth : Math.max(42, innerWidth / rows.length)

  return (
    <div className="relative">
      <div className="flex flex-wrap gap-4 mb-4">
        {metrics.map((metric) => (
          <div key={metric.key} className="flex items-center gap-2 text-xs text-gray-300">
            <span className="inline-block w-3 h-3 rounded-full" style={{ backgroundColor: metric.stroke }} />
            <span>{metric.label}</span>
          </div>
        ))}
      </div>
      <div
        className="rounded-lg border border-gray-800 bg-dark-900/60 p-3"
        onMouseLeave={() => setHoveredIndex(null)}
      >
        <svg viewBox={`0 0 ${width} ${height}`} className="w-full h-[320px]">
          {ticks.map((tick, index) => (
            <g key={index}>
              <line x1={padding.left} y1={tick.y} x2={width - padding.right} y2={tick.y} stroke="#23335f" strokeDasharray="4 4" />
              <text x={padding.left - 10} y={tick.y + 4} textAnchor="end" fontSize="11" fill="#94a3b8">
                {fmt(tick.value)}
              </text>
            </g>
          ))}

          <line x1={padding.left} y1={padding.top} x2={padding.left} y2={height - padding.bottom} stroke="#475569" />
          <line x1={padding.left} y1={height - padding.bottom} x2={width - padding.right} y2={height - padding.bottom} stroke="#475569" />

          {metrics.map((metric) => (
            <path
              key={metric.key}
              d={linePath(metric.key)}
              fill="none"
              stroke={metric.stroke}
              strokeWidth="3"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          ))}

          {rows.map((row, index) => {
            const x = xForIndex(index)
            return (
              <rect
                key={`hover-${index}`}
                x={x - hoverBandWidth / 2}
                y={padding.top}
                width={hoverBandWidth}
                height={innerHeight}
                fill="transparent"
                onMouseEnter={() => setHoveredIndex(index)}
              />
            )
          })}

          {metrics.map((metric) => rows.map((row, index) => {
            const x = xForIndex(index)
            const y = yForValue(row[metric.key])
            return (
              <g key={`${metric.key}-${index}`}>
                <circle
                  cx={x}
                  cy={y}
                  r={hoveredIndex === index ? '6' : '5'}
                  fill={metric.stroke}
                  stroke="#0f172a"
                  strokeWidth="2"
                  onMouseEnter={() => setHoveredIndex(index)}
                />
              </g>
            )
          }))}

          {rows.map((row, index) => {
            const x = xForIndex(index)
            return (
              <text
                key={index}
                x={x}
                y={height - padding.bottom + 18}
                textAnchor="end"
                transform={`rotate(-20 ${x} ${height - padding.bottom + 18})`}
                fontSize="10"
                fill="#94a3b8"
              >
                {row.key}
              </text>
            )
          })}
        </svg>
      </div>
      {activeRow && activeX !== null && (
        <div
          className="pointer-events-none absolute z-10 min-w-56 rounded-xl border border-neon-cyan/30 bg-dark-900/95 px-4 py-3 text-xs shadow-lg"
          style={{
            left: `calc(${((activeX / width) * 100).toFixed(2)}% - 48px)`,
            top: '84px',
          }}
        >
          <p className="text-white font-semibold">{activeRow.fullLabel}</p>
          {activeRow.meta && <p className="text-gray-400 mt-1">{activeRow.meta}</p>}
          <div className="mt-2 space-y-1">
            {metrics.map((metric) => (
              <div key={metric.key} className="flex items-center justify-between gap-3">
                <span className="text-gray-400">{metric.label}</span>
                <span className="text-white">{fmt(activeRow[metric.key])}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

export default function DatabricksModelReport() {
  const { sensors, selectedSensorId, setSelectedSensorId } = useDevices()
  const [status, setStatus] = useState(null)
  const [report, setReport] = useState({ models: [] })
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  const activeSensorId = selectedSensorId || getSensorId(sensors?.[0])
  const activeSensor = sensors.find((sensor) => getSensorId(sensor) === activeSensorId)
  const models = report?.models || []

  const loadReport = async () => {
    if (!activeSensorId) return
    try {
      setLoading(true)
      setError('')
      const [statusRes, reportRes] = await Promise.all([
        api.get('/api/sensors/databricks/status').catch(() => ({ data: null })),
        api.get(`/api/sensors/${activeSensorId}/model-leaderboard`).catch(() => ({ data: { models: [] } })),
      ])
      setStatus(statusRes.data)
      setReport(reportRes.data || { models: [] })
    } catch (err) {
      setError(err.response?.data?.detail || 'Khong tai duoc bao cao mo hinh Databricks.')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    if (sensors.length && !selectedSensorId) setSelectedSensorId(getSensorId(sensors[0]))
  }, [sensors, selectedSensorId, setSelectedSensorId])

  useEffect(() => {
    loadReport()
  }, [activeSensorId])

  const derived = useMemo(() => {
    const normalized = [...models].sort((a, b) => {
      const targetCompare = String(a.target_variable || '').localeCompare(String(b.target_variable || ''))
      if (targetCompare !== 0) return targetCompare
      if (Boolean(b.is_best) !== Boolean(a.is_best)) return Number(Boolean(b.is_best)) - Number(Boolean(a.is_best))
      return Number(a.rmse ?? Number.MAX_SAFE_INTEGER) - Number(b.rmse ?? Number.MAX_SAFE_INTEGER)
    })

    const firstBest = (target) => normalized.find((row) => row.target_variable === target && row.is_best) || normalized.find((row) => row.target_variable === target) || null
    const latestCreatedAt = normalized
      .map((row) => row.created_at)
      .filter(Boolean)
      .sort((a, b) => new Date(b) - new Date(a))[0]
    const modelTypes = Array.from(new Set(normalized.map((row) => familyLabel(row.model_type))))
    const perLocationCount = normalized.filter((row) => row.training_mode === 'per_location').length
    const globalCount = normalized.filter((row) => row.training_mode === 'global').length
    const uniqueModelNames = Array.from(new Set(normalized.map((row) => row.model_name).filter(Boolean)))

    const makeRows = (rows) => rows.map((row, index) => ({
      key: `${shortModelName(row.model_name)} ${shortTarget(row.target_variable)} ${index + 1}`,
      rmse: Number(row.rmse ?? 0),
      mae: Number(row.mae ?? 0),
      r2: Number(row.r2 ?? 0),
      fullLabel: `${row.model_name || '--'} · ${targetLabel(row.target_variable)}`,
      meta: `${familyLabel(row.model_type)} · ${trainingLabel(row.training_mode)}`,
    }))

    const temperatureModels = normalized.filter((row) => row.target_variable === 'temperature')
    const humidityModels = normalized.filter((row) => row.target_variable === 'humidity')

    return {
      normalized: [...temperatureModels, ...humidityModels],
      bestTemperature: firstBest('temperature'),
      bestHumidity: firstBest('humidity'),
      latestCreatedAt,
      modelTypes,
      bestCount: normalized.filter((row) => row.is_best).length,
      perLocationCount,
      globalCount,
      uniqueModelNames,
      chartRows: makeRows([...temperatureModels.slice(0, 5), ...humidityModels.slice(0, 5)]),
      temperatureChartRows: makeRows(temperatureModels.slice(0, 6)),
      humidityChartRows: makeRows(humidityModels.slice(0, 6)),
      locationLabel: normalized.find((row) => row.location_name)?.location_name || normalized.find((row) => row.location_id)?.location_id || activeSensor?.location_query || activeSensor?.location || '--',
    }
  }, [models, activeSensor])

  return (
    <div className="min-h-screen bg-dark-900 p-8">
      <div className="flex flex-col xl:flex-row xl:items-start xl:justify-between gap-5 mb-8">
        <div>
          <div className="flex items-center gap-3 mb-2">
            <BrainCircuit className="w-7 h-7 text-neon-cyan" />
            <h1 className="text-4xl font-bold text-white">Databricks Model Report</h1>
          </div>
          <p className="text-gray-400 max-w-3xl">
            Man hinh nay dung de demo cho thay cac mo hinh da train trong Databricks, winner theo tung chi so, va cac metric danh gia nhu RMSE, MAE, R2.
          </p>
        </div>

        <button
          type="button"
          onClick={loadReport}
          className="inline-flex items-center gap-2 self-start rounded-lg border border-neon-cyan/30 bg-neon-cyan/10 px-4 py-3 text-sm text-neon-cyan hover:bg-neon-cyan/20 transition-all"
        >
          <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
          Lam moi bao cao
        </button>
      </div>

      <div className="rounded-xl border border-neon-cyan/20 bg-dark-800 p-6 mb-6">
        <div className="grid grid-cols-1 xl:grid-cols-[1.2fr_1fr_auto] gap-5 items-end">
          <label>
            <span className="block text-sm text-gray-300 mb-2">Sensor / Outdoor Location</span>
            <select
              value={activeSensorId || ''}
              onChange={(e) => setSelectedSensorId(e.target.value)}
              className="w-full bg-dark-900 border border-gray-700 rounded-lg px-4 py-3 text-white focus:outline-none focus:border-neon-cyan"
            >
              {sensors.map((sensor) => (
                <option key={getSensorId(sensor)} value={getSensorId(sensor)}>
                  {sensor.name} ({sensor.location_query || sensor.location || 'unknown location'})
                </option>
              ))}
            </select>
          </label>

          <div className="rounded-lg border border-gray-700 bg-dark-900/80 px-4 py-3">
            <p className="text-xs uppercase tracking-wide text-gray-500 mb-1">Matched Location</p>
            <p className="text-white font-semibold">{derived.locationLabel}</p>
            <p className="text-xs text-gray-500 mt-1">Scope: {report?.leaderboard_scope || 'location'}</p>
          </div>

          <div className="rounded-lg border border-gray-700 bg-dark-900/80 px-4 py-3">
            <p className="text-xs uppercase tracking-wide text-gray-500 mb-1">Latest Metrics Sync</p>
            <p className="text-white font-semibold">{derived.latestCreatedAt ? formatVNDateTime(derived.latestCreatedAt, false) : '--'}</p>
          </div>
        </div>
      </div>

      {error && (
        <div className="mb-6 rounded-lg border border-yellow-400/30 bg-yellow-500/10 px-4 py-3 text-yellow-200 text-sm">
          {error}
        </div>
      )}

      <div className="grid grid-cols-1 xl:grid-cols-4 gap-5 mb-6">
        <SummaryCard
          icon={Database}
          title="Databricks Status"
          value={status?.enabled && status?.configured ? 'Connected' : 'Unavailable'}
          detail={status ? `${status.catalog}.${status.schema}` : 'Dang tai trang thai Databricks'}
          accent={status?.enabled && status?.configured ? 'green' : 'yellow'}
        />
        <SummaryCard
          icon={Activity}
          title="Rows Dang Hien Thi"
          value={intFmt(models.length)}
          detail={`Unique models: ${intFmt(derived.uniqueModelNames.length)} · Winner rows: ${intFmt(derived.bestCount)}`}
        />
        <SummaryCard
          icon={MapPinned}
          title="Per-location / Global"
          value={`${intFmt(derived.perLocationCount)} / ${intFmt(derived.globalCount)}`}
          detail="So rows model metric theo location / global"
          accent="yellow"
        />
        <SummaryCard
          icon={Sparkles}
          title="Model Families"
          value={derived.modelTypes.length ? derived.modelTypes.join(', ') : '--'}
          detail={status?.evaluation_table || 'Databricks evaluation source'}
        />
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-2 gap-5 mb-6">
        <WinnerCard title="Best Temperature Model" model={derived.bestTemperature} />
        <WinnerCard title="Best Humidity Model" model={derived.bestHumidity} accent="yellow" />
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-2 gap-5 mb-6">
        <ChartCard
          title="Temperature Model Metrics"
          subtitle="Top model rows cho temperature. RMSE va MAE cang thap, R2 cang cao thi cang tot."
        >
          <MetricLineChart
            rows={derived.temperatureChartRows}
            metrics={[
              { key: 'rmse', label: 'RMSE', stroke: '#22d3ee' },
              { key: 'mae', label: 'MAE', stroke: '#facc15' },
              { key: 'r2', label: 'R2', stroke: '#f472b6' },
            ]}
          />
        </ChartCard>

        <ChartCard
          title="Humidity Model Metrics"
          subtitle="Top model rows cho humidity. Day la chart rieng de thay model nao giu do on dinh tot hon."
        >
          <MetricLineChart
            rows={derived.humidityChartRows}
            metrics={[
              { key: 'rmse', label: 'RMSE', stroke: '#38bdf8' },
              { key: 'mae', label: 'MAE', stroke: '#f97316' },
              { key: 'r2', label: 'R2', stroke: '#a855f7' },
            ]}
          />
        </ChartCard>
      </div>

      <div className="rounded-xl border border-neon-cyan/20 bg-dark-800 p-6">
        <div className="flex items-center justify-between gap-4 mb-5">
          <div>
            <div className="flex items-center gap-2 mb-1">
              <BarChart3 className="w-5 h-5 text-neon-cyan" />
              <h2 className="text-2xl font-bold text-white">Model Leaderboard</h2>
            </div>
            <p className="text-sm text-gray-400 mt-1">
              Dang show {intFmt(models.length)} rows model metric cho sensor nay. MAPE da duoc bo khoi UI de bang va chart gon hon.
            </p>
          </div>
          <div className="text-right text-xs text-gray-500">
            <p>Forecast table: {status?.forecast_table || '--'}</p>
            <p>Refresh cache: {status?.forecast_refresh_seconds ? `${status.forecast_refresh_seconds}s` : '--'}</p>
          </div>
        </div>

        {!models.length && !loading ? (
          <div className="rounded-lg border border-gray-700 bg-dark-900/70 px-4 py-10 text-center text-gray-500">
            Chua tim thay model metric nao cho sensor/location nay trong Databricks.
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead>
                <tr className="border-b border-gray-700 text-gray-400">
                  <th className="px-3 py-3 text-left">Best</th>
                  <th className="px-3 py-3 text-left">Target</th>
                  <th className="px-3 py-3 text-left">Model</th>
                  <th className="px-3 py-3 text-left">Family</th>
                  <th className="px-3 py-3 text-left">Training</th>
                  <th className="px-3 py-3 text-left">Location</th>
                  <th className="px-3 py-3 text-right">RMSE</th>
                  <th className="px-3 py-3 text-right">MAE</th>
                  <th className="px-3 py-3 text-right">R2</th>
                  <th className="px-3 py-3 text-right">Train</th>
                  <th className="px-3 py-3 text-right">Test</th>
                  <th className="px-3 py-3 text-left">Created</th>
                </tr>
              </thead>
              <tbody>
                {derived.normalized.map((row, index) => (
                  <tr key={`${row.target_variable}-${row.model_name}-${row.training_mode}-${row.location_id || 'global'}-${index}`} className="border-b border-gray-800/80 hover:bg-dark-900/60">
                    <td className="px-3 py-3">
                      {row.is_best ? (
                        <span className="inline-flex rounded-full border border-neon-cyan/40 bg-neon-cyan/10 px-2 py-1 text-xs text-neon-cyan">Winner</span>
                      ) : (
                        <span className="text-gray-600">-</span>
                      )}
                    </td>
                    <td className="px-3 py-3 text-white">{targetLabel(row.target_variable)}</td>
                    <td className="px-3 py-3 text-white font-medium">{row.model_name || '--'}</td>
                    <td className="px-3 py-3 text-gray-300">{familyLabel(row.model_type)}</td>
                    <td className="px-3 py-3 text-gray-300">{trainingLabel(row.training_mode)}</td>
                    <td className="px-3 py-3 text-gray-300">{row.location_name || row.location_id || 'Global'}</td>
                    <td className="px-3 py-3 text-right text-white">{fmt(row.rmse)}</td>
                    <td className="px-3 py-3 text-right text-white">{fmt(row.mae)}</td>
                    <td className="px-3 py-3 text-right text-white">{fmt(row.r2)}</td>
                    <td className="px-3 py-3 text-right text-gray-300">{intFmt(row.train_rows)}</td>
                    <td className="px-3 py-3 text-right text-gray-300">{intFmt(row.test_rows)}</td>
                    <td className="px-3 py-3 text-gray-400 whitespace-nowrap">{row.created_at ? formatVNDateTime(row.created_at, false) : '--'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  )
}
