import { useState, useEffect } from 'react'
import { Thermometer, Plus, Calendar, TrendingUp } from 'lucide-react'
import { useDevices } from '../context/DeviceContext'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'
import AddDeviceModal from './AddDeviceModal'
import api from '../api'

export default function UserDashboard() {
  const { iotDevices: devices, createIoTDevice } = useDevices()
  const [showAddDeviceModal, setShowAddDeviceModal] = useState(false)
  const [addingDevice, setAddingDevice] = useState(false)
  const [selectedDeviceId, setSelectedDeviceId] = useState(null)
  const [fromDate, setFromDate] = useState(() => {
    // Default to today (1-day view = hourly data)
    return new Date().toISOString().split('T')[0]
  })
  const [toDate, setToDate] = useState(new Date().toISOString().split('T')[0])
  const [chartData, setChartData] = useState([])
  const [loading, setLoading] = useState(false)

  const selectedDevice = devices?.find(d => d.id === selectedDeviceId) || devices?.[0]

  useEffect(() => {
    if (devices?.length > 0 && !selectedDeviceId) {
      setSelectedDeviceId(devices[0].id)
    }
  }, [devices])

  // Fetch and aggregate data
  useEffect(() => {
    if (!selectedDevice) return

    const fetchData = async () => {
      try {
        setLoading(true)
        console.log(`[UserDashboard] Fetching data for ${selectedDevice.name} from ${fromDate} to ${toDate}`)
        
        const response = await api.get('/api/metrics/history-by-date', {
          params: {
            metric_type: selectedDevice.device_type,
            source: selectedDevice.source,
            from_date: fromDate,
            to_date: toDate
          }
        })

        const metrics = response.data.data || []
        console.log(`[UserDashboard] Received ${metrics.length} metrics from API`)

        // Calculate days difference
        const from = new Date(fromDate)
        const to = new Date(toDate)
        const daysDiff = Math.ceil((to - from) / (1000 * 60 * 60 * 24)) + 1

        console.log(`[UserDashboard] Date range: ${daysDiff} days, showHourly=${daysDiff === 1}`)

        if (daysDiff === 1) {
          // Show hourly data
          console.log(`[UserDashboard] Aggregating into hourly buckets`)
          const hourlyData = {}
          metrics.forEach(m => {
            const d = new Date(m.timestamp)
            const hour = d.getHours().toString().padStart(2, '0')
            const key = `${hour}:00`
            if (!hourlyData[key]) {
              hourlyData[key] = { values: [], hour: key }
            }
            hourlyData[key].values.push(m.value)
          })

          const data = Object.values(hourlyData)
            .sort((a, b) => a.hour.localeCompare(b.hour))
            .map(h => ({
              time: h.hour,
              value: parseFloat((h.values.reduce((a, b) => a + b, 0) / h.values.length).toFixed(2))
            }))
          console.log(`[UserDashboard] Hourly data points: ${data.length}`)
          setChartData(data)
        } else {
          // Show daily data
          console.log(`[UserDashboard] Aggregating into daily buckets`)
          const dailyData = {}
          metrics.forEach(m => {
            const d = new Date(m.timestamp)
            const key = d.toISOString().split('T')[0]
            if (!dailyData[key]) {
              dailyData[key] = { values: [], date: key }
            }
            dailyData[key].values.push(m.value)
          })

          const data = Object.values(dailyData)
            .sort((a, b) => a.date.localeCompare(b.date))
            .map(d => ({
              time: d.date,
              value: parseFloat((d.values.reduce((a, b) => a + b, 0) / d.values.length).toFixed(2))
            }))
          console.log(`[UserDashboard] Daily data points: ${data.length}`)
          setChartData(data)
        }
      } catch (err) {
        console.error('Failed to fetch chart data:', err)
        console.error('Error details:', err.response?.data || err.message)
        setChartData([])
      } finally {
        setLoading(false)
      }
    }

    fetchData()
  }, [selectedDevice, fromDate, toDate])

  const handleAddDevice = async (deviceData) => {
    try {
      setAddingDevice(true)
      await createIoTDevice(deviceData)
      setShowAddDeviceModal(false)
    } catch (err) {
      console.error('Failed to add device:', err)
      throw err
    } finally {
      setAddingDevice(false)
    }
  }

  return (
    <div className="min-h-screen bg-dark-900 p-8">
      {/* Header */}
      <div className="mb-8">
        <h1 className="text-4xl font-bold text-white mb-2">Dashboard</h1>
        <p className="text-gray-400">Overview of your IoT devices</p>
      </div>

      {/* Stats Grid */}
      <div className="grid grid-cols-1 gap-6 mb-8 max-w-xl">
        {/* IoT Devices Card */}
        <div className="bg-dark-800 border border-neon-cyan/20 rounded-xl p-8 hover:border-neon-cyan/40 transition-all">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-lg font-semibold text-white">IoT Devices</h2>
            <Thermometer className="w-6 h-6 text-neon-cyan" />
          </div>
          <p className="text-5xl font-bold text-neon-cyan mb-4">{devices?.length || 0}</p>
          <p className="text-sm text-gray-400 mb-4">
            {devices?.length === 0 
              ? 'No devices created yet' 
              : devices?.length === 1
              ? '1 device connected'
              : `${devices?.length} devices connected`}
          </p>
          <button
            onClick={() => setShowAddDeviceModal(true)}
            className="w-full px-4 py-2 bg-neon-cyan/20 text-neon-cyan border border-neon-cyan/40 rounded-lg hover:bg-neon-cyan/30 transition-all flex items-center justify-center gap-2 text-sm"
          >
            <Plus className="w-4 h-4" />
            Add Device
          </button>
        </div>
      </div>

      {/* Charts Section */}
      {devices && devices.length > 0 && (
        <div className="bg-dark-800 border border-neon-cyan/20 rounded-xl p-8">
          <div className="mb-6">
            <h2 className="text-2xl font-bold text-white mb-6 flex items-center gap-2">
              <TrendingUp className="w-6 h-6 text-neon-cyan" />
              Sensor Data Analysis
            </h2>

            {/* Filters */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
              {/* Sensor Selector */}
              <div>
                <label className="block text-sm font-medium text-gray-300 mb-2">Select Sensor</label>
                <select
                  value={selectedDeviceId || ''}
                  onChange={(e) => setSelectedDeviceId(parseInt(e.target.value))}
                  className="w-full bg-dark-900 border border-gray-700 rounded-lg px-4 py-2 text-white focus:border-neon-cyan outline-none"
                >
                  {devices.map(device => (
                    <option key={device.id} value={device.id}>
                      {device.name} ({device.device_type})
                    </option>
                  ))}
                </select>
              </div>

              {/* From Date */}
              <div>
                <label className="block text-sm font-medium text-gray-300 mb-2">From Date</label>
                <div className="flex items-center gap-2">
                  <Calendar className="w-4 h-4 text-gray-400" />
                  <input
                    type="date"
                    value={fromDate}
                    onChange={(e) => setFromDate(e.target.value)}
                    className="w-full bg-dark-900 border border-gray-700 rounded-lg px-4 py-2 text-white focus:border-neon-cyan outline-none"
                  />
                </div>
              </div>

              {/* To Date */}
              <div>
                <label className="block text-sm font-medium text-gray-300 mb-2">To Date</label>
                <div className="flex items-center gap-2">
                  <Calendar className="w-4 h-4 text-gray-400" />
                  <input
                    type="date"
                    value={toDate}
                    onChange={(e) => setToDate(e.target.value)}
                    className="w-full bg-dark-900 border border-gray-700 rounded-lg px-4 py-2 text-white focus:border-neon-cyan outline-none"
                  />
                </div>
              </div>
            </div>

            {/* Chart */}
            {loading ? (
              <div className="h-96 flex items-center justify-center text-gray-400">
                Loading chart data...
              </div>
            ) : chartData.length > 0 ? (
              <div className="h-96 w-full">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={chartData} margin={{ top: 5, right: 30, left: 0, bottom: 5 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                    <XAxis 
                      dataKey="time" 
                      stroke="#999"
                      tick={{ fontSize: 12 }}
                    />
                    <YAxis 
                      stroke="#999"
                      tick={{ fontSize: 12 }}
                    />
                    <Tooltip 
                      contentStyle={{ backgroundColor: '#1a1a2e', border: '1px solid #0f3460' }}
                      labelStyle={{ color: '#00d4ff' }}
                    />
                    <Legend />
                    <Line
                      type="monotone"
                      dataKey="value"
                      stroke="#00d4ff"
                      strokeWidth={2}
                      dot={{ fill: '#00d4ff', r: 4 }}
                      activeDot={{ r: 6 }}
                      name={`${selectedDevice?.name} (${selectedDevice?.device_type})`}
                    />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            ) : (
              <div className="h-96 flex items-center justify-center text-gray-400">
                No data available for the selected date range
              </div>
            )}

            {/* Info */}
            <div className="mt-4 p-3 bg-neon-cyan/10 border border-neon-cyan/30 rounded-lg">
              <p className="text-xs text-neon-cyan">
                💡 Select 1 day to see hourly averages, or 2+ days to see daily averages
              </p>
            </div>
          </div>
        </div>
      )}

      {/* Modal */}
      <AddDeviceModal
        isOpen={showAddDeviceModal}
        onClose={() => setShowAddDeviceModal(false)}
        onAdd={handleAddDevice}
        isLoading={addingDevice}
      />
    </div>
  )
}
