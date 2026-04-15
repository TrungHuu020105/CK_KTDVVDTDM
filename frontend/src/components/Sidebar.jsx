import { BarChart3, Gauge, AlertCircle, Thermometer } from 'lucide-react'
import { useDevices } from '../context/DeviceContext'

export default function Sidebar({ activeMenu, setActiveMenu, health }) {
  const { iotDevices } = useDevices()

  const menuItems = [
    { id: 'dashboard', label: 'My Dashboard', icon: Gauge },
    { id: 'iot-devices', label: 'IoT Devices', icon: Thermometer },
    { id: 'alerts', label: 'Alerts', icon: AlertCircle },
  ]

  return (
    <div className="w-72 bg-dark-800 border-r border-neon-cyan/20 p-6 flex flex-col overflow-y-auto h-screen">
      {/* Logo */}
      <div className="mb-8 flex items-center gap-3">
        <div className="p-2 bg-neon-cyan/20 rounded-lg">
          <BarChart3 className="w-6 h-6 text-neon-cyan" />
        </div>
        <div>
          <h1 className="text-lg font-bold text-neon-cyan neon-glow">MetricsPulse</h1>
          <p className="text-xs text-gray-400">PRO V2.0</p>
        </div>
      </div>

      {/* Health Status */}
      {health && (
        <div className="mb-6 p-3 bg-dark-900 rounded-lg border border-green-500/30">
          <div className="flex items-center gap-2">
            <div className="w-2 h-2 bg-green-500 rounded-full animate-pulse"></div>
            <span className="text-xs text-green-400">Backend Online</span>
          </div>
        </div>
      )}

      {/* Main Menu */}
      <nav className="flex-1 space-y-2 mb-6">
        {menuItems.map((item) => {
          const Icon = item.icon
          const isActive = activeMenu === item.id

          return (
            <button
              key={item.id}
              onClick={() => setActiveMenu(item.id)}
              className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg transition-all ${
                isActive
                  ? 'bg-neon-cyan/20 text-neon-cyan border border-neon-cyan/40'
                  : 'text-gray-400 hover:text-gray-200 hover:bg-dark-700'
              }`}
            >
              <Icon className="w-5 h-5" />
              <span className="flex-1 text-left">{item.label}</span>
            </button>
          )
        })}
      </nav>

      {/* No Devices Warning */}
      {(!iotDevices || iotDevices.length === 0) && (
        <div className="mb-6 pb-6 border-b border-gray-700 p-3 bg-yellow-500/10 border border-yellow-500/30 rounded-lg">
          <p className="text-xs text-yellow-400">
            No devices yet. Create your first IoT device.
          </p>
        </div>
      )}

      {/* Footer */}
      <div className="pt-4 border-t border-gray-700 space-y-4">
        <p className="text-gray-500 text-xs">Version 2.0.0</p>
      </div>
    </div>
  )
}
