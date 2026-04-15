import { useState, useEffect } from 'react'
import { DeviceProvider } from './context/DeviceContext'
import api from './api'
import Sidebar from './components/Sidebar'
import UserDashboard from './components/UserDashboard'
import IoTMetrics from './components/IoTMetrics'
import IoTDeviceManager from './components/IoTDeviceManager'
import Alerts from './components/Alerts'

function AppContent() {
  const [activeMenu, setActiveMenu] = useState('iot-devices')
  const [health, setHealth] = useState(null)

  useEffect(() => {
    // Check backend health
    const checkHealth = async () => {
      try {
        const response = await api.get('/api/health')
        setHealth(response.data)
      } catch (error) {
        console.error('Backend not available:', error)
      }
    }

    checkHealth()
    const interval = setInterval(checkHealth, 10000)
    return () => clearInterval(interval)
  }, [])

  const renderContent = () => {
    switch (activeMenu) {
      case 'dashboard':
        return <UserDashboard />
      case 'iot-devices':
        return <IoTDeviceManager />
      case 'iot':
        return <IoTMetrics />
      case 'alerts':
        return <Alerts />
      default:
        return <UserDashboard />
    }
  }

  return (
    <div className="flex h-screen bg-dark-900">
      <Sidebar activeMenu={activeMenu} setActiveMenu={setActiveMenu} health={health} />
      <div className="flex-1 overflow-y-auto">
        {renderContent()}
      </div>
    </div>
  )
}

export default function App() {
  return (
    <DeviceProvider>
      <AppContent />
    </DeviceProvider>
  )
}
