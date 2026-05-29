import { useState, useEffect, useRef } from 'react'
import { AuthProvider, useAuth } from './context/AuthContext'
import { DeviceProvider } from './context/DeviceContext'
import { NotificationProvider } from './context/NotificationContext'
import api from './api'
import Sidebar from './components/Sidebar'
import UserDashboard from './components/UserDashboard'
import AdminDashboard from './components/AdminDashboard'
import CPUMetrics from './components/CPUMetrics'
import IoTMetrics from './components/IoTMetrics'
import IoTDeviceManager from './components/IoTDeviceManager'
import MemoryMetrics from './components/MemoryMetrics'
import Alerts from './components/Alerts'
import AdminPanel from './components/AdminPanel'
import ClientMonitor from './components/ClientMonitor'
import SupportChat from './components/SupportChat'
import Login from './components/Login'

function AppContent() {
  const [activeMenu, setActiveMenu] = useState('iot-devices')
  const [health, setHealth] = useState(null)
  const { user, token, loading } = useAuth()
  const healthInFlightRef = useRef(false)

  useEffect(() => {
    // Check backend health
    const checkHealth = async () => {
      if (healthInFlightRef.current) return
      if (document.visibilityState === 'hidden') return
      try {
        healthInFlightRef.current = true
        const response = await api.get('/api/health')
        setHealth(response.data)
      } catch (error) {
        console.error('Backend not available:', error)
      } finally {
        healthInFlightRef.current = false
      }
    }

    checkHealth()
    const interval = setInterval(checkHealth, 20000)
    return () => clearInterval(interval)
  }, [])

  const renderContent = () => {
    switch (activeMenu) {
      case 'dashboard':
        if (user?.role === 'admin') {
          return <AdminDashboard />
        } else {
          return <UserDashboard />
        }
      case 'iot-devices':
        return <IoTDeviceManager />
      case 'cpu':
        return <CPUMetrics />
      case 'memory':
        return <MemoryMetrics />
      case 'iot':
        return <IoTMetrics />
      case 'alerts':
        return <Alerts />
      case 'client-monitor':
        return <ClientMonitor />
      case 'admin-panel':
        return <AdminPanel />
      case 'support-chat':
        return <SupportChat />
      default:
        // Default: admin gets dashboard, users get IoT devices (UserDashboard)
        return user?.role === 'admin' ? <AdminDashboard /> : <UserDashboard />
    }
  }

  // Show loading
  if (loading) {
    return (
      <div className="min-h-screen bg-dark-900 flex items-center justify-center">
        <p className="text-gray-400">Loading...</p>
      </div>
    )
  }

  // Show login if not authenticated
  if (!token || !user) {
    return <Login />
  }

  // Show dashboard if authenticated
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
    <NotificationProvider>
      <AuthProvider>
        <DeviceProvider>
          <AppContent />
        </DeviceProvider>
      </AuthProvider>
    </NotificationProvider>
  )
}
