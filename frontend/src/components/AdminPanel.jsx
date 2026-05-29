import { useState, useEffect } from 'react'
import { CheckCircle, XCircle, Trash2, AlertCircle, DollarSign, Power, PowerOff } from 'lucide-react'
import api from '../api'
import { useAuth } from '../context/AuthContext'
import { formatVNDate } from '../utils/vnTime'

export default function AdminPanel() {
  const { user } = useAuth()
  const [activeTab, setActiveTab] = useState('pending-users')
  const [pendingUsers, setPendingUsers] = useState([])
  const [allUsers, setAllUsers] = useState([])
  const [loading, setLoading] = useState(false)
  const [message, setMessage] = useState('')
  const [devices, setDevices] = useState([])

  // Fetch pending users
  const fetchPendingUsers = async () => {
    try {
      setLoading(true)
      const response = await api.get('/api/admin/users/pending')
      setPendingUsers(response.data.users || [])
    } catch (error) {
      setMessage('Failed to fetch pending users: ' + error.response?.data?.detail)
    } finally {
      setLoading(false)
    }
  }

  // Fetch all users
  const fetchAllUsers = async () => {
    try {
      setLoading(true)
      const response = await api.get('/api/admin/users')
      setAllUsers(response.data.users || [])
    } catch (error) {
      setMessage('Failed to fetch users: ' + error.response?.data?.detail)
    } finally {
      setLoading(false)
    }
  }

  // Approve user
  const approveUser = async (userId) => {
    try {
      await api.post(`/api/admin/users/${userId}/approve`)
      setMessage('User approved successfully')
      fetchPendingUsers()
      fetchAllUsers()
    } catch (error) {
      setMessage('Failed to approve user: ' + error.response?.data?.detail)
    }
  }

  // Reject user
  const rejectUser = async (userId) => {
    try {
      await api.post(`/api/admin/users/${userId}/reject`)
      setMessage('User rejected successfully')
      fetchPendingUsers()
      fetchAllUsers()
    } catch (error) {
      setMessage('Failed to reject user: ' + error.response?.data?.detail)
    }
  }

  // Delete user
  const deleteUser = async (userId) => {
    if (!confirm('Delete this user? This action cannot be undone.')) return
    
    try {
      await api.delete(`/api/admin/users/${userId}`)
      setMessage('User deleted successfully')
      fetchAllUsers()
    } catch (error) {
      setMessage('Failed to delete user: ' + error.response?.data?.detail)
    }
  }

  // ============== SENSORS/DEVICES MANAGEMENT ==============

  const fetchDevices = async () => {
    try {
      setLoading(true)
      const response = await api.get('/api/admin/devices')
      setDevices(response.data.devices || [])
    } catch (error) {
      console.error('Failed to fetch devices:', error)
      setMessage('Failed to fetch devices: ' + (error.response?.data?.detail || error.message))
    } finally {
      setLoading(false)
    }
  }

  const toggleDeviceActive = async (deviceId, currentStatus) => {
    try {
      const response = await api.put(`/api/admin/devices/${deviceId}/toggle`)
      setMessage(response.data.message || `Device ${response.data.is_active ? 'enabled' : 'disabled'}`)
      fetchDevices()
    } catch (error) {
      setMessage('Failed to toggle device: ' + (error.response?.data?.detail || error.message))
    }
  }




  // Load data on mount
  useEffect(() => {
    fetchPendingUsers()
    fetchAllUsers()
    fetchDevices()
  }, [])

  // Auto-clear message after 3 seconds
  useEffect(() => {
    if (message) {
      const timer = setTimeout(() => setMessage(''), 3000)
      return () => clearTimeout(timer)
    }
  }, [message])

  return (
    <div className="p-6 space-y-6">
      <div>
        <h1 className="text-3xl font-bold text-white">Admin Panel</h1>
        <p className="text-gray-400 mt-2">Manage users</p>
      </div>

      {/* Message */}
      {message && (
        <div className="p-4 bg-blue-500/20 border border-blue-500/50 text-blue-400 rounded">
          {message}
        </div>
      )}

      {/* Tabs */}
      <div className="flex gap-2 border-b border-gray-700">
        <button
          onClick={() => setActiveTab('pending-users')}
          className={`px-4 py-2 font-semibold border-b-2 transition-all ${
            activeTab === 'pending-users'
              ? 'text-neon-cyan border-neon-cyan'
              : 'text-gray-400 border-transparent hover:text-gray-300'
          }`}
        >
          Pending Users ({pendingUsers.length})
        </button>
        <button
          onClick={() => setActiveTab('all-users')}
          className={`px-4 py-2 font-semibold border-b-2 transition-all ${
            activeTab === 'all-users'
              ? 'text-neon-cyan border-neon-cyan'
              : 'text-gray-400 border-transparent hover:text-gray-300'
          }`}
        >
          All Users
        </button>
        <button
          onClick={() => setActiveTab('sensors')}
          className={`px-4 py-2 font-semibold border-b-2 transition-all ${
            activeTab === 'sensors'
              ? 'text-neon-cyan border-neon-cyan'
              : 'text-gray-400 border-transparent hover:text-gray-300'
          }`}
        >
          Sensors ({devices.length})
        </button>

      </div>

      {/* Pending Users Tab */}
      {activeTab === 'pending-users' && (
        <div className="space-y-4">
          {pendingUsers.length === 0 ? (
            <p className="text-gray-400">No pending users</p>
          ) : (
            pendingUsers.map((user) => (
              <div key={user.id} className="card-border p-4 bg-dark-800 flex items-center justify-between">
                <div>
                  <p className="text-white font-semibold">{user.username}</p>
                  <p className="text-sm text-gray-400">{user.email}</p>
                  <p className="text-xs text-gray-500 mt-1">Applied: {formatVNDate(user.created_at)}</p>
                </div>
                <div className="flex gap-2">
                  <button
                    onClick={() => approveUser(user.id)}
                    className="flex items-center gap-2 px-4 py-2 bg-green-500/20 text-green-400 rounded hover:bg-green-500/30 transition-all"
                  >
                    <CheckCircle className="w-4 h-4" />
                    Approve
                  </button>
                  <button
                    onClick={() => rejectUser(user.id)}
                    className="flex items-center gap-2 px-4 py-2 bg-red-500/20 text-red-400 rounded hover:bg-red-500/30 transition-all"
                  >
                    <XCircle className="w-4 h-4" />
                    Reject
                  </button>
                </div>
              </div>
            ))
          )}
        </div>
      )}

      {/* All Users Tab */}
      {activeTab === 'all-users' && (
        <div className="space-y-4">
          {allUsers.map((user) => (
            <div
              key={user.id}
              className="card-border p-4 bg-dark-800 flex items-center justify-between"
            >
              <div className="flex-1">
                <p className="text-white font-semibold">{user.username}</p>
                <p className="text-sm text-gray-400">{user.email}</p>
                <p className="text-xs text-gray-500 mt-1">
                  Role: <span className="text-neon-yellow">{user.role}</span>
                  {user.is_approved ? ' ✓ Approved' : ' ⏳ Pending'}
                </p>
              </div>
              {user.role !== 'admin' && (
                <button
                  onClick={() => deleteUser(user.id)}
                  className="ml-2 px-3 py-1 bg-red-500/20 text-red-400 rounded hover:bg-red-500/30 transition-all"
                  title="Delete user"
                >
                  <Trash2 className="w-4 h-4" />
                </button>
              )}
            </div>
          ))}
        </div>
      )}

      {/* Sensors Tab */}
      {activeTab === 'sensors' && (
        <div className="space-y-4">
          <div className="text-xs text-gray-400 bg-gray-900/50 p-3 rounded">
            <p>📌 Demo sensors are disabled by default. Enable them to start generating metrics.</p>
          </div>
          {devices.length === 0 ? (
            <p className="text-gray-400">No sensors found</p>
          ) : (
            devices.map((device) => (
              <div
                key={device.id}
                className="card-border p-4 bg-dark-800 flex items-center justify-between"
              >
                <div className="flex-1">
                  <div className="flex items-center gap-2 mb-1">
                    <p className="text-white font-semibold">{device.name}</p>
                    <span className={`text-xs px-2 py-1 rounded ${
                      device.is_active 
                        ? 'bg-green-500/20 text-green-400' 
                        : 'bg-red-500/20 text-red-400'
                    }`}>
                      {device.is_active ? '🟢 Active' : '⚫ Inactive'}
                    </span>
                  </div>
                  <p className="text-sm text-gray-400">{device.device_type}</p>
                  <p className="text-xs text-gray-500">
                    Source: <code className="font-mono">{device.source}</code>
                  </p>
                  <p className="text-xs text-gray-500">
                    User:{' '}
                    {device.users && device.users.length > 0
                      ? device.users.map((u) => u.username || u.email).join(', ')
                      : 'No user assigned'}
                  </p>
                  {device.location && (
                    <p className="text-xs text-gray-500">Location: {device.location}</p>
                  )}
                </div>
                <button
                  onClick={() => toggleDeviceActive(device.id, device.is_active)}
                  disabled={loading}
                  className={`ml-2 px-3 py-2 rounded transition-all flex items-center gap-1 ${
                    device.is_active 
                      ? 'bg-yellow-500/20 text-yellow-400 hover:bg-yellow-500/30' 
                      : 'bg-green-500/20 text-green-400 hover:bg-green-500/30'
                  } disabled:opacity-50`}
                  title={device.is_active ? 'Disable metric generation' : 'Enable metric generation'}
                >
                  {device.is_active ? <PowerOff className="w-4 h-4" /> : <Power className="w-4 h-4" />}
                  {device.is_active ? 'Disable' : 'Enable'}
                </button>
              </div>
            ))
          )}
        </div>
      )}

    </div>
  )
}

