import { useEffect, useState } from 'react'
import { CheckCircle, XCircle, Trash2, AlertCircle, DollarSign } from 'lucide-react'
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

  const fetchPendingUsers = async () => {
    try {
      setLoading(true)
      const response = await api.get('/api/admin/users/pending')
      setPendingUsers(response.data.users || [])
    } catch (error) {
      setMessage('Failed to fetch pending users: ' + (error.response?.data?.detail || error.message))
    } finally {
      setLoading(false)
    }
  }

  const fetchAllUsers = async () => {
    try {
      setLoading(true)
      const response = await api.get('/api/admin/users')
      setAllUsers(response.data.users || [])
    } catch (error) {
      setMessage('Failed to fetch users: ' + (error.response?.data?.detail || error.message))
    } finally {
      setLoading(false)
    }
  }

  const approveUser = async (userId) => {
    try {
      await api.post(`/api/admin/users/${userId}/approve`)
      setMessage('User approved successfully')
      fetchPendingUsers()
      fetchAllUsers()
    } catch (error) {
      setMessage('Failed to approve user: ' + (error.response?.data?.detail || error.message))
    }
  }

  const rejectUser = async (userId) => {
    try {
      await api.post(`/api/admin/users/${userId}/reject`)
      setMessage('User rejected successfully')
      fetchPendingUsers()
      fetchAllUsers()
    } catch (error) {
      setMessage('Failed to reject user: ' + (error.response?.data?.detail || error.message))
    }
  }

  const deleteUser = async (userId) => {
    if (user?.id === userId) {
      setMessage('You cannot delete your own account')
      return
    }
    if (!window.confirm('Delete this user?')) return
    try {
      await api.delete(`/api/admin/users/${userId}`)
      setMessage('User deleted successfully')
      fetchPendingUsers()
      fetchAllUsers()
    } catch (error) {
      setMessage('Failed to delete user: ' + (error.response?.data?.detail || error.message))
    }
  }

  useEffect(() => {
    fetchPendingUsers()
    fetchAllUsers()
  }, [])

  useEffect(() => {
    if (!message) return undefined
    const timer = window.setTimeout(() => setMessage(''), 3000)
    return () => window.clearTimeout(timer)
  }, [message])

  return (
    <div className="p-6 space-y-6">
      <div>
        <h1 className="text-4xl font-bold text-white mb-2">Admin Panel</h1>
        <p className="text-gray-400">Manage users</p>
      </div>

      {message && (
        <div className="flex items-center gap-2 px-4 py-3 rounded-lg bg-neon-cyan/10 border border-neon-cyan/30 text-neon-cyan">
          <AlertCircle className="w-5 h-5" />
          <span>{message}</span>
        </div>
      )}

      <div className="flex border-b border-gray-700">
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
      </div>

      {activeTab === 'pending-users' && (
        <div className="space-y-4">
          {pendingUsers.length === 0 ? (
            <p className="text-gray-400">No pending users</p>
          ) : (
            pendingUsers.map((row) => (
              <div key={row.id} className="card-border p-4 bg-dark-800 flex items-center justify-between">
                <div>
                  <p className="text-white font-semibold">{row.username}</p>
                  <p className="text-sm text-gray-400">{row.email}</p>
                  <p className="text-xs text-gray-500 mt-1">Applied: {formatVNDate(row.created_at)}</p>
                </div>
                <div className="flex gap-2">
                  <button
                    onClick={() => approveUser(row.id)}
                    disabled={loading}
                    className="px-4 py-2 bg-green-500/20 text-green-400 rounded-lg hover:bg-green-500/30 transition-all disabled:opacity-50 flex items-center gap-2"
                  >
                    <CheckCircle className="w-4 h-4" />
                    Approve
                  </button>
                  <button
                    onClick={() => rejectUser(row.id)}
                    disabled={loading}
                    className="px-4 py-2 bg-red-500/20 text-red-400 rounded-lg hover:bg-red-500/30 transition-all disabled:opacity-50 flex items-center gap-2"
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

      {activeTab === 'all-users' && (
        <div className="space-y-4">
          {allUsers.map((row) => (
            <div
              key={row.id}
              className="card-border p-4 bg-dark-800 flex items-center justify-between"
            >
              <div className="flex-1">
                <p className="text-white font-semibold">{row.username}</p>
                <p className="text-sm text-gray-400">{row.email}</p>
                <p className="text-xs text-gray-500 mt-1">
                  Role: <span className="capitalize">{row.role}</span>
                  {' • '}
                  {row.is_active ? 'Active' : 'Inactive'}
                  {' • '}
                  {row.is_approved ? 'Approved' : 'Pending'}
                </p>
              </div>
              <div className="flex items-center gap-3">
                <span className={`px-3 py-1 rounded-full text-xs font-semibold ${
                  row.is_approved ? 'bg-green-500/20 text-green-400' : 'bg-yellow-500/20 text-yellow-400'
                }`}>
                  {row.is_approved ? 'Approved' : 'Pending'}
                </span>
                {user?.id !== row.id && (
                  <button
                    onClick={() => deleteUser(row.id)}
                    disabled={loading}
                    className="px-3 py-2 bg-red-500/20 text-red-400 rounded-lg hover:bg-red-500/30 transition-all disabled:opacity-50 flex items-center gap-2"
                  >
                    <Trash2 className="w-4 h-4" />
                    Delete
                  </button>
                )}
              </div>
            </div>
          ))}
        </div>
      )}

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="card-border p-4 bg-dark-800">
          <div className="flex items-center gap-2 mb-2">
            <CheckCircle className="w-5 h-5 text-green-400" />
            <h3 className="text-white font-semibold">Approved Users</h3>
          </div>
          <p className="text-3xl font-bold text-green-400">
            {allUsers.filter((entry) => entry.is_approved).length}
          </p>
        </div>

        <div className="card-border p-4 bg-dark-800">
          <div className="flex items-center gap-2 mb-2">
            <XCircle className="w-5 h-5 text-yellow-400" />
            <h3 className="text-white font-semibold">Pending Approval</h3>
          </div>
          <p className="text-3xl font-bold text-yellow-400">{pendingUsers.length}</p>
        </div>

        <div className="card-border p-4 bg-dark-800">
          <div className="flex items-center gap-2 mb-2">
            <DollarSign className="w-5 h-5 text-neon-cyan" />
            <h3 className="text-white font-semibold">Total Users</h3>
          </div>
          <p className="text-3xl font-bold text-neon-cyan">{allUsers.length}</p>
        </div>
      </div>
    </div>
  )
}
