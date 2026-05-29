import { useState } from 'react'
import { useAuth } from '../context/AuthContext'

export default function Login() {
  const [isLogin, setIsLogin] = useState(true)
  const [username, setUsername] = useState('')
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [role, setRole] = useState('user')
  const [error, setError] = useState('')
  const [successMessage, setSuccessMessage] = useState('')
  const [loading, setLoading] = useState(false)

  const { login, register } = useAuth()

  const handleSubmit = async (e) => {
    e.preventDefault()
    setError('')
    setSuccessMessage('')
    setLoading(true)

    try {
      if (isLogin) {
        // Login mode
        const result = await login(username, password)
        if (!result.success) {
          setError(result.message)
        }
      } else {
        // Register mode
        if (!email) {
          setError('Email is required')
          setLoading(false)
          return
        }
        const result = await register(username, email, password, role)
        if (result.success) {
          const waitingApprovalMessage = role === 'admin'
            ? 'Đăng ký thành công. Bạn có thể đăng nhập ngay bây giờ.'
            : 'Đăng ký thành công. Yêu cầu của bạn đã được gửi và đang chờ quản trị viên phê duyệt.'

          setSuccessMessage(waitingApprovalMessage)
          setIsLogin(true)
          setPassword('')
        } else {
          setError(result.message)
        }
      }
    } catch (err) {
      setError(err.message || 'An error occurred')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="min-h-screen bg-dark-900 flex items-center justify-center p-4">
      <div className="w-full max-w-md">
        {/* Logo & Title */}
        <div className="text-center mb-8">
          <h1 className="text-4xl font-bold text-neon-cyan neon-glow">MetricsPulse</h1>
          <p className="text-gray-400 mt-2">Real-Time Metrics Monitoring</p>
        </div>

        {/* Card */}
        <div className="card-border bg-dark-800 p-8">
          {/* Tab Toggle */}
          <div className="flex gap-4 mb-6">
            <button
              onClick={() => {
                setIsLogin(true)
                setError('')
                setSuccessMessage('')
              }}
              className={`flex-1 py-2 px-4 rounded text-sm font-semibold transition-all ${
                isLogin
                  ? 'bg-neon-cyan text-dark-900'
                  : 'bg-dark-700 text-gray-400 hover:text-white'
              }`}
            >
              Login
            </button>
            <button
              onClick={() => {
                setIsLogin(false)
                setError('')
                setSuccessMessage('')
              }}
              className={`flex-1 py-2 px-4 rounded text-sm font-semibold transition-all ${
                !isLogin
                  ? 'bg-neon-cyan text-dark-900'
                  : 'bg-dark-700 text-gray-400 hover:text-white'
              }`}
            >
              Register
            </button>
          </div>

          {/* Form */}
          <form onSubmit={handleSubmit} className="space-y-4">
            {/* Error Message */}
            {error && (
              <div className="p-3 bg-red-500/20 border border-red-500/50 rounded text-red-400 text-sm">
                {error}
              </div>
            )}

            {successMessage && (
              <div className="p-3 bg-emerald-500/20 border border-emerald-500/50 rounded text-emerald-300 text-sm">
                {successMessage}
              </div>
            )}

            {/* Username */}
            <div>
              <label className="block text-gray-400 text-sm mb-2">Username</label>
              <input
                type="text"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                placeholder="Enter username"
                className="w-full bg-dark-700 border border-gray-600 rounded px-4 py-2 text-white placeholder-gray-500 focus:outline-none focus:border-neon-cyan"
                required
              />
            </div>

            {/* Email (Register only) */}
            {!isLogin && (
              <div>
                <label className="block text-gray-400 text-sm mb-2">Email</label>
                <input
                  type="email"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  placeholder="Enter email"
                  className="w-full bg-dark-700 border border-gray-600 rounded px-4 py-2 text-white placeholder-gray-500 focus:outline-none focus:border-neon-cyan"
                  required
                />
              </div>
            )}

            {/* Password */}
            <div>
              <label className="block text-gray-400 text-sm mb-2">Password</label>
              <input
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                placeholder="Enter password"
                className="w-full bg-dark-700 border border-gray-600 rounded px-4 py-2 text-white placeholder-gray-500 focus:outline-none focus:border-neon-cyan"
                required
              />
            </div>

            {/* Role (Register only) */}
            {!isLogin && (
              <div>
                <label className="block text-gray-400 text-sm mb-2">Role</label>
                <select
                  value={role}
                  onChange={(e) => setRole(e.target.value)}
                  className="w-full bg-dark-700 border border-gray-600 rounded px-4 py-2 text-white focus:outline-none focus:border-neon-cyan"
                >
                  <option value="user">User</option>
                  <option value="admin">Admin</option>
                </select>
              </div>
            )}

            {/* Submit Button */}
            <button
              type="submit"
              disabled={loading}
              className="w-full bg-neon-cyan text-dark-900 font-bold py-2 px-4 rounded hover:bg-neon-cyan/80 disabled:opacity-50 disabled:cursor-not-allowed transition-all"
            >
              {loading ? 'Processing...' : isLogin ? 'Login' : 'Register'}
            </button>
          </form>

          {/* Demo Credentials */}
          {isLogin && (
            <div className="mt-6 p-4 bg-dark-700/50 rounded border border-gray-600 text-sm">
              <p className="text-gray-400 mb-2">Demo Credentials:</p>
              <p className="text-gray-500">👤 admin / 123456</p>
              <p className="text-gray-500">👤 user / 123456</p>
            </div>
          )}
        </div>

        {/* Footer */}
        <p className="text-center text-gray-500 text-xs mt-6">
          © 2026 MetricsPulse. All rights reserved.
        </p>
      </div>
    </div>
  )
}
