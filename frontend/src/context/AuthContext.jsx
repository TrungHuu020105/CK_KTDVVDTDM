import { createContext, useState, useContext, useEffect } from 'react'
import api from '../api'

const AuthContext = createContext()

export function AuthProvider({ children }) {
  const [user, setUser] = useState(null)
  const [token, setToken] = useState(null)
  const [loading, setLoading] = useState(true)

  // Load token from localStorage on mount
  useEffect(() => {
    const storedToken = localStorage.getItem('access_token')
    if (storedToken) {
      setToken(storedToken)
      // Verify token by fetching current user
      fetchCurrentUser(storedToken)
    } else {
      setLoading(false)
    }
  }, [])

  // Fetch current user info
  const fetchCurrentUser = async (token) => {
    try {
      // Set auth header before making request
      api.defaults.headers.common['Authorization'] = `Bearer ${token}`
      
      const response = await api.get('/api/auth/me', {
        headers: { Authorization: `Bearer ${token}` }
      })
      setUser(response.data)
    } catch (error) {
      console.error('Failed to fetch user:', error)
      // Token invalid, clear it
      localStorage.removeItem('access_token')
      setToken(null)
      delete api.defaults.headers.common['Authorization']
    } finally {
      setLoading(false)
    }
  }

  // Login function
  const login = async (username, password) => {
    try {
      const response = await api.post('/api/auth/login', {
        username,
        password
      })
      const { access_token, user: userData } = response.data
      
      setToken(access_token)
      setUser(userData)
      localStorage.setItem('access_token', access_token)
      
      // Set default auth header
      api.defaults.headers.common['Authorization'] = `Bearer ${access_token}`
      
      return { success: true }
    } catch (error) {
      return { 
        success: false, 
        message: error.response?.data?.detail || 'Login failed'
      }
    }
  }

  // Register function
  const register = async (username, email, password, role) => {
    try {
      await api.post('/api/auth/register', {
        username,
        email,
        password,
        role
      })
      return { success: true }
    } catch (error) {
      return { 
        success: false, 
        message: error.response?.data?.detail || 'Registration failed'
      }
    }
  }

  // Logout function
  const logout = () => {
    setUser(null)
    setToken(null)
    localStorage.removeItem('access_token')
    delete api.defaults.headers.common['Authorization']
  }

  return (
    <AuthContext.Provider value={{ user, token, loading, login, register, logout }}>
      {children}
    </AuthContext.Provider>
  )
}

export function useAuth() {
  const context = useContext(AuthContext)
  if (!context) {
    throw new Error('useAuth must be used within AuthProvider')
  }
  return context
}
