import { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react'
import { AlertTriangle, CheckCircle2, Info, X, XCircle } from 'lucide-react'

const NotificationContext = createContext(null)

const AUTO_HIDE_MS = 4500

const normalizeMessage = (message) => {
  if (typeof message === 'string') return message
  if (message === undefined || message === null) return ''
  try {
    return JSON.stringify(message)
  } catch {
    return String(message)
  }
}

const inferTypeFromMessage = (message) => {
  const text = normalizeMessage(message).toLowerCase()
  if (text.includes('error') || text.includes('failed') || text.includes('lỗi')) return 'error'
  if (text.includes('warning') || text.includes('cảnh báo')) return 'warning'
  if (text.includes('success') || text.includes('thành công') || text.includes('đã')) return 'success'
  return 'info'
}

const toastStyles = {
  success: {
    icon: CheckCircle2,
    border: 'border-emerald-400/50',
    iconColor: 'text-emerald-300',
    title: 'Thành công',
  },
  error: {
    icon: XCircle,
    border: 'border-red-400/50',
    iconColor: 'text-red-300',
    title: 'Có lỗi xảy ra',
  },
  warning: {
    icon: AlertTriangle,
    border: 'border-amber-400/50',
    iconColor: 'text-amber-300',
    title: 'Lưu ý',
  },
  info: {
    icon: Info,
    border: 'border-cyan-400/50',
    iconColor: 'text-cyan-300',
    title: 'Thông báo',
  },
}

export function NotificationProvider({ children }) {
  const [toasts, setToasts] = useState([])

  const removeToast = useCallback((id) => {
    setToasts((prev) => prev.filter((toast) => toast.id !== id))
  }, [])

  const notify = useCallback((message, type = 'info') => {
    const normalized = normalizeMessage(message)
    const toastType = type || inferTypeFromMessage(normalized)
    const id = `${Date.now()}_${Math.random().toString(36).slice(2, 9)}`
    setToasts((prev) => [...prev, { id, message: normalized, type: toastType }])
    window.setTimeout(() => removeToast(id), AUTO_HIDE_MS)
  }, [removeToast])

  useEffect(() => {
    const handleNotifyEvent = (event) => {
      const message = event?.detail?.message ?? ''
      notify(message, inferTypeFromMessage(message))
    }

    window.addEventListener('metricspulse:notify', handleNotifyEvent)
    return () => window.removeEventListener('metricspulse:notify', handleNotifyEvent)
  }, [notify])

  const value = useMemo(() => ({ notify }), [notify])

  return (
    <NotificationContext.Provider value={value}>
      {children}

      <div className="fixed top-4 right-4 z-[2000] flex w-[360px] max-w-[calc(100vw-2rem)] flex-col gap-3">
        {toasts.map((toast) => {
          const style = toastStyles[toast.type] || toastStyles.info
          const Icon = style.icon
          return (
            <div
              key={toast.id}
              className={`rounded-xl border ${style.border} bg-dark-800/95 shadow-2xl backdrop-blur p-4`}
            >
              <div className="flex items-start gap-3">
                <Icon className={`mt-0.5 h-5 w-5 ${style.iconColor}`} />
                <div className="flex-1">
                  <p className="text-sm font-semibold text-white">{style.title}</p>
                  <p className="mt-1 text-sm text-gray-200 leading-relaxed">{toast.message}</p>
                </div>
                <button
                  onClick={() => removeToast(toast.id)}
                  className="rounded p-1 text-gray-400 transition-colors hover:text-white"
                  aria-label="Close notification"
                >
                  <X className="h-4 w-4" />
                </button>
              </div>
            </div>
          )
        })}
      </div>
    </NotificationContext.Provider>
  )
}

export function useNotification() {
  const context = useContext(NotificationContext)
  if (!context) {
    throw new Error('useNotification must be used within NotificationProvider')
  }
  return context
}
