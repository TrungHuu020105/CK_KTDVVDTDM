import { useDevices } from '../context/DeviceContext'
import { AlertTriangle } from 'lucide-react'

/**
 * ProtectedFeature - Hiển thị tính năng chỉ nếu user có devices
 * Nếu không, hiển thị thông báo "chưa cấp quyền"
 */
export function ProtectedFeature({ 
  children, 
  featureName = 'Feature',
  requireDeviceType = null // Optional: specify required device type
}) {
  const { devices } = useDevices()

  const hasAccess = devices && devices.length > 0
  const hasRequiredDevice = !requireDeviceType || 
    devices?.some(d => d.device_type === requireDeviceType)

  if (hasAccess && hasRequiredDevice) {
    return <>{children}</>
  }

  return (
    <div className="flex items-center justify-center min-h-screen bg-dark-900">
      <div className="bg-dark-800 border border-yellow-500/30 rounded-xl p-12 max-w-md text-center">
        <AlertTriangle className="w-12 h-12 text-yellow-400 mx-auto mb-4" />
        <h3 className="text-xl font-bold text-white mb-3">{featureName} Not Available</h3>
        <p className="text-gray-400 mb-6">
          {!hasAccess 
            ? "You don't have access to any devices yet. Please contact your administrator."
            : `You don't have access to ${requireDeviceType} devices. Please request access from your administrator.`
          }
        </p>
        <div className="bg-yellow-500/10 border border-yellow-500/30 rounded-lg p-3">
          <p className="text-sm text-yellow-400">
            💡 Administrators can grant device access through the Admin Panel.
          </p>
        </div>
      </div>
    </div>
  )
}

/**
 * OptionalFeature - Ẩn phần này nếu user không có quyền (thay vì hiển thị lỗi)
 */
export function OptionalFeature({ 
  children,
  requireDeviceType = null
}) {
  const { devices } = useDevices()

  const hasAccess = devices && devices.length > 0
  const hasRequiredDevice = !requireDeviceType || 
    devices?.some(d => d.device_type === requireDeviceType)

  if (!hasAccess || !hasRequiredDevice) {
    return null
  }

  return <>{children}</>
}

/**
 * DeviceTypeFilter - Hiển thị component chỉ nếu user có device loại đó
 */
export function DeviceTypeFilter({ 
  deviceType, 
  children,
  fallbackMessage = null
}) {
  const { devices } = useDevices()

  const hasDeviceType = devices?.some(d => d.device_type === deviceType)

  if (hasDeviceType) {
    return <>{children}</>
  }

  if (fallbackMessage) {
    return (
      <div className="p-6 bg-dark-800 border border-gray-700 rounded-lg text-center">
        <p className="text-gray-400">{fallbackMessage}</p>
      </div>
    )
  }

  return null
}

export default ProtectedFeature
