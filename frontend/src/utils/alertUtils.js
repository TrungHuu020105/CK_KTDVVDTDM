/**
 * Alert System Utilities
 * Defines thresholds and alert logic for different metrics
 */

const ALERT_THRESHOLDS = {
  cpu: {
    warning: 80,
    critical: 90,
    unit: '%'
  },
  memory: {
    warning: 85,
    critical: 95,
    unit: '%'
  }
}

/**
 * Check if a metric value triggers an alert
 * @param {string} metricType - Type of metric (cpu, memory, temperature, etc)
 * @param {number} value - Current value
 * @returns {object} { status: 'normal' | 'warning' | 'critical', message: string }
 */
export const checkAlert = (metricType, value) => {
  const threshold = ALERT_THRESHOLDS[metricType]
  
  if (!threshold) {
    return { status: 'normal', message: '' }
  }

  // Handle metrics with single thresholds (cpu, memory)
  if (threshold.critical !== undefined) {
    if (value >= threshold.critical) {
      return {
        status: 'critical',
        message: `CRITICAL: ${metricType} at ${value}${threshold.unit}! (Threshold: ${threshold.critical}${threshold.unit})`
      }
    }
    if (value >= threshold.warning) {
      return {
        status: 'warning',
        message: `WARNING: ${metricType} at ${value}${threshold.unit}. (Threshold: ${threshold.warning}${threshold.unit})`
      }
    }
  }

  // Handle metrics with dual thresholds (not used in current system alerts)
  if (threshold.critical_low !== undefined) {
    if (value <= threshold.critical_low || value >= threshold.critical_high) {
      return {
        status: 'critical',
        message: `CRITICAL: ${metricType} at ${value}${threshold.unit}! (Safe range: ${threshold.critical_low}-${threshold.critical_high}${threshold.unit})`
      }
    }
    if (value <= threshold.warning_low || value >= threshold.warning_high) {
      return {
        status: 'warning',
        message: `WARNING: ${metricType} at ${value}${threshold.unit}. (Safe range: ${threshold.warning_low}-${threshold.warning_high}${threshold.unit})`
      }
    }
  }

  return { status: 'normal', message: '' }
}

/**
 * Create an alert object for storage/display
 */
export const createAlert = (metricType, value, timestamp = new Date()) => {
  const alert = checkAlert(metricType, value)
  
  if (alert.status === 'normal') {
    return null
  }

  return {
    id: `${metricType}-${timestamp.getTime()}`,
    metricType,
    value,
    timestamp,
    status: alert.status,
    message: alert.message
  }
}

/**
 * Get alert badge color and icon
 */
export const getAlertStyle = (status) => {
  switch (status) {
    case 'critical':
      return {
        bgColor: 'bg-red-900/30',
        textColor: 'text-red-400',
        borderColor: 'border-red-500/50',
        icon: '🔴',
        label: 'CRITICAL'
      }
    case 'warning':
      return {
        bgColor: 'bg-yellow-900/30',
        textColor: 'text-yellow-400',
        borderColor: 'border-yellow-500/50',
        icon: '🟡',
        label: 'WARNING'
      }
    default:
      return {
        bgColor: 'bg-green-900/30',
        textColor: 'text-green-400',
        borderColor: 'border-green-500/50',
        icon: '🟢',
        label: 'NORMAL'
      }
  }
}

/**
 * Check metric alert status and return color info
 * Wrapper for checkAlert with additional color info
 */
export const checkMetricAlert = (metricType, value) => {
  const alert = checkAlert(metricType, value)
  
  // Map status to hex color codes
  const statusColorMap = {
    critical: '#ff3333',
    warning: '#ffcc00',
    normal: '#00ff88'
  }

  return {
    status: alert.status,
    message: alert.status === 'normal' ? 'Normal' : alert.status.toUpperCase(),
    color: statusColorMap[alert.status],
    fullMessage: alert.message
  }
}

export default ALERT_THRESHOLDS
