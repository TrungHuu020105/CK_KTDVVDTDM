/**
 * Alert Service
 * Handles saving alerts to the backend database
 */

import api from '../api'

/**
 * Save an alert to the database
 * @param {object} alert - Alert object with metric info
 * @returns {Promise<object>} - Saved alert from database
 */
export const saveAlert = async (metricType, status, currentValue, threshold, message) => {
  try {
    const response = await api.post('/api/alerts', {
      metric_type: metricType,
      status: status,
      current_value: currentValue,
      threshold: threshold,
      message: message,
      source: 'system'
    })
    
    console.log('Alert saved:', response.data)
    return response.data
  } catch (error) {
    console.error('Error saving alert:', error)
    // Don't throw - alerts should not break the metrics display
    return null
  }
}

/**
 * Get threshold value for a metric type
 * Used to determine which threshold was exceeded
 */
export const getThresholdForStatus = (metricType, status) => {
  const thresholds = {
    cpu: { warning: 80, critical: 90 },
    memory: { warning: 85, critical: 95 },
  }

  const threshold = thresholds[metricType]
  if (!threshold) return null

  if (status === 'critical') {
    return threshold.critical || threshold.critical_low
  } else if (status === 'warning') {
    return threshold.warning || threshold.warning_low
  }

  return null
}
