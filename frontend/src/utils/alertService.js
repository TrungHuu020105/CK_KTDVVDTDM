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
      source: 'iot'
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
    temperature: { warning: 30, critical: 35 },
    humidity: { 
      warning_low: 30, warning_high: 85,
      critical_low: 20, critical_high: 95
    },
    soil_moisture: {
      warning_low: 20, warning_high: 90,
      critical_low: 10, critical_high: 100
    },
    light_intensity: {
      warning_low: 50, warning_high: 900,
      critical_low: 100, critical_high: 950
    },
    pressure: {
      warning_low: 950, warning_high: 1050,
      critical_low: 940, critical_high: 1060
    }
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
