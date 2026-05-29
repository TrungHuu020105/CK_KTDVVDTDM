export const VN_TIMEZONE = 'Asia/Ho_Chi_Minh'

const ISO_NO_TZ_REGEX = /^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$/

export const toDateSafe = (value, options = {}) => {
  const { naiveAsUTC = false } = options
  if (value instanceof Date) return value
  if (value === null || value === undefined) return null
  if (typeof value === 'string') {
    const normalized = value.trim()
    // Some endpoints return naive timestamps without timezone metadata.
    if (ISO_NO_TZ_REGEX.test(normalized)) {
      const base = normalized.replace(' ', 'T')
      const asTZ = naiveAsUTC ? `${base}Z` : `${base}+07:00`
      const parsed = new Date(asTZ)
      if (!Number.isNaN(parsed.getTime())) return parsed
    }
  }
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? null : date
}

export const formatVNTime = (value, withSeconds = false, options = {}) => {
  const date = toDateSafe(value, options)
  if (!date) return 'N/A'
  return date.toLocaleTimeString('vi-VN', {
    timeZone: VN_TIMEZONE,
    hour12: false,
    hour: '2-digit',
    minute: '2-digit',
    ...(withSeconds ? { second: '2-digit' } : {}),
  })
}

export const formatVNDate = (value, options = {}) => {
  const date = toDateSafe(value, options)
  if (!date) return 'N/A'
  return date.toLocaleDateString('en-CA', { timeZone: VN_TIMEZONE }) // YYYY-MM-DD
}

export const formatVNDateTime = (value, withSeconds = true, options = {}) => {
  const date = toDateSafe(value, options)
  if (!date) return 'N/A'
  return date.toLocaleString('vi-VN', {
    timeZone: VN_TIMEZONE,
    hour12: false,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    ...(withSeconds ? { second: '2-digit' } : {}),
  })
}

export const getVNDateInputValue = () => formatVNDate(new Date())
