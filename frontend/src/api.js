import axios from 'axios'

const useSameOriginApi = String(import.meta.env.VITE_USE_SAME_ORIGIN_API || 'false').toLowerCase() === 'true'

const coreIP = import.meta.env.VITE_CORE_SERVER_IP || import.meta.env.VITE_SERVER_IP || 'localhost'
const corePort = import.meta.env.VITE_CORE_SERVER_PORT || import.meta.env.VITE_SERVER_PORT || '8000'
const iotIP = import.meta.env.VITE_IOT_SERVER_IP || coreIP
const iotPort = import.meta.env.VITE_IOT_SERVER_PORT || '8100'

const coreBaseURL = useSameOriginApi ? '' : `http://${coreIP}:${corePort}`
const iotBaseURL = useSameOriginApi ? '' : `http://${iotIP}:${iotPort}`

const iotApiPrefixes = [
    '/api/iot-control',
]

const isIotApi = (url = '') => iotApiPrefixes.some(prefix => url.startsWith(prefix))

const api = axios.create({
    baseURL: coreBaseURL,
    headers: {
        'Content-Type': 'application/json',
    },
    timeout: 30000 // 30 second timeout
})

api.interceptors.request.use((config) => {
    const targetBase = isIotApi(config.url || '') ? iotBaseURL : coreBaseURL
    config.baseURL = targetBase
    return config
})

if (import.meta.env.DEV) {
    console.log('[API] Core Base URL:', coreBaseURL || '(same-origin)')
    console.log('[API] IoT Base URL:', iotBaseURL || '(same-origin)')
}

// Add error handling
api.interceptors.response.use(
    response => response,
    error => {
        const url = error.config?.url || ''
        const isKnownOptionalHistory = url.startsWith('/api/metrics/history')
        if (error.response?.status === 404 && !isKnownOptionalHistory) {
            console.error('Endpoint not found:', error.config.url)
        }
        if (!isKnownOptionalHistory) {
            console.error('[API] Error:', {
                message: error.message,
                code: error.code,
                url: error.config?.url
            })
        }
        return Promise.reject(error)
    }
)

export default api
