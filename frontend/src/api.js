import axios from 'axios'

// Detect server URL based on environment
const getServerURL = () => {
    // If running in development with Vite, use the server IP
    // Change this to your actual server IP if needed
    const serverIP =
        import.meta.env.VITE_SERVER_IP || 'localhost'
    const serverPort =
        import.meta.env.VITE_SERVER_PORT || '8000'

    const url = `http://${serverIP}:${serverPort}`
    console.log('[API] Server URL:', url)
    return url
}

const api = axios.create({
    baseURL: getServerURL(),
    headers: {
        'Content-Type': 'application/json',
    },
    timeout: 30000 // 30 second timeout
})

// Add error handling
api.interceptors.response.use(
    response => response,
    error => {
        if (error.response?.status === 404) {
            console.error('Endpoint not found:', error.config.url)
        }
        console.error('[API] Error:', {
            message: error.message,
            code: error.code,
            url: error.config?.url
        })
        return Promise.reject(error)
    }
)

export default api