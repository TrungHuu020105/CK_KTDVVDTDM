import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App'
import './index.css'

if (
  typeof window !== 'undefined' &&
  import.meta.env.VITE_PATCH_WINDOW_ALERT === 'true' &&
  !window.__metricsPulseAlertPatched
) {
  window.__metricsPulseAlertPatched = true
  window.alert = (message) => {
    window.dispatchEvent(
      new CustomEvent('metricspulse:notify', {
        detail: { message: String(message ?? '') }
      })
    )
  }
}

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
)
