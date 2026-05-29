# MetricsPulse Dashboard - Frontend

Real-time metrics dashboard built with React + Vite. Monitor system performance with beautiful visualizations and real-time data updates.

## 🚀 Features

- **Real-Time Dashboard** - Live CPU, Memory, and Request metrics with auto-refresh
- **IoT Sensor Monitoring** - Track temperature, humidity, soil moisture, light intensity, and pressure
- **Detailed Metrics Pages** - Separate pages for each metric type with statistics and charts
- **Dark Theme Design** - Modern dark UI with neon accent colors
- **Always-On Updates** - Data refreshes automatically every 1-5 seconds
- **Responsive Layout** - Sidebar navigation with clean component structure
- **Chart Visualizations** - Line and bar charts using Recharts

## 📋 Pages

1. **Dashboard** - Overview of all metrics with gauge charts and status
2. **CPU Metrics** - Detailed CPU usage history and statistics
3. **Memory Metrics** - Memory usage trends over time
4. **Request Metrics** - API request counts and analysis
5. **IoT Sensors** - Sensor data from 5 different sensor types
6. **Alerts** - Alert system (planned for next phase)

## 🛠️ Tech Stack

- **React 18** - UI framework
- **Vite** - Build tool
- **Tailwind CSS** - Styling
- **Recharts** - Data visualization
- **Axios** - HTTP client
- **Lucide Icons** - Icon library

## 📦 Installation

```bash
cd frontend
npm install
```

## 🚀 Development

```bash
npm run dev
```

Server will start at `http://localhost:3000`

## 🔗 API Connection

Make sure the backend is running on `http://localhost:8000`. The app will proxy API requests automatically.

**Backend commands:**
```bash
# Generate IoT data
python generate_iot_data.py

# Start server
python -m uvicorn app.main:app --reload
```

## 📊 Generated Pages with Charts

- CPU usage over 60 minutes
- Memory usage over 60 minutes  
- Request counts over 60 minutes
- IoT sensor data over 120 minutes (selectable by sensor type)

## 🎨 Color Scheme

- **Background:** Dark blue (#0a0e27)
- **Primary:** Neon Cyan (#00f0ff)
- **Secondary:** Neon Purple (#c400ff)
- **Success:** Neon Green (#00ff88)
- **Warning:** Neon Yellow (#ffaa00)

## 🔄 Auto-Refresh Intervals

- Dashboard: 1 second
- CPU/Memory/Requests: 5 second
- IoT Metrics: 10 seconds
- Health Check: 10 seconds

## 📱 Responsive Design

- Desktop optimized
- Sidebar collapses on smaller screens
- Grid layouts adjust based on screen size

## 🎯 Future Enhancements

- [ ] Real-time WebSocket updates
- [ ] Alert notifications
- [ ] System anomaly detection
- [ ] Email notifications
- [ ] Data export functionality
- [ ] Custom time range selection
- [ ] Database statistics

## 📄 License

MIT
