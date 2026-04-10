# IoT Real-time Streaming Dashboard

Beautiful, real-time IoT device monitoring dashboard built with **FastAPI** + **React + Vite** + **Databricks**.

![Dashboard](frontend/public/dashboard.png)

---

## 🚀 Quick Start

### Prerequisites
- **Databricks** cluster + SQL Warehouse running
- **Python 3.8+**
- **Node.js 16+**

### 1️⃣ Generate Sample Data (One-time in Databricks)

Upload and run the streaming notebook:

```bash
# Copy to Databricks Workspace
databricks_iot_streaming.py
```

**In Databricks:**
- Workspace → Create notebook → Paste contents
- Select cluster → **Run All**
- Generates 1 hour of IoT data (~720 readings per device)
- Tables created: `workspace.metrics_app_streaming.iot_sensor_data`

### 2️⃣ Start Backend (Port 8000)

```bash
cd backend

# Create .env with Databricks credentials
# DATABRICKS_HOST=xxx
# DATABRICKS_PATH=xxx
# DATABRICKS_TOKEN=xxx

# Install dependencies
pip install -r requirements.txt

# Run server
python server.py
```

✅ Backend ready at `http://localhost:8000`

### 3️⃣ Start Frontend (Port 5173)

```bash
cd frontend

# Install dependencies
npm install

# Run dev server
npm run dev
```

✅ Dashboard ready at `http://localhost:5173`

### 4️⃣ View Dashboard

Open browser → `http://localhost:5173`
- 🟢 WebSocket connected (real-time streaming)
- 📊 Cards show latest device values
- 📈 Click card → view 2-hour chart with stats

---

## 🏗️ How It Works

```
┌─────────────────────────────────────────────────────────────────┐
│                     ARCHITECTURE OVERVIEW                        │
└─────────────────────────────────────────────────────────────────┘

DATABRICKS (Data Processing)
├── Raw Data Generation (Every 5s)
│   └── iot_sensor_data table
│
├── Aggregation Pipeline (databricks_data_aggregation.py)
│   ├── iot_sensor_data_minutely (1-min grouped)
│   ├── device_statistics (all_time min/max/avg)
│   ├── device_anomalies (outlier detection)
│   ├── dashboard_summary (latest + stats)
│   └── device_timeseries_minutely (2-hour history)
│
└── Pre-calculated Tables (Ready for instant queries)

         ↓↓↓ QUERY (50ms) ↓↓↓

FASTAPI BACKEND (Port 8000)
├── /api/health → health check
├── /api/devices → all devices + latest values
├── /api/device/{id}/stats → min/max/avg (calculated on-the-fly)
├── /api/device/{id}/timeseries → 2-hour timeline
└── /ws/devices → WebSocket stream (every 2s)

         ↓↓↓ JSON over HTTP/WS ↓↓↓

REACT FRONTEND (Port 5173)
├── Device Cards (latest values, status)
├── Detail Modal (stats + 2-hour chart)
├── Real-time Updates (WebSocket + auto-reconnect)
└── Vietnam Timezone (UTC+7, HH:MM format)
```

### Data Flow

1. **Data Generation** (Databricks)
   - Raw sensor readings every 5 seconds
   - Stored in `iot_sensor_data` table

2. **Aggregation** (Databricks)
   - Groups readings by minute
   - Calculates min/max/avg/stddev
   - Creates pre-calculated views

3. **Frontend Queries** (FastAPI)
   - `/api/devices`: Latest device values (realtime)
   - `/api/device/{id}/stats`: Fresh stats calculated from aggregated data
   - `/api/device/{id}/timeseries`: Last 2 hours for charting
   - `/ws/devices`: WebSocket stream every 2 seconds

4. **Display** (React)
   - Card grid showing all devices
   - WebSocket auto-updates every 2s
   - Click card → modal with chart
   - Vietnam timezone (UTC+7)

---

## 📊 Key Features

| Feature | Details |
|---------|---------|
| **Real-time Streaming** | WebSocket + 2s refresh rate |
| **Smart Data Aggregation** | 1-minute grouping in Databricks |
| **Fresh Stats** | FastAPI calculates on-the-fly from aggregated data |
| **No Job Scheduler Needed** | Tables auto-update on next aggregation run |
| **Vietnam Timezone** | All charts show UTC+7 time (HH:MM format) |
| **Beautiful UI** | Navy + Cyan theme, card-based layout |
| **Responsive Design** | Works on desktop & mobile |
| **Error Handling** | Auto-reconnect (5 attempts) + REST fallback |

---

## 🔧 Project Structure

```
.
├── backend/
│   ├── server.py              # FastAPI app (90 lines)
│   ├── requirements.txt        # Dependencies
│   └── .env                    # Databricks credentials (gitignored)
│
├── frontend/
│   ├── src/
│   │   ├── components/         # React components
│   │   ├── context/            # AuthContext, DeviceContext
│   │   ├── utils/              # alertService, helpers
│   │   ├── App.jsx             # Main app + WebSocket
│   │   └── App.css             # Styling
│   ├── vite.config.js
│   └── package.json
│
├── databricks_iot_streaming.py    # Data generation (Databricks)
├── databricks_data_aggregation.py # Aggregation pipeline (Databricks)
└── README.md                       # This file
```

---

## 📝 API Endpoints

### REST API (Port 8000)

```bash
# Health check
GET /api/health

# Get all devices with latest values
GET /api/devices

# Get device statistics (min/max/avg)
GET /api/device/{device_id}/stats

# Get 2-hour time series for charting
GET /api/device/{device_id}/timeseries
```

### WebSocket (Port 8000)

```bash
# Real-time device stream
WS /ws/devices
# Sends dashboard_summary every 2 seconds
```

---

## ⚙️ Configuration

### Backend (.env file)

```env
DATABRICKS_HOST=xxx.cloud.databricks.com
DATABRICKS_PATH=/api/2.0/sql/connectors/xxx
DATABRICKS_TOKEN=your-token-here
```

### Frontend (.env.local)

```env
VITE_API_URL=http://localhost:8000
```

---

## 🔄 How It Works

**Data flow (Real-time):**

1. **Databricks streaming** (`databricks_iot_streaming.py`) generates raw data every 5 seconds
2. **FastAPI backend** (port 8000):
   - `/api/devices` → Latest value per device (WebSocket + REST)
   - `/api/device/{id}/stats` → Min/Max/Avg calculated fresh from raw data (last 2h)
   - `/api/device/{id}/timeseries` → 1-minute aggregated data (last 2h)
3. **WebSocket** pushes device updates every 2 seconds
4. **React frontend** (port 5173) displays real-time dashboard

**Key point:** Backend calculates stats & charts on-the-fly, no pre-aggregation needed!

---

## 🐛 Troubleshooting

### ❌ Lỗi: "ModuleNotFoundError: No module named 'fastapi'"

**Giải pháp**:
```bash
# Activate venv
venv\Scripts\activate

# Install requirements
pip install -r requirements.txt
```

### ❌ Lỗi: "Cannot reach backend (http://localhost:8000)"

**Giải pháp**:
1. Kiểm tra backend chạy: `curl http://localhost:8000/api/health`
2. Kiểm tra `.env` có credentials đúng?
3. Kiểm tra SQL Warehouse **đang Running**

### ❌ Lỗi: "Table not found" hoặc "iot_sensor_data không tồn tại"

**Giải pháp**:
1. Kiểm tra streaming notebook đã chạy? (Databricks → `databricks_iot_streaming.py` → Run All)
2. Query Databricks để verify data:
   ```sql
   SELECT COUNT(*) FROM workspace.metrics_app_streaming.iot_sensor_data;
   ```
3. Nếu trống, chạy lại notebook, chờ 30 giây

### ❌ Frontend không load

**Giải pháp**:
1. Check `http://localhost:5173` - có Vite error?
2. Browser console (F12) - có error message?
3. Rebuild: `cd frontend && npm run build`

### ❌ WebSocket connection error

**Nguyên nhân**: Backend không chạy hoặc CORS issue

**Giải pháp**:
1. Kiểm tra backend chạy trên port 8000
2. Browser console (F12 → Network tab) - xem WebSocket request
3. CORS đã enable ở FastAPI (`allow_origins=["*"]`)

### ❌ Biểu đồ không hiển thị dữ liệu (Chart shows "No data available")

**Nguyên nhân & Giải pháp**:

1. **Streaming đã dừng** - Data cũ hơn 2 giờ
   - Kiểm tra:
   ```sql
   SELECT COUNT(*) FROM workspace.metrics_app_streaming.iot_sensor_data
   WHERE timestamp >= CURRENT_TIMESTAMP() - INTERVAL 2 HOURS;
   ```
   - Để fix: Chạy lại `databricks_iot_streaming.py` (hoặc thiết lập Databricks Job chạy theo lịch)

2. **Backend lỗi query** - Xem error chi tiết
   - Mở browser console (F12 → Console tab)
   - Xem warning/error message
   - Kiểm tra endpoint:
   ```bash
   curl http://localhost:8000/api/device/LR_TEMP_001/timeseries
   ```

3. **Card OK nhưng chart trống** - WebSocket works nhưng timeseries endpoint fail
   - Kiểm tra SQL Warehouse đang running
   - Verify credentials ở `.env`

### ❌ "AMBIGUOUS_REFERENCE" error

**Giải pháp**: Backend đã fix, error này không xảy ra nữa

### ✓ Mọi thứ chạy nhưng dữ liệu không update

**Nguyên nhân**: Streaming script dừng sau 1 giờ

**Giải pháp**:
- Setup Databricks Job chạy `databricks_iot_streaming.py` theo lịch (mỗi ngày)
- Hoặc chạy thủ công mỗi lần cần data mới

---

## 📚 Documentation

- **Architecture**: See "How It Works" above
- **Databricks Setup**: Run `databricks_data_aggregation.py` notebook
- **API Reference**: See "API Endpoints" section

---

## 👨‍💻 Tech Stack

- **Backend**: FastAPI, Databricks SQL Connector, Python 3.8+
- **Frontend**: React 18, Vite, Tailwind CSS
- **Database**: Databricks Delta Lake
- **Streaming**: WebSocket (JSON)
- **Deployment**: Local development (can scale to cloud)

---

**Build with ❤️ for IoT monitoring**
