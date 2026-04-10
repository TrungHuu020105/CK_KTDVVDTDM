# 📱 IoT Devices Dashboard - FastAPI + React + Vite

## 🎯 Tổng Quan

Hệ thống IoT streaming real-time với:
- **Backend**: FastAPI (Python) - Port **8000**
- **Frontend**: React + Vite - Port **5173**
- **Giao diện**: Card-based design, click để xem chi tiết stats & charts

---

## 📂 Cấu Trúc Dự Án

```
.
├── backend/                          # FastAPI backend
│   ├── server.py                     # FastAPI application
│   ├── requirements.txt              # Python dependencies
│   ├── .env                          # Databricks credentials
│   └── venv/                         # Virtual environment
│
├── frontend/                         # React + Vite app
│   ├── src/
│   │   ├── App.jsx                   # Main component
│   │   ├── App.css                   # Styling (dark Navy + Cyan)
│   │   ├── main.jsx
│   │   ├── components/
│   │   │   ├── DeviceCard.jsx        # Device card component
│   │   │   ├── DeviceModal.jsx       # Detail modal component
│   │   │   └── Chart.jsx             # SVG chart component
│   │   ├── api/
│   │   │   └── api.js                # API client
│   │   └── index.css
│   ├── package.json
│   ├── vite.config.js
│   └── node_modules/
│
├── databricks_iot_streaming.py       # Streaming engine
├── HuongDan.md                       # File hướng dẫn này
└── README.md
```

---

## 🚀 Quy Trình Triển Khai

### **BƯỚC 1: Chuẩn Bị Databricks**

#### 1.1 Tạo Catalog & Schema
Trên Databricks SQL Editor:
```sql
CREATE CATALOG IF NOT EXISTS workspace;
CREATE SCHEMA IF NOT EXISTS workspace.metrics_app_streaming;
```

#### 1.2 Xác Minh
```sql
SHOW CATALOGS;  -- Kết quả: samples, system, workspace ✅
SHOW SCHEMAS IN workspace;
```

---

### **BƯỚC 2: Chạy Streaming Engine**

#### 2.1 Upload Notebook
1. Databricks **Workspace** → **Repos** → **Create**
2. Upload file: `databricks_iot_streaming.py`
3. **Select cluster** → **Run All**

**Thời gian**: ~100 giây (10 iterations, mỗi 10s)

#### 2.2 Kiểm Tra Data
```sql
-- Nên có >= 50 rows
SELECT COUNT(*) FROM workspace.metrics_app_streaming.iot_sensor_data;
```

---

### **BƯỚC 3: Lấy Databricks Credentials**

Cần 3 thông tin:

#### 🔑 **Lấy 3 Thông Tin Từ Databricks**
1. **Server Hostname**: Databricks → Settings → Workspace URL
2. **SQL Warehouse HTTP Path**: SQL Warehouses → Connection details
3. **Personal Access Token**: Avatar → User Settings → Developer → Personal access tokens

---

### **BƯỚC 4: Setup Backend (FastAPI)**

#### 4.1 Tạo Virtual Environment & Cài Packages
```bash
cd backend

# Tạo virtual environment
python -m venv venv

# Activate venv
# Windows:
venv\Scripts\activate
# Mac/Linux:
# source venv/bin/activate

# Cài dependencies
pip install -r requirements.txt
```

#### 4.2 Update `.env`
File: `backend/.env`
```
DATABRICKS_HOST=<your_workspace_url>
DATABRICKS_PATH=<your_warehouse_path>
DATABRICKS_TOKEN=<your_access_token>
```
**Điền credentials từ Bước 3 vào đây**

#### 4.3 Chạy Backend
```bash
python server.py
# hoặc
uvicorn server:app --reload --port 8000

# Output: 
# INFO:     Uvicorn running on http://0.0.0.0:8000
```

#### 4.4 Test Backend
```bash
# Health check
curl http://localhost:8000/api/health

# Get devices
curl http://localhost:8000/api/devices
```

---

### **BƯỚC 5: Setup Frontend**

#### 5.1 Cài Dependencies
```bash
cd frontend
npm install
```

#### 5.2 Chạy Frontend
```bash
npm run dev
# Output:
# ➜  Local:   http://localhost:5173/
```

#### 5.3 Mở Browser
```
http://localhost:5173
```

---

## 🎨 Giao Diện Dashboard

### **Layout Chính**
```
┌─────────────────────────────────────────────────────────┐
│ 📱 IoT Devices              [🔄 Refresh] [➕ Add Device] │
│ Manage your IoT sensors and devices                     │
└─────────────────────────────────────────────────────────┘

┌─────────────────┬─────────────────┬─────────────────┐
│ ROOM 1 TEMP     │ BR HUMIDITY     │ GD SOIL MOISTURE│
│ [temperature]   │ [humidity]      │ [soil_moisture] │
│ 📍 Living Room  │ 📍 Bedroom      │ 📍 Garden       │
│ 19.19 °C        │ 81.49 %         │ 29.90 %         │
│ ● Active        │ ● Active        │ ● Active        │
│ [View] [DC] [D] │ [View] [DC] [D] │ [View] [DC] [D] │
└─────────────────┴─────────────────┴─────────────────┘
```

### **Modal Detail (Click "View Details")**
```
┌─────────────────────────────────────────────────────┐
│ ROOM 1 TEMPERATURE - Detailed Statistics         [X]
├─────────────────────────────────────────────────────┤
│ 📊 Current Value: 19.19 °C
│
│ 📈 Statistics (Last 2 Hours):
│ ┌──────────┬──────────┬──────────┐
│ │ Average  │ Minimum  │ Maximum  │
│ │  19.19   │  18.50   │  20.05   │
│ └──────────┴──────────┴──────────┘
│
│ 📉 Trend Chart (Last 2 Hours):
│ ┌─────────────────────────────┐
│ │    [SVG Line Chart]         │
│ │    (Cyan line + points)     │
│ └─────────────────────────────┘
└─────────────────────────────────────────────────────┘
```

---

## 🔄 Workflow

### **Quick Start (5 phút)**

#### Terminal 1 - Backend
```bash
cd backend

# Activate venv (Windows)
venv\Scripts\activate

# Run
python server.py
# Chờ: "INFO:     Uvicorn running on http://0.0.0.0:8000"
```

#### Terminal 2 - Test Backend (Optional)
```bash
curl http://localhost:8000/api/health
# Output: {"status":"ok","message":"Backend is running"}
```

#### Terminal 3 - Frontend
```bash
cd frontend
npm run dev
# Chờ: "➜  Local:   http://localhost:5173/"
```

#### Browser
```
http://localhost:5173
```

---

## 📊 API Endpoints

| Endpoint | Method | Response |
|----------|--------|----------|
| `/api/health` | GET | `{"status": "ok"}` |
| `/api/devices` | GET | Array of devices |
| `/api/device/{id}/stats` | GET | `{min_value, max_value, avg_value}` |
| `/api/device/{id}/timeseries` | GET | Array of `{timestamp, value}` |

### **Ví dụ Requests**

```bash
# Lấy tất cả devices
curl http://localhost:8000/api/devices

# Lấy stats của device
curl http://localhost:8000/api/device/LR_TEMP_001/stats

# Lấy timeseries
curl http://localhost:8000/api/device/LR_TEMP_001/timeseries
```

---

## 🛠️ Cấu Hình Nâng Cao

### **Chạy Streaming Lâu Hơn**

Sửa `databricks_iot_streaming.py` - PART 8:

```python
# Hiện tại: 100 giây (10 iterations)
max_iterations = 10

# Thay bằng:
max_iterations = 1440   # 4 giờ
max_iterations = 4320   # 12 giờ
```

### **Tăng Tần Suất Generate Data** (5s thay vì 10s)

```python
# Hiện tại
time.sleep(10)

# Thay bằng
time.sleep(5)
```

### **Production Deployment**

```bash
# Sử dụng Gunicorn
pip install gunicorn
gunicorn -w 4 -b 0.0.0.0:8000 server:app

# Với hot reload
gunicorn -w 4 -b 0.0.0.0:8000 --reload server:app
```

---

## 🔍 Troubleshooting

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

### ❌ Lỗi: "Table not found"

**Giải pháp**:
1. Kiểm tra streaming notebook đã chạy?
2. Query Databricks:
   ```sql
   SELECT COUNT(*) FROM workspace.metrics_app_streaming.iot_sensor_data;
   ```

### ❌ Frontend không load

**Giải pháp**:
1. Check `http://localhost:5173` - Vite error?
2. Browser console (F12) - có error?
3. Rebuild: `npm run build`

### ❌ CORS Error

**Giải pháp**: CORS đã enable ở FastAPI (allow all origins), kiểm tra backend chạy chưa

### ❌ Biểu đồ không hiển thị dữ liệu (Chart shows "No data available")

**Nguyên nhân & Giải pháp**:
1. **Streaming đã dừng**: Streaming script chỉ tạo dữ liệu trong khoảng thời gian nhất định
   - Kiểm tra: Xem `iot_sensor_data` có hàng mới trong 2 giờ qua?
   ```sql
   SELECT COUNT(*) FROM workspace.metrics_app_streaming.iot_sensor_data
   WHERE timestamp >= CURRENT_TIMESTAMP() - INTERVAL 2 HOURS;
   ```
   - Để fix: Chạy lại `databricks_iot_streaming.py` (hoặc thiết lập Databricks Job chạy theo lịch)

2. **Dữ liệu chưa được tập hợp**: Kiểm tra table `device_timeseries_minutely` có dữ liệu không
   ```sql
   SELECT COUNT(*) FROM workspace.metrics_app_streaming.device_timeseries_minutely;
   ```

3. **Xem lỗi chi tiết**: Mở browser console (F12), tab "Console" - có error message không?

4. **Dashboard cards OK nhưng chart trống**: Điều này có nghĩa WebSocket + `/api/devices` hoạt động, nhưng `/api/device/:id/timeseries` gặp vấn đề
   - Kiểm tra trực tiếp:
   ```bash
   # Lấy device ID từ dashboard
   curl http://localhost:8000/api/device/LR_TEMP_001/timeseries
   ```

---

## 📞 Development Commands

### **Backend**
```bash
# Start server
python server.py

# Start với auto-reload
uvicorn server:app --reload --port 8000

# Test endpoint
curl http://localhost:8000/api/health
```

### **Frontend**
```bash
# Dev server
npm run dev

# Build
npm run build

# Preview
npm run preview
```

---

## ✅ Checklist Triển Khai

- [ ] Databricks workspace tạo
- [ ] `databricks_iot_streaming.py` upload & chạy
- [ ] Data được tạo (>= 50 rows)
- [ ] Lấy 3 Databricks credentials
- [ ] Update `backend/.env` với credentials
- [ ] `cd backend && python -m venv venv`
- [ ] Activate venv: `venv\Scripts\activate`
- [ ] `pip install -r requirements.txt`
- [ ] `python server.py` (Backend port 8000)
- [ ] `cd frontend && npm install`
- [ ] `npm run dev` (Frontend port 5173)
- [ ] Mở `http://localhost:5173`
- [ ] ✨ Click device cards!

---

## 🎬 Architecture

```
┌─────────────────────────┐
│   Browser (5173)        │ ← User thao tác
│   React + Vite          │
└────────────┬────────────┘
             │ HTTP
             ↓
┌─────────────────────────┐
│   FastAPI Backend (8000)│ ← API proxy
│   Python                │
└────────────┬────────────┘
             │ DBSQL
             ↓
┌─────────────────────────┐
│   Databricks            │ ← Data source
│   Delta Lake            │
│ metrics_app_streaming   │
└─────────────────────────┘
             ↑
             │ Streaming
             │
┌─────────────────────────┐
│   Streaming Engine      │
│   databricks_iot_       │
│   streaming.py          │
└─────────────────────────┘
```

---

## 📱 Devices Trong Hệ Thống

| Device | Type | Location | Range | Unit |
|--------|------|----------|-------|------|
| LR_TEMP_001 | temperature | Living Room | 15-28 | °C |
| BR_HUM_001 | humidity | Bedroom | 30-70 | % |
| GD_SOIL_001 | soil_moisture | Garden | 20-80 | % |
| OUT_TEMP_001 | temperature | Outdoor | 5-35 | °C |
| KT_HUM_001 | humidity | Kitchen | 30-75 | % |

---

## 🎨 Design Specs

### **Colors (CSS Variables)**
```css
--bg-dark: #0f1419;           /* Main dark background */
--bg-card: #1a2640;           /* Card background */
--border-cyan: #00d4ff;       /* Cyan borders */
--cyan-bright: #00d4ff;       /* Bright cyan text */
--orange-accent: #ff8c00;     /* Orange buttons */
--red-accent: #f43f5e;        /* Red delete button */
--green-active: #4ade80;      /* Green status */
```

---

## 💡 Tips

- **Databricks Credentials**: Copy từ Databricks UI vào `.env` (khỏi config trong code)
- **Virtual Environment**: Luôn activate venv trước khi run backend
- **CORS**: Đã enable ở FastAPI, không cần cấu hình thêm
- **Auto-reload**: Dùng `uvicorn server:app --reload` khi dev

---

## 📚 References

- **FastAPI**: https://fastapi.tiangolo.com/
- **Databricks SQL Connector**: https://docs.databricks.com/dev-tools/python-sql-connector
- **React**: https://react.dev/
- **Vite**: https://vitejs.dev/

---

**Happy Monitoring! 🚀**

