# 📱 IoT Devices Dashboard - Hướng Dẫn Sử Dụng

## 🎯 Tổng Quan

Hệ thống IoT streaming real-time với:
- **Backend**: Databricks + Spark Streaming + Delta Lake
- **Frontend**: Streamlit Web App (localhost:8501)
- **Giao diện**: Card-based design, click để xem chi tiết stats & charts

---

## 📂 Cấu Trúc Dự Án

```
CK1_DTDM/
├── databricks_iot_streaming.py          # Streaming engine
├── streamlit_dashboard.py               # Web dashboard
├── requirements.txt                     # Dependencies
├── HuongDan.md                          # File hướng dẫn này
└── README.md
```

---

## 🚀 Quy Trình Triển Khai

### **BƯỚC 1: Chuẩn Bị Databricks**

#### 1.1 Tạo Catalog & Schema
Trên Databricks SQL Editor, chạy:
```sql
CREATE CATALOG IF NOT EXISTS workspace;
CREATE SCHEMA IF NOT EXISTS workspace.metrics_app_streaming;
```

#### 1.2 Xác Minh
```sql
-- Kiểm tra catalog
SHOW CATALOGS;
-- Kết quả: samples, system, workspace ✅

-- Kiểm tra schema
SHOW SCHEMAS IN workspace;
```

---

### **BƯỚC 2: Chạy Streaming Engine**

#### 2.1 Upload & Chạy Notebook
1. Mở **Databricks Workspace**
2. **Repos** → **Create** → Upload file `databricks_iot_streaming.py`
3. Select cluster → **Run All**

**Thời gian**: ~100 giây (10 iterations, mỗi 10s)

#### 2.2 Kiểm Tra Data
```sql
-- Nên có >= 50 rows
SELECT COUNT(*) FROM workspace.metrics_app_streaming.iot_sensor_data;

-- Xem mẫu data
SELECT * FROM workspace.metrics_app_streaming.iot_latest_readings LIMIT 5;
```

---

### **BƯỚC 3: Lấy Databricks Credentials**

Cần 3 thông tin để kết nối từ local:

#### 🔑 **1. Server Hostname**
```
Databricks → Settings (top-right) → Workspace URL
Ví dụ: dbc-8ffd6052-91ee.cloud.databricks.com
```

#### 🔑 **2. SQL Warehouse HTTP Path**
```
SQL Warehouses → Chọn warehouse → Connection details
Dạng: /sql/1.0/warehouses/3920086a375b89dc
```

#### 🔑 **3. Personal Access Token (PAT)**
```
Avatar (top-right) → User Settings → Developer 
→ Personal access tokens → Generate new token
Dạng: dapixxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

---

### **BƯỚC 4: Config Local Machine**

#### 4.1 Cài Đặt Python Packages
```bash
pip install streamlit databricks-sql-connector plotly pandas
```

#### 4.2 Update Credentials
Mở `streamlit_dashboard.py`, tìm `DATABRICKS_CONFIG`:

```python
DATABRICKS_CONFIG = {
    "server_hostname": "dbc-8ffd6052-91ee.cloud.databricks.com",  # ← Thay
    "http_path": "/sql/1.0/warehouses/3920086a375b89dc",         # ← Thay
    "personal_access_token": "dapixxxxxxxxxxxxxxxxxxxxxxxxxxxx",  # ← Thay
    "catalog": "workspace",
    "schema": "metrics_app_streaming"
}
```

#### 4.3 Chạy Streamlit
```bash
cd "d:\DuLieuCuaHuu\HK2_20252026\KTHDVVDTDM\CK\CK1_DTDM"
streamlit run streamlit_dashboard.py
```

**Output:**
```
You can now view your Streamlit app in your browser.
Local URL: http://localhost:8501
```

#### 4.4 Mở Browser
```
http://localhost:8501
```

---

## 🎨 Giao Diện Dashboard

### **Layout Chính**
- **Header**: Tiêu đề "📱 IoT Devices" + nút Refresh
- **Card Grid**: 3 cột hiển thị các thiết bị
- **Modal Detail**: Click device card → xem chi tiết

### **Mỗi Device Card**
```
┌─────────────────────────────────────┐
│ ROOM 1 TEMPERATURE        [temperature]
│ Source: sensor_1
│ Created by: letrunghuu
│ 📍 Living Room
│
│ ┌─ Real-time Value ─┐
│ │      19.19 °C     │
│ └───────────────────┘
│
│ ● Active
│ [👁️ View Details] ← Click!
│ [🔌 Disconnect] [🗑️ Delete]
└─────────────────────────────────────┘
```

### **Modal View (Click "View Details")**
```
┌─────────────────────────────────────────┐
│ ROOM 1 TEMPERATURE - Detailed Stats  [X]
│────────────────────────────────────────│
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
│ │  [Line chart with Plotly]   │
│ └─────────────────────────────┘
│
│ [❌ Close]
└─────────────────────────────────────────┘
```

---

## 🔄 Hướng Dẫn Sử Dụng

### **Workflow Cơ Bản**

1. **Bật Streaming** (Databricks)
   ```
   Run databricks_iot_streaming.py
   Chờ ~100 giây để generate data
   ```

2. **Bật Dashboard** (Local)
   ```
   streamlit run streamlit_dashboard.py
   Mở http://localhost:8501
   ```

3. **Xem Device Cards**
   ```
   Dashboard hiển thị 5 devices (3 loại: temperature, humidity, soil_moisture)
   Devices được hiển thị chung trong layout 3-cột
   ```

4. **Xem Chi Tiết**
   ```
   Click "👁️ View Details" → Modal mở ra
   Hiển thị: Average, Min, Max + Trend Chart
   ```

5. **Refresh Data**
   ```
   Click nút "🔄 Refresh" để cập nhật data mới
   ```

---

## 📊 Devices Trong Hệ Thống

| Device | Type | Location | Giá Trị Mẫu | Đơn Vị |
|--------|------|----------|-----------|--------|
| LR_TEMP_001 | temperature | Living Room | 19.19 | °C |
| BR_HUM_001 | humidity | Bedroom | 81.49 | % |
| GD_SOIL_001 | soil_moisture | Garden | 29.90 | % |
| OUT_TEMP_001 | temperature | Outdoor | 15.75 | °C |
| KT_HUM_001 | humidity | Kitchen | 45.20 | % |

---

## 💾 Delta Lake Tables

### **1. iot_sensor_data** (Raw streaming data)
```
timestamp (TIMESTAMP)       - Thời điểm đo
device_id (STRING)          - ID thiết bị
device_name (STRING)        - Tên thiết bị
device_type (STRING)        - Loại (temperature/humidity/soil_moisture)
location (STRING)           - Vị trí
value (DOUBLE)              - Giá trị đo được
unit (STRING)               - Đơn vị (°C, %, lux, ...)
batch_id (STRING)           - ID batch/session
_processing_time (TIMESTAMP)- Thời gian xử lý
```

### **2. iot_latest_readings** (Latest values per device)
```
device_id (STRING)          - ID thiết bị
device_name (STRING)        - Tên thiết bị
device_type (STRING)        - Loại cảm biến
location (STRING)           - Vị trí
latest_value (DOUBLE)       - Giá trị mới nhất
unit (STRING)               - Đơn vị
last_update (TIMESTAMP)     - Lần cập nhật gần nhất
status (STRING)             - ACTIVE / INACTIVE
```

### **3. iot_device_metadata** (Device configuration)
```
device_id (STRING)          - ID thiết bị
device_name (STRING)        - Tên thiết bị
device_type (STRING)        - Loại
location (STRING)           - Vị trí
unit (STRING)               - Đơn vị
min_value (DOUBLE)          - Giá trị min
max_value (DOUBLE)          - Giá trị max
mean_value (DOUBLE)         - Giá trị trung bình
std_dev (DOUBLE)            - Độ lệch chuẩn
active (BOOLEAN)            - Đang hoạt động?
created_at (TIMESTAMP)      - Thời tạo
```

---

## ⚙️ Cấu Hình Nâng Cao

### **Chạy Streaming Lâu Hơn**

Sửa `databricks_iot_streaming.py` - PART 8:

```python
# Hiện tại: 100 giây (10 iterations)
max_iterations = 10

# Thay bằng:
max_iterations = 360    # 1 giờ
max_iterations = 1440   # 4 giờ
max_iterations = 4320   # 12 giờ
```

### **Tăng Tần Suất** (Mỗi 5 giây thay vì 10 giây)

```python
# Hiện tại
time.sleep(10)

# Thay bằng
time.sleep(5)
```

### **Giảm Tần Suất** (Mỗi 30 giây)

```python
time.sleep(30)
```

---

## 🔍 Troubleshooting

### ❌ Lỗi: "Table not found"
**Giải pháp**:
1. Kiểm tra `DATABRICKS_CONFIG` trong `streamlit_dashboard.py`
2. Chạy query trên Databricks SQL:
   ```sql
   SELECT COUNT(*) FROM workspace.metrics_app_streaming.iot_sensor_data;
   ```
3. Nếu lỗi, chạy lại streaming notebook

### ❌ Lỗi: "Connection timeout"
**Giải pháp**:
1. Xác nhận credentials: hostname, http_path, token
2. Kiểm tra SQL Warehouse **đang Running** (không Stopped)
3. Kiểm tra internet connection

### ❌ Dashboard không hiển thị gì
**Giải pháp**:
1. Bấm nút "🔄 Refresh"
2. Kiểm tra console có error không
3. Xác nhận streaming notebook đã tạo data

### ❌ Streamlit không khởi động
**Giải pháp**:
```bash
# Cập nhật
pip install --upgrade streamlit

# Chạy lại
streamlit run streamlit_dashboard.py
```

---

## 📝 SQL Queries Hữu Ích

### **Kiểm Tra Data Count**
```sql
SELECT 
    'iot_sensor_data' as table_name,
    COUNT(*) as row_count
FROM workspace.metrics_app_streaming.iot_sensor_data
UNION ALL
SELECT 
    'iot_latest_readings',
    COUNT(*)
FROM workspace.metrics_app_streaming.iot_latest_readings;
```

### **Xem Latest Data**
```sql
SELECT * FROM workspace.metrics_app_streaming.iot_latest_readings
ORDER BY last_update DESC;
```

### **Reset (Xóa tất cả data)**
```sql
DROP TABLE IF EXISTS workspace.metrics_app_streaming.iot_sensor_data;
DROP TABLE IF EXISTS workspace.metrics_app_streaming.iot_latest_readings;
DROP TABLE IF EXISTS workspace.metrics_app_streaming.iot_device_metadata;
```

### **Thống Kê theo Device Type**
```sql
SELECT 
    device_type,
    COUNT(*) as count,
    ROUND(AVG(value), 2) as avg_value,
    ROUND(MIN(value), 2) as min_value,
    ROUND(MAX(value), 2) as max_value
FROM workspace.metrics_app_streaming.iot_sensor_data
GROUP BY device_type
ORDER BY device_type;
```

---

## ✅ Checklist Triển Khai

- [ ] Databricks workspace sẵn sàng
- [ ] Catalog `workspace` + Schema `metrics_app_streaming` tạo
- [ ] `databricks_iot_streaming.py` upload & chạy
- [ ] Data được tạo (check: >= 50 rows trong table)
- [ ] Lấy 3 credentials (hostname, http_path, token)
- [ ] Update credentials ở `streamlit_dashboard.py`
- [ ] Install packages: `pip install streamlit databricks-sql-connector plotly pandas`
- [ ] Chạy: `streamlit run streamlit_dashboard.py`
- [ ] Mở: `http://localhost:8501`
- [ ] ✨ Click device cards để xem chi tiết!

---

## 🎬 Quick Start (2 phút)

```bash
# 1. Databricks - Run notebook
databricks_iot_streaming.py → Run All
Chờ 100 giây...

# 2. Local - Config & Run
# 2.1 Sửa DATABRICKS_CONFIG ở streamlit_dashboard.py
# 2.2 Chạy:
streamlit run streamlit_dashboard.py

# 3. Browser
http://localhost:8501
```

---

## 📞 Hỗ Trợ

| Vấn đề | File | Giải Pháp |
|--------|------|----------|
| Streaming không chạy | `databricks_iot_streaming.py` | Check PART 1-8, Run All |
| Data không update | Console log | Kiểm tra iteration counter |
| Connection error | `streamlit_dashboard.py` | Update credentials (line 114-119) |
| Dashboard trống | Network | Refresh, check warehouse running |

---

**Happy Monitoring! 🚀**

💡 **Tip**: Nếu muốn data liên tục, set `max_iterations = 4320` (12 giờ) rồi bookmark dashboard để monitor real-time! 📊
