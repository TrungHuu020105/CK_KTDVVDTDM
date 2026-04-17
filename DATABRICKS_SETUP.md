# 🔗 Setup Databricks Connection

Hướng dẫn này giúp bạn kết nối Backend tới Databricks để lấy dữ liệu từ bảng `iot_analytics.smart_filtered_measurements`.

---

## 📋 Yêu Cầu

1. ✅ Databricks Workspace có sẵn
2. ✅ Bảng `iot_analytics.smart_filtered_measurements` được tạo (bằng notebook Databricks)
3. ✅ Kafka topic `metrics` (hoặc tên khác) có dữ liệu từ sensor

---

## 🔑 Step 1: Lấy Databricks Credentials

### 1.1 Lấy Server Hostname

1. Đăng nhập vào Databricks workspace
2. Xem URL: `https://adb-<xxx>.cloud.databricks.com/?o=<xxx>`
3. **Server Hostname** = `adb-<xxx>.cloud.databricks.com`

Ví dụ:
```
URL: https://adb-1234567890.cloud.databricks.com/?o=123
Server Hostname: adb-1234567890.cloud.databricks.com
```

### 1.2 Lấy HTTP Path

1. Databricks → SQL Warehouses → (Select your warehouse)
2. Xem tab "Connection details"
3. Copy: **HTTP path** (e.g., `/sql/1.0/warehouses/abc123`)

### 1.3 Lấy Personal Access Token

1. Databricks → Settings → Developer tools
2. Personal access tokens → Generate new token
3. Copy token ngay (chỉ hiển thị 1 lần)

---

## 🔧 Step 2: Cấu Hình Backend

### 2.1 Tạo file `.env` trong folder `backend/`

```bash
cd backend
cp .env.example .env
```

### 2.2 Sửa file `backend/.env`

```env
# DATABRICKS Configuration
DATABRICKS_SERVER_HOSTNAME=adb-1234567890.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/abc123xyz
DATABRICKS_TOKEN=dapi1234567890abcdef
DATABRICKS_CATALOG=iot_analytics
DATABRICKS_TABLE=smart_filtered_measurements

# KAFKA (giữ nguyên hoặc update nếu cần)
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=iot-sensor-data
```

### 2.3 Cài dependencies

```bash
cd backend
pip install -r requirements.txt
```

**Dependencies mới:**
- `databricks-sql-connector==0.4.1` - Kết nối Databricks
- `pandas==2.1.3` - Xử lý dữ liệu
- `pytz==2024.1` - Timezone

---

## ✅ Step 3: Kiểm Tra Kết Nối

### 3.1 Test trong Python REPL

```bash
cd backend
python
```

```python
from databricks_client import get_databricks_client

client = get_databricks_client()
print("Connected:", client.is_connected())

# Lấy danh sách sensors
sensors = client.get_sensors()
print("Sensors:", sensors)

# Query data
df = client.query_measurements(
    sensor_id='sensor_1',
    metric_type='temperature',
    from_date='2024-01-01',
    to_date='2024-01-31'
)
print("Records:", len(df))
```

**Output mong đợi:**
```
Connected: True
Sensors: [
  {'sensor_id': 'sensor_1', 'location': 'Living_Room', 'metric_type': 'temperature', 'unit': '°C'},
  ...
]
Records: 150
```

### 3.2 Test Backend API

Khởi động backend:
```bash
python main.py
```

Test endpoints:

```bash
# Test 1: Lấy danh sách sensors
curl http://localhost:8000/api/analytics/sensors

# Test 2: Lấy dữ liệu
curl "http://localhost:8000/api/analytics/measurements?sensor_id=sensor_1&metric_type=temperature&from_date=2024-01-01&to_date=2024-01-31&limit=1000"
```

**Response mong đợi:**
```json
{
  "status": "ok",
  "count": 150,
  "data": [
    {
      "event_ts": "2024-01-31T23:59:00+07:00",
      "sensor_id": "sensor_1",
      "location": "Living_Room",
      "metric_type": "temperature",
      "metric_value": 24.5,
      "unit": "°C"
    },
    ...
  ]
}
```

---

## 🚀 Step 4: Chạy Với Frontend

### 4.1 Install Frontend dependencies

```bash
cd frontend
npm install
```

### 4.2 Khởi động Backend + Frontend

**Terminal 1 - Backend:**
```bash
cd backend
python main.py
```

**Terminal 2 - Frontend:**
```bash
cd frontend
npm run dev
```

### 4.3 Mở Dashboard

```
http://localhost:5173
```

Chọn tab **"📊 Analytics"** để xem dữ liệu từ Databricks!

---

## 🐛 Troubleshooting

### ❌ "Databricks not connected"

```
Nguyên nhân: Credentials sai hoặc server không phản hồi
Cách sửa:
1. Double-check DATABRICKS_SERVER_HOSTNAME (không có https://)
2. Kiểm tra DATABRICKS_TOKEN còn hợp lệ không
3. Kiểm tra SQL warehouse chạy không (trên Databricks UI)
4. Kiểm tra internet connection
```

### ❌ "Invalid HTTP Path"

```
Nguyên nhân: HTTP path sai
Cách sửa:
1. Vào Databricks → SQL Warehouses
2. Copy đúng HTTP path từ "Connection details"
3. Format: /sql/1.0/warehouses/abc123xyz
```

### ❌ "No data found"

```
Nguyên nhân: Bảng rỗng hoặc query sai
Cách sửa:
1. Kiểm tra notebook Databricks đã chạy không
2. Kiểm tra Kafka topic có data không
3. Kiểm tra date range có data không
```

### ❌ "Import error databricks.sql"

```
Nguyên nhân: Library không cài
Cách sửa:
pip install databricks-sql-connector==0.4.1
pip install -r requirements.txt
```

---

## 📊 Cấu trúc Dữ Liệu

**Bảng: `iot_analytics.smart_filtered_measurements`**

```sql
event_ts TIMESTAMP          -- Timestamp cảm biến ghi nhận (VN timezone)
sensor_id STRING            -- Mã cảm biến (sensor_1, sensor_2, ...)
location STRING             -- Vị trí (Living_Room, Garden, Outdoor)
metric_type STRING          -- Loại chỉ số (temperature, humidity, ...)
metric_value DOUBLE         -- Giá trị đo được
unit STRING                 -- Đơn vị (°C, %, lux, hPa)
threshold DOUBLE            -- Ngưỡng smart filtering
store_reason STRING         -- Lý do lưu (threshold, max_interval, first_record)
kafka_topic STRING          -- Topic Kafka
kafka_partition INT         -- Partition Kafka
kafka_offset LONG           -- Offset Kafka
processed_at TIMESTAMP      -- Khi nào lưu vào Delta
```

---

## 💡 Tips

1. **Performance:** Query giới hạn date range để tránh lấy dữ liệu quá nhiều
2. **Caching:** Frontend sẽ cache dữ liệu khi lấy từ API
3. **Real-time:** Databricks dữ liệu sẽ ~delay 1-2 phút so với WebSocket real-time
4. **Cost:** Databricks SQL tính chi phí theo compute, hạn chế query liên tục

---

## 📚 Tham Khảo

- Databricks SQL Connector: https://docs.databricks.com/en/dev-tools/python-sql-connector.html
- Databricks API: https://docs.databricks.com/en/api/
