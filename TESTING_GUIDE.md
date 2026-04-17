# 🧪 Testing Databricks Connection

Hướng dẫn kiểm tra kết nối Databricks và dữ liệu.

---

## 📋 Step 1: Cài Dependencies

```bash
cd backend
pip install -r requirements.txt
```

**New packages:**
- `databricks-sql-connector[pyarrow]` - Connect to Databricks
- `pandas` - Data processing
- `pytz` - Timezone handling

---

## 🔑 Step 2: Setup .env File

Tạo hoặc cập nhật `backend/.env`:

```bash
# DATABRICKS Configuration (required)
DATABRICKS_SERVER_HOSTNAME=adb-1234567890.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/abc123xyz
DATABRICKS_TOKEN=dapi1234567890abcdef

# Optional
DATABRICKS_CATALOG=iot_analytics
DATABRICKS_TABLE=smart_filtered_measurements

# Kafka config (keep existing)
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=iot-sensor-data
```

**Cách lấy credentials:**
1. Vào Databricks workspace
2. Xem URL: `https://adb-<xxx>.cloud.databricks.com`
   → **Server Hostname** = `adb-<xxx>.cloud.databricks.com`
3. SQL Warehouses → Your Warehouse → Connection details
   → Copy **HTTP path**
4. Settings → Developer tools → Personal access tokens
   → **Generate and copy token**

---

## 🧪 Step 3: Run Test Scripts

### Test 1: Quick Test

```bash
cd backend
python test_databrick.py
```

**Expected output:**
```
Connected: True
Sensors: [{'sensor_id': 'sensor_1', 'location': '...', ...}, ...]
Records: N
```

### Test 2: Detailed Test (Recommended)

```bash
cd backend
python test_databricks_detailed.py
```

**This will test:**
1. ✅ Connection & table existence
2. ✅ Sample data from table
3. ✅ Distinct sensors & metrics
4. ✅ Date range of data
5. ✅ `get_sensors()` method
6. ✅ `query_measurements()` method

**Expected output:**
```
============================================================
🧪 Testing Databricks Connection & Tables
============================================================
✅ Kết nối Databricks thành công

📋 Test 1: Check nếu bảng tồn tại
------------------------------------------------------------
✅ Bảng tồn tại
   Total records: 1000

📊 Test 2: Sample data từ bảng
------------------------------------------------------------
Sample rows:

  Record 1:
    event_ts: 2026-04-17T14:35:22.000+07:00 (type: str)
    sensor_id: sensor_1
    location: Living_Room
    metric_type: temperature
    metric_value: 24.5
    unit: °C
    processed_at: 2026-04-17T14:35:22.000+07:00

[... more records ...]

📈 Test 3: Distinct sensors & metrics
------------------------------------------------------------
Found 5 sensor-metric combinations:
  • sensor_1 → temperature (°C)
  • sensor_2 → humidity (%)
  • sensor_3 → soil_moisture (%)
  • sensor_4 → light_intensity (lux)
  • sensor_5 → pressure (hPa)

📅 Test 4: Date range của dữ liệu
------------------------------------------------------------
  Earliest: 2026-04-16T14:35:22.000+07:00
  Latest: 2026-04-17T14:35:22.000+07:00
  Total: 1000 records

🔧 Test 5: Test databricks_client.get_sensors()
------------------------------------------------------------
✅ get_sensors() trả về 5 results:
  {'sensor_id': 'sensor_1', 'location': 'Living_Room', ...}
  ...

🔍 Test 6: Test databricks_client.query_measurements()
------------------------------------------------------------
✅ query_measurements() trả về 10 rows
Columns: ['event_ts', 'sensor_id', 'location', 'metric_type', 'metric_value', 'unit']
First row:
  event_ts: 2026-04-17T14:35:22.000+07:00
  sensor_id: sensor_1
  ...

============================================================
✅ Testing complete!
============================================================
```

---

## 🚀 Step 4: Test Backend API

Khi test script thành công, khởi động backend:

```bash
python main.py
```

Sẽ thấy:
```
INFO:     Uvicorn running on http://0.0.0.0:8000
```

### Test API endpoints

#### 4.1 Test `/api/analytics/sensors`

```bash
curl http://localhost:8000/api/analytics/sensors
```

**Response:**
```json
{
  "status": "ok",
  "data": [
    {
      "sensor_id": "sensor_1",
      "location": "Living_Room",
      "metric_type": "temperature",
      "unit": "°C"
    },
    ...
  ]
}
```

#### 4.2 Test `/api/analytics/measurements`

```bash
curl "http://localhost:8000/api/analytics/measurements?sensor_id=sensor_1&metric_type=temperature&from_date=2026-04-01&to_date=2026-04-30&limit=100"
```

**Response:**
```json
{
  "status": "ok",
  "count": 50,
  "data": [
    {
      "event_ts": "2026-04-17T14:35:22.000+07:00",
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

## 🐛 Troubleshooting

### ❌ "The table or view `iot_analytics`.`smart_filtered_measurements` cannot be found"

**Nguyên nhân:** Bảng chưa tồn tại hoặc tên sai

**Cách sửa:**
1. Kiểm tra bảng tồn tại trên Databricks:
   ```sql
   SHOW TABLES IN iot_analytics;
   ```
2. Nếu không có, tạo bảng từ notebook Databricks (xem DATABRICKS_SETUP.md)
3. Kiểm tra `DATABRICKS_CATALOG` và `DATABRICKS_TABLE` trong .env

### ❌ "Databricks credentials not fully configured"

**Nguyên nhân:** Missing env variables

**Cách sửa:**
```bash
# Kiểm tra .env có đủ các key:
cat backend/.env

# Phải có:
# DATABRICKS_SERVER_HOSTNAME=...
# DATABRICKS_HTTP_PATH=...
# DATABRICKS_TOKEN=...
```

### ❌ "Connection refused"

**Nguyên nhân:** Server hostname sai hoặc warehouse không chạy

**Cách sửa:**
1. Double-check `DATABRICKS_SERVER_HOSTNAME` (không có https://)
2. Kiểm tra SQL warehouse **đang chạy** trên Databricks UI

### ❌ "Invalid token"

**Nguyên nhân:** Token hết hạn hoặc sai

**Cách sửa:**
1. Vào Databricks → Settings → Developer tools
2. Generate **new** token
3. Update `.env`

### ❌ "pyarrow is not installed"

**Nguyên nhân:** Older databricks-sql-connector version

**Cách sửa:**
```bash
pip install --upgrade "databricks-sql-connector[pyarrow]>=2.0.0"
```

---

## 📊 Expected Table Schema

Bảng `iot_analytics.smart_filtered_measurements` phải có các cột:

```sql
event_ts TIMESTAMP          -- Timestamp (format: "2026-04-17T14:35:22.000+07:00")
sensor_id STRING            -- e.g., "sensor_1"
location STRING             -- e.g., "Living_Room"
metric_type STRING          -- e.g., "temperature", "humidity", ...
metric_value DOUBLE         -- e.g., 24.5
unit STRING                 -- e.g., "°C", "%", "lux"
threshold DOUBLE            -- Smart filtering threshold
store_reason STRING         -- "threshold", "max_interval", "first_record"
kafka_topic STRING          -- e.g., "iot-sensor-data"
kafka_partition INT         -- Partition number
kafka_offset LONG           -- Offset number
processed_at TIMESTAMP      -- When saved to table
```

---

## ✅ Validation Checklist

- [ ] `.env` file có đủ Databricks credentials
- [ ] SQL Warehouse **đang chạy** trên Databricks
- [ ] Bảng `iot_analytics.smart_filtered_measurements` tồn tại
- [ ] Bảng có **ít nhất 1 bản ghi** dữ liệu
- [ ] `test_databricks_detailed.py` chạy thành công
- [ ] Backend API `/api/analytics/sensors` trả về dữ liệu
- [ ] Backend API `/api/analytics/measurements` trả về dữ liệu

---

## 🎯 Next Steps

Khi tất cả test pass:

1. **Frontend:** `cd frontend && npm install && npm run dev`
2. **Open:** `http://localhost:5173`
3. **Tab:** "📊 Analytics"
4. **Select:** Sensor, metric, date range
5. **Click:** "🔍 Tìm Dữ Liệu"
6. **View:** Line chart từ Databricks data!

---

## 📚 References

- Databricks SQL Connector: https://docs.databricks.com/en/dev-tools/python-sql-connector.html
- Databricks API: https://docs.databricks.com/en/api/
- SQL Warehouses: https://docs.databricks.com/en/sql/admin/sql-warehouses.html
