# 🚀 IoT Real-time Dashboard with Analytics

**Hệ thống IoT hoàn chỉnh** từ mô phỏng cảm biến → Dashboard real-time → Phân tích sâu trên Databricks.

Dữ liệu chảy qua **2 luồng chính**: Luồng 1 cấp Real-time WebSocket (~100-350ms latency) và Luồng 2 Batch Analytics trên Databricks với Smart Filtering giảm 90-95% dung lượng lưu trữ.

---

## 📊 Kiến Trúc Hệ Thống

### **Luồng 1: Real-time Dashboard (WebSocket - Latency ~100-350ms)**
```
┌─────────────────────┐
│ Sensor Simulator    │ (5 cảm biến: temp, humidity, soil_moisture, light, pressure)
│ Python Script       │
└──────────┬──────────┘
           │ MQTT (port 1883)
           ↓
┌─────────────────────┐
│ Mosquitto Broker    │ (Local Docker)
│ (Local - Docker)    │
└──────────┬──────────┘
           │
           ↓
┌─────────────────────┐
│ MQTT→Kafka Bridge   │ (Chuyển MQTT → Kafka JSON)
└──────────┬──────────┘
           │ Kafka API (TLS/SASL)
           ↓
┌─────────────────────┐
│ Confluent Cloud     │
│ Kafka Cluster       │
│ (Cloud - Managed)   │
└──────────┬──────────┘
           │
           ↓
┌─────────────────────┐
│ FastAPI Backend     │ (Aiokafka Async Consumer)
│ (port 8000)         │
└──────────┬──────────┘
           │ WebSocket
           ↓
┌─────────────────────┐
│ React Dashboard     │ (Real-time UI)
│ (port 5173)         │
└─────────────────────┘
```

### **Luồng 2: Analytics (Batch - Databricks)**
```
Confluent Cloud Kafka → Databricks Spark Streaming
                     → Smart Filtering (Threshold + Max Interval)
                     → Delta Lake Tables
                     → Data Analysis & Reports
```
**Lợi ích:** 90-95% tiết kiệm dung lượng

---

## 📁 Cấu Trúc Dự Án

```
CK3_DTDM/
│
├── 📄 docker-compose.yml              # Khởi động MQTT + Kafka
├── 📄 README.md                       # Tài liệu này
├── 📄 QUICK_START.md                  # Setup nhanh 5 phút
├── 📄 TESTING_GUIDE.md                # Hướng dẫn test Databricks
├── 📄 DATABRICKS_SETUP.md             # Setup Databricks
│
├── 📁 sensor/                         # Sensor Simulator
│   ├── sensor_simulator.py            # Mô phỏng 5 cảm biến
│   └── requirements.txt
│
├── 📁 mosquitto/                      # MQTT Message Broker
│   ├── config/
│   │   └── mosquitto.conf
│   ├── data/
│   └── log/
│
├── 📁 bridge/                         # MQTT → Kafka Bridge
│   ├── mqtt_kafka_bridge.py
│   └── requirements.txt
│
├── 📁 backend/                        # FastAPI Backend
│   ├── main.py                        # WebSocket + Kafka Consumer + Databricks API
│   ├── databricks_client.py           # Databricks SQL Connector
│   └── requirements.txt
│
├── 📁 frontend/                       # React + Vite Dashboard
│   ├── package.json
│   ├── vite.config.js
│   ├── index.html
│   ├── src/
│   │   ├── main.jsx
│   │   ├── App.jsx
│   │   ├── App.css
│   │   ├── index.css
│   │   └── components/
│   │       ├── Dashboard.jsx          # Main dashboard + device cards
│   │       ├── Analytics.jsx          # Analytics tab (Databricks queries)
│   │       ├── DatabricksChart.jsx    # Line chart component
│   │       └── styles/
│   │           ├── Analytics.css
│   │           └── Dashboard.css
│   └── public/
│
└── 📁 databricks/                     # Analytics Notebooks
    ├── smart_filtering_kafka_to_delta_fixed.ipynb
    ├── synthetic_iot_dashboard_data_generator.ipynb
    ├── model_XGBoost/
    │   ├── 01_minute_aggregation.py
    │   ├── 02_direct_shared_features.py
    │   ├── 03_train_xgboost_direct_shared.py
    │   └── 04_generate_1h_forecast.py
    └── manifest.mf
```

---

## 🔌 5 Cảm Biến (Sensor Data)

Mỗi cảm biến đo **1 chỉ số duy nhất**, dữ liệu được mô phỏng bằng **Random Walk model + Day/Night trend cycles**:

| Sensor | Chỉ Số | Vị Trí | Phạm Vi | Đơn Vị | Mô Phỏng |
|--------|--------|--------|---------|--------|----------|
| **sensor_1** | Temperature | Living_Room | 15-40 | °C | Chu kỳ ngày/đêm |
| **sensor_2** | Humidity | Living_Room | 20-95 | % | Ngược chiều temp |
| **sensor_3** | Soil Moisture | Garden | 8-95 | % | Bốc hơi + tưới giả lập |
| **sensor_4** | Light Intensity | Outdoor | 0-60000 | lux | Mặt trời từ 6h-18h |
| **sensor_5** | Pressure | Outdoor | 990-1035 | hPa | Chu kỳ áp suất 24h |

**Format JSON (MQTT/Kafka):**
```json
{
  "timestamp": "2026-04-17T14:35:22.123456",
  "sensor_id": "sensor_1",
  "location": "Living_Room",
  "metric_type": "temperature",
  "unit": "°C",
  "temperature": 24.5
}
```

---

## 🌐 Ports & Services

| Service | Port | Vị Trí | Mục Đích | Ghi Chú |
|---------|------|--------|---------|--------|
| **Mosquitto MQTT** | 1883 | Local (Docker) | Message Broker | Không auth mặc định |
| **Mosquitto WebSocket** | 9001 | Local (Docker) | MQTT over WebSocket | For browser clients |
| **Confluent Cloud Kafka** | 9092 (API) | Cloud | Message Queue | SASL_SSL + API keys |
| **FastAPI** | 8000 | Local | REST API + WebSocket | ws://localhost:8000/ws |
| **React Frontend** | 5173 | Local | Dashboard UI | http://localhost:5173 |

---

## 🛠️ Tech Stack

| Layer | Technology | Version | Mục Đích |
|-------|-----------|---------|---------|
| **Message Broker** | Eclipse Mosquitto | Latest | MQTT protocol |
| **Message Queue** | Apache Kafka / Confluent Cloud | 7.5.0 | Distributed streaming |
| **Backend Framework** | FastAPI | 0.104.1 | Async REST API |
| **Async Kafka Client** | Aiokafka | 0.10.0 | Async Kafka consumer |
| **Frontend Framework** | React + Vite | 18.2.0 + 5.0.8 | Modern UI |
| **Charting** | Recharts | 2.10.3 | Line/Area charts |
| **Analytics** | Databricks + Spark | Latest | Phân tích lớn |
| **Storage** | Delta Lake | Latest | ACID transactions |
| **Containerization** | Docker + Compose | Latest | Local development |

---

## 📋 Yêu Cầu Hệ Thống

### **Local Machine (Bắt buộc)**
```bash
✅ Docker & Docker Compose (≥ 1.29)
✅ Python 3.8+ 
✅ Node.js 16+ & npm
✅ 2GB RAM tối thiểu
✅ 5GB disk space
✅ Internet connection (Confluent Cloud Kafka)
```

**Kiểm tra:**
```bash
docker-compose --version  # >= 1.29
python --version           # >= 3.8
node --version             # >= 16
npm --version              # >= 7
```

### **Cloud (Bắt buộc - Cho Kafka & Databricks Analytics)**
```bash
✅ Confluent Cloud account + Kafka cluster API keys
✅ Databricks workspace + SQL Warehouse
⭕ Unity Catalog (tuỳ chọn)
```

---

## 🚀 Quick Start

### **⏱️ Setup trong 5 phút:**

1. **Khởi động Docker (MQTT + Kafka):**
   ```bash
   docker-compose up -d
   sleep 15  # Chờ Kafka start
   docker-compose ps
   ```

2. **Cài đặt Python dependencies (3 dự án):**
   ```bash
   cd backend && pip install -r requirements.txt && cd ..
   cd bridge && pip install -r requirements.txt && cd ..
   cd sensor && pip install -r requirements.txt && cd ..
   ```

3. **Cài đặt Frontend:**
   ```bash
   cd frontend && npm install && cd ..
   ```

4. **Chạy 4 services (Mở 4 terminals):**

   **Terminal 1 - Backend (FastAPI WebSocket Server):**
   ```bash
   cd backend
   python main.py
   # Chờ: INFO: Uvicorn running on http://0.0.0.0:8000
   ```

   **Terminal 2 - Bridge (MQTT → Kafka):**
   ```bash
   cd bridge
   python mqtt_kafka_bridge.py
   # Chờ: ✓ Kết nối tới MQTT broker thành công
   ```

   **Terminal 3 - Sensor Simulator:**
   ```bash
   cd sensor
   python sensor_simulator.py
   # Chờ: [1] ✓ Sensor: sensor_1 | temperature: 24.5 °C
   ```

   **Terminal 4 - Frontend (React Vite):**
   ```bash
   cd frontend
   npm run dev
   # Chờ: ➜ Local: http://localhost:5173/
   ```

5. **Mở Dashboard:**
   ```
   http://localhost:5173
   ```
   Bạn sẽ thấy:
   - 🟢 **WebSocket Status:** Connected
   - 📈 **5 Sensor Cards** cập nhật real-time
   - 📊 **Analytics Tab** (nếu có Databricks)

---

## ✅ Verify Setup

### **Health Check Backend:**
```bash
curl http://localhost:8000/api/health
```
**Response:**
```json
{"status": "ok", "connected_clients": 1, "kafka_connected": true}
```

### **Test MQTT Messages:**
```bash
docker-compose exec mosquitto mosquitto_sub -t "sensors/iot/data"
```

### **Test Kafka Topic:**
```bash
docker exec <kafka-container> kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic iot-sensor-data --from-beginning
```

---

## 📡 API Documentation

### **REST Endpoints**

```bash
# Health check
GET /api/health
→ {status, connected_clients, kafka_connected}

# System status  
GET /api/status
→ {app, version, kafka_topic, connected_clients, kafka_broker}

# Sensors từ Databricks
GET /api/analytics/sensors
→ [{sensor_id, location, metric_type, unit}, ...]

# Query measurements từ Databricks
GET /api/analytics/measurements?sensor_id=sensor_1&metric_type=temperature&from_date=2026-04-01&to_date=2026-04-30&limit=1000
→ {status, count, data: [{event_ts, sensor_id, metric_value, unit}, ...]}

# Recent minutely data (last 2 hours)
GET /api/analytics/recent-minutely?sensor_id=sensor_1&metric_type=temperature&lookback_minutes=120
→ {status, count, data: [{minute_ts, avg_value, sample_count, unit}, ...]}
```

### **WebSocket**

```javascript
// Connect
const ws = new WebSocket('ws://localhost:8000/ws');

// Receive real-time data
ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log(data.sensor_id, data.temperature);
};

// Send ping (keep-alive)
ws.send('ping');
```

**Real-time Data Format:**
```json
{
  "timestamp": "2026-04-17T14:35:22.123456",
  "sensor_id": "sensor_1",
  "location": "Living_Room",
  "metric_type": "temperature",
  "unit": "°C",
  "temperature": 24.5
}
```

---

## 🎯 Features & Use Cases

### **✨ Main Features**
- ✅ **Real-time Streaming** - WebSocket data updates every 5 seconds
- ✅ **Async Processing** - Aiokafka async consumer (non-blocking)
- ✅ **Auto-Reconnect** - Frontend auto-reconnect với exponential backoff
- ✅ **Dynamic Discovery** - Cards auto-create từ sensor payloads
- ✅ **Modal History** - Click sensor card → modal chart (last 2 hours)
- ✅ **Databricks Integration** - Query analytics từ Delta Lake
- ✅ **Smart Filtering** - 90-95% storage savings
- ✅ **Production-Ready** - Error handling, logging, CORS

### **🎯 Use Cases**
| Use Case | Luồng | Latency | Dữ Liệu |
|----------|-------|---------|--------|
| **Real-time Monitoring** | WebSocket | ~100-350ms | All 5 sensors |
| **Dashboard Alerts** | WebSocket | ~200ms | Threshold breach |
| **Historical Analytics** | Databricks | ~seconds | Aggregated by minute |
| **Trend Analysis** | Databricks | ~minutes | Day/week/month stats |
| **Predictive ML** | Databricks | ~hours | Trained models |
| **Storage Optimization** | Smart Filter | - | 90% less data |

---

## 🛑 Stop & Cleanup

### **Dừng Services:**
```bash
# Mỗi terminal: Ctrl+C
```

### **Dừng Docker:**
```bash
docker-compose down
```

### **Xóa Volumes (tuỳ chọn):**
```bash
docker-compose down -v
```

---

## 🐛 Troubleshooting

| Vấn Đề | Nguyên Nhân | Cách Sửa |
|--------|-----------|---------|
| **"Connection refused" trên Frontend** | Backend chưa start | `cd backend && python main.py` |
| **Kafka không start** | Chờ lâu | `sleep 20` rồi `docker-compose logs kafka` |
| **MQTT không kết nối** | Mosquitto down | `docker-compose restart mosquitto` |
| **Port 8000 đã dùng** | Process cũ còn chạy | `lsof -i :8000` hoặc `Get-NetTCPConnection -LocalPort 8000` |
| **"WebSocket connection failed"** | CORS hoặc backend down | Kiểm tra http://localhost:8000/api/health |
| **Kafka producer timeout** | Network issue | Kiểm tra KAFKA_BOOTSTRAP_SERVERS |

---

## 📚 Next Steps

1. **Xem [QUICK_START.md](QUICK_START.md)** - Chi tiết từng bước setup
2. **Setup Confluent Cloud Kafka** - Lấy API keys & bootstrap server
3. **Setup Databricks workspace** - SQL Warehouse + Delta tables
4. **Explore Notebooks** - `databricks/smart_filtering_kafka_to_delta_fixed.ipynb`

---

## 🎓 Architecture Concepts

### **Smart Filtering (Databricks)**
```
Input: All Kafka messages
↓
Filter:
  1. Threshold check (±0.5°C for temp, ±2% for humidity, etc.)
  2. Max interval check (send if no data for 5 minutes)
  3. First record (always save first message)
↓
Output: Only important changes → Delta Lake
Result: 90-95% storage savings
```

### **Real-time Data Flow**
```
Sensor Value Changes
    ↓
MQTT Publish (every 5 sec)
    ↓
Mosquitto Routes
    ↓
Bridge Receives + Converts to JSON
    ↓
Kafka Produces
    ↓
FastAPI Consumes (async)
    ↓
Broadcasts to WebSocket clients
    ↓
React updates UI
Total Latency: ~100-350ms
```

---

## 📞 Support

- **Setup Issue?** → Xem [QUICK_START.md](QUICK_START.md)
- **Docker Issue?** → `docker-compose logs -f mosquitto`
- **Kafka Connection Issue?** → Kiểm tra Confluent Cloud API keys
- **Databricks Error?** → Kiểm tra credentials & warehouse running

---

## ✅ Success Checklist

- [ ] `docker-compose ps` shows all containers running
- [ ] `curl http://localhost:8000/api/health` returns `{"status": "ok", ...}`
- [ ] Dashboard loads at `http://localhost:5173`
- [ ] WebSocket shows "Connected" ✓
- [ ] Sensor cards update every 5 seconds
- [ ] No errors in browser console
- [ ] Backend, Bridge, Sensor logs show no errors

---

**🚀 Chúc mừng! Hệ thống IoT của bạn đã sẵn sàng để chạy!**

Latency: ~100-350ms | Storage Savings: 90-95% | Real-time Monitoring ✓ | Analytics ✓
