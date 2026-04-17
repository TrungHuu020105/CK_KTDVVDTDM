# 🚀 IoT Real-time Dashboard with Analytics

Hệ thống **IoT hoàn chỉnh** từ cảm biến mô phỏng đến dashboard real-time + phân tích sâu trên Databricks. Dữ liệu chảy qua 2 luồng chính:

## 📊 2 Luồng Dữ Liệu

### **Luồng 1: Real-time Dashboard** (WebSocket)
```
Sensor Simulator (Python)
    ↓ MQTT (1883)
Mosquitto Broker
    ↓
MQTT→Kafka Bridge
    ↓ Kafka (9092)
Confluent Cloud / Local Kafka
    ↓
FastAPI Backend (Aiokafka Consumer)
    ↓ WebSocket
React Dashboard (Real-time UI)
```
**Latency:** ~100-350ms ⚡

### **Luồng 2: Analytics** (Batch Processing)
```
Confluent Cloud Kafka
    ↓
Databricks Cluster (Spark Streaming)
    ↓ Smart Filtering (Threshold + Max Interval)
Delta Lake Tables
    ↓
Data Analysis & Reports
```
**Lợi ích:** 90-95% tiết kiệm storage, phân tích sâu

---

## 📁 Cấu trúc Dự Án

```
CK3_DTDM/
├── docker-compose.yml                    # MQTT + Local Kafka
├── README.md                             # Documentation này
├── QUICK_START.md                        # Setup nhanh 5 phút
│
├── sensor/                               # Sensor Simulator
│   ├── sensor_simulator.py               # 5 sensors: temp, humidity, soil_moisture, light, pressure
│   └── requirements.txt
│
├── mosquitto/                            # MQTT Message Broker
│   └── config/mosquitto.conf             # Cấu hình
│
├── bridge/                               # MQTT → Kafka Bridge
│   ├── mqtt_kafka_bridge.py              # Chuyển đổi MQTT → Kafka JSON
│   └── requirements.txt
│
├── backend/                              # FastAPI Backend
│   ├── main.py                           # WebSocket Server + Kafka Consumer
│   └── requirements.txt
│
├── frontend/                             # React + Vite Dashboard
│   ├── package.json
│   ├── vite.config.js
│   ├── index.html
│   └── src/
│       ├── main.jsx
│       ├── App.jsx
│       ├── App.css
│       └── components/Dashboard.jsx
│
└── databricks/                           # Analytics (Databricks)
    └── smart_filtering_kafka_to_delta_fixed.ipynb
        ├─ Read from Kafka
        ├─ Smart Filtering (threshold + max interval)
        └─ Write to Delta Lake
```

---

## 📋 Yêu cầu

### **Local Machine**
- **Docker & Docker Compose** (MQTT, Local Kafka)
- **Python 3.8+** (Sensor, Bridge, Backend)
- **Node.js 16+** & npm (Frontend)

### **Cloud (Optional)**
- **Confluent Cloud** account (Cloud Kafka)
- **Databricks workspace** (Analytics)

---

## 🔌 Dữ Liệu Cảm Biến

**5 Sensors** (mỗi sensor đo 1 metric):

| Sensor | Loại | Location | Phạm vi | Ngưỡng Databricks |
|--------|------|----------|---------|-------------------|
| sensor_1 | Temperature | Living_Room | 15-40°C | ±0.50°C |
| sensor_2 | Humidity | Living_Room | 20-95% | ±2.00% |
| sensor_3 | Soil Moisture | Garden | 8-95% | ±3.00% |
| sensor_4 | Light Intensity | Outdoor | 0-60K lux | ±100 lux |
| sensor_5 | Pressure | Outdoor | 990-1035 hPa | ±1.00 hPa |

**Format JSON (Kafka/MQTT):**
```json
{
  "timestamp": "2024-01-15T10:30:45.123456",
  "sensor_id": "sensor_1",
  "location": "Living_Room",
  "temperature": 24.5,
  "humidity": 65.2,
  "soil_moisture": 55.0,
  "light_intensity": 500.0,
  "pressure": 1013.25
}
```

---

## 🌐 Ports & Services

| Service | Port | Chứng thực | Mục đích |
|---------|------|-----------|---------|
| Mosquitto MQTT | 1883 | Optional | Message Broker |
| Mosquitto WebSocket | 9001 | No | MQTT over WS |
| Kafka | 9092 | SASL_SSL | Message Queue |
| FastAPI | 8000 | No | REST API + WebSocket |
| React Frontend | 3000 | No | Dashboard UI |

---

## 🛠️ Tech Stack

| Layer | Technology | Version |
|-------|-----------|---------|
| **Broker** | Eclipse Mosquitto | Latest |
| **Queue** | Apache Kafka / Confluent Cloud | 7.5.0 |
| **Backend** | FastAPI + Aiokafka | 0.104.1 |
| **Frontend** | React + Vite | 18.2.0 |
| **Analytics** | Databricks + Spark | Latest |
| **Storage** | Delta Lake | Latest |
| **Container** | Docker + Compose | Latest |

---

## 🚀 Quick Start

Xem **[QUICK_START.md](./QUICK_START.md)** để setup trong 5 phút!

---

## 📡 API Endpoints

### REST API
```bash
GET  /api/health       # Health check
GET  /api/status       # System status
```

### WebSocket
```
ws://localhost:8000/ws  # Real-time data stream
```

### Response Format
```json
{
  "timestamp": "ISO8601",
  "sensor_id": "sensor_1",
  "location": "Living_Room",
  "temperature": 24.5,
  "humidity": 65.2,
  "soil_moisture": 55.0,
  "light_intensity": 500.0,
  "pressure": 1013.25
}
```

---

## 🔍 Monitoring & Troubleshooting

### Health Check
```bash
curl http://localhost:8000/api/health
```

### Test MQTT
```bash
docker-compose exec mosquitto mosquitto_sub -t "sensors/iot/data"
```

### Test Kafka
```bash
docker exec <kafka-container> kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic iot-sensor-data
```

### Check Logs
```bash
docker-compose logs -f mosquitto
docker-compose logs -f kafka
```

---

## 📚 Thêm Thông Tin

- **Backend đọc Kafka:** `aiokafka.AIOKafkaConsumer` (async)
- **Frontend WebSocket:** `useEffect` hook + auto-reconnect
- **Smart Filtering:** Threshold-based + max-interval rule
- **Delta Lake:** MERGE operation cho state tracking

---

## 🎯 Use Cases

✅ **Real-time Monitoring** - Dashboard cập nhật tức thì
✅ **Data Analytics** - Phân tích trends trên Databricks
✅ **Storage Optimization** - Smart filtering giảm 90% dung lượng
✅ **Alerting** - Có thể thêm alert khi vượt ngưỡng
✅ **Predictive Maintenance** - ML trên Delta tables

---

**Phát triển bởi:** IoT Team | **Ngày:** 2024
- **Build Tool:** Vite (lightning-fast build)

---

## 📚 Tài Liệu

- [QUICK_START.md](QUICK_START.md) - Setup nhanh 5 phút
- [ARCHITECTURE.md](ARCHITECTURE.md) - Thiết kế hệ thống chi tiết
- [ENDPOINTS.md](ENDPOINTS.md) - API & WebSocket documentation
- [TROUBLESHOOTING.md](TROUBLESHOOTING.md) - Giải quyết vấn đề phổ biến

---

## 🔴 Dừng Hệ thống

```bash
# 1. Ctrl+C tất cả terminals
# 2. Dừng Docker
docker-compose down

# 3. Xóa volumes (tuỳ chọn)
docker-compose down -v
```

---

## ✨ Tính Năng

✅ Real-time data streaming
✅ Low-latency WebSocket communication
✅ Async Kafka processing
✅ Auto-reconnect functionality
✅ Modern React UI with Vite
✅ Fast build & hot module reload
✅ Docker containerization
✅ Production-ready code

---

## 📞 Gặp Vấn đề?

1. Xem [TROUBLESHOOTING.md](TROUBLESHOOTING.md)
2. Kiểm tra logs: `docker-compose logs`
3. Đọc [QUICK_START.md](QUICK_START.md)

---

**Chúc mừng! Hệ thống của bạn đã sẵn sàng.** 🚀
