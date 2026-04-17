# ⚡ Quick Start - 5 Phút Setup

**Dự án IoT hoàn chỉnh từ Sensor → Dashboard → Analytics**

---

## 🎯 Chuẩn bị

Trước khi bắt đầu, kiểm tra:

```bash
# Check versions
docker-compose --version     # >= 1.29
python --version              # >= 3.8
node --version                # >= 16
npm --version                 # >= 7
```

---

## ⏱️ Step 1: Docker Setup (2 phút)

Khởi động **MQTT broker** và **local Kafka**:

```bash
# Từ root folder CK3_DTDM
docker-compose up -d

# Chờ Kafka khởi động
sleep 15

# Kiểm tra containers
docker-compose ps
```

**Output mong đợi:**
```
STATUS      PORTS
Up 2 min    0.0.0.0:1883->1883/tcp    mosquitto
Up 1 min    0.0.0.0:9092->9092/tcp    kafka
```

---

## 🐍 Step 2: Install Python Dependencies (1.5 phút)

Cài đặt packages cho 3 Python services:

```bash
# 1. Backend
cd backend
pip install -r requirements.txt
cd ..

# 2. Bridge  
cd bridge
pip install -r requirements.txt
cd ..

# 3. Sensor
cd sensor
pip install -r requirements.txt
cd ..
```

---

## 📦 Step 3: Install Frontend (1 phút)

Cài đặt Node packages:

```bash
cd frontend
npm install
cd ..
```

---

## 🚀 Step 4: Chạy Services (Mở 4 terminals)

### **Terminal 1: Backend (FastAPI)**
```bash
cd backend
python main.py
```

**Chờ đến khi thấy:**
```
INFO:     Application startup complete [uvicorn]
INFO:     Uvicorn running on http://0.0.0.0:8000
```

✅ Port 8000 sẵn sàng

---

### **Terminal 2: Bridge (MQTT → Kafka)**
```bash
cd bridge
python mqtt_kafka_bridge.py
```

**Chờ đến khi thấy:**
```
✓ Kết nối tới MQTT broker thành công
✓ Kết nối tới Kafka (Confluent Cloud) thành công
📨 Subscribe vào topic: sensors/iot/data
```

✅ Bridge chạy

---

### **Terminal 3: Sensor Simulator**
```bash
cd sensor
python sensor_simulator.py
```

**Chờ đến khi thấy:**
```
🚀 MQTT to Kafka Bridge bắt đầu...
[1] ✓ Gửi: Sensor: sensor_1 | temperature: 24.5 °C
[2] ✓ Gửi: Sensor: sensor_2 | humidity: 62.0 %
```

✅ Dữ liệu chảy vào hệ thống

---

### **Terminal 4: Frontend (React)**
```bash
cd frontend
npm run dev
```

**Chờ đến khi thấy:**
```
  ➜  Local:   http://localhost:5173/
  ➜  Press h to show help
```

✅ Frontend sẵn sàng

---

## 📊 Step 5: Xem Dashboard

Mở browser:
```
http://localhost:5173
```

**Bạn sẽ thấy:**
- 🟢 **WebSocket Status:** Connected
- 📈 **5 Sensor Cards** cập nhật real-time:
  - 🌡️ Temperature (Living Room)
  - 💧 Humidity (Living Room)
  - 🌱 Soil Moisture (Garden)
  - 💡 Light Intensity (Outdoor)
  - 🎯 Pressure (Outdoor)

---

## ✅ Kiểm tra Hệ thống

### Health Check Backend
```bash
curl http://localhost:8000/api/health
```

**Response:**
```json
{
  "status": "ok",
  "connected_clients": 1,
  "kafka_connected": true
}
```

### Xem MQTT Messages
```bash
docker-compose exec mosquitto mosquitto_sub -t "sensors/iot/data"
```

### Xem Kafka Topics
```bash
docker exec -it <kafka-container-name> kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

### Xem Kafka Messages
```bash
docker exec -it <kafka-container-name> kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic iot-sensor-data \
  --from-beginning
```

---

## 🔧 Troubleshooting

### ❌ "Connection refused" trên Frontend
```
→ Check: Backend chạy chưa? (http://localhost:8000)
→ Sửa: cd backend && python main.py
```

### ❌ Kafka không start
```
→ Chờ thêm 15-20 giây
→ docker-compose logs kafka
→ docker-compose restart kafka
```

### ❌ MQTT không kết nối
```
→ docker-compose logs mosquitto
→ docker-compose restart mosquitto
```

### ❌ Port đã dùng
```
# Tìm process đang dùng port
# Linux/Mac:
lsof -i :8000    # FastAPI
lsof -i :5173    # React
lsof -i :1883    # MQTT

# Windows PowerShell:
Get-NetTCPConnection -LocalPort 8000 | Select-Object State, OwningProcess
```

---

## 🛑 Dừng Tất Cả

### Từng Terminal
```bash
# Mỗi terminal: Ctrl+C
```

### Docker
```bash
docker-compose down
```

**Hoặc xoá toàn bộ volumes:**
```bash
docker-compose down -v
```

---

## 📈 Tiếp Theo: Databricks Analytics (Optional)

Nếu có Databricks workspace, chạy notebook để phân tích sâu:

```
databricks/smart_filtering_kafka_to_delta_fixed.ipynb
```

**Điều kiện:**
- Confluent Cloud Kafka API keys
- Databricks workspace + cluster
- UC volumes

---

## 📊 Dữ Liệu Mẫu

**Frontend sẽ hiển thị:**
```
🌡️  Temperature: 24.5°C   (sensor_1)
💧 Humidity:     62.0%    (sensor_2)
🌱 Soil Moisture: 55.0%   (sensor_3)
💡 Light:        500 lux  (sensor_4)
🎯 Pressure:    1013.2 hPa (sensor_5)

Cập nhật cứ 5 giây 🔄
```

---

## 🎉 Hoàn tất!

Dashboard của bạn đã sẵn sàng. Data chảy qua toàn bộ pipeline:
```
Sensor (Python) → MQTT → Bridge → Kafka → Backend → WebSocket → React Dashboard
```

**Latency:** ~100-350ms ⚡

---

**Cần giúp?** Check logs hoặc README.md chính!

---

**Xong! 🎉 Hệ thống đang chạy!**

Để hiểu thêm, xem [README.md](README.md)
