# ⚡ Quick Start - 5 Phút Setup

**Khởi chạy hệ thống IoT Real-time Dashboard từ đầu đến cuối chỉ trong 5 phút!**

---

## 📋 Kiểm Tra Yêu Cầu

Trước khi bắt đầu, đảm bảo bạn đã cài đặt:

```bash
# Check Docker
docker-compose --version     # >= 1.29
docker --version             # >= 20.0

# Check Python
python --version              # >= 3.8

# Check Node.js
node --version                # >= 16
npm --version                 # >= 7
```

**Nếu thiếu gì, hãy cài đặt:**
- **Docker:** https://docs.docker.com/get-docker/
- **Python:** https://www.python.org/downloads/
- **Node.js:** https://nodejs.org/

**Bạn cũng cần:**
- ✅ **Confluent Cloud Kafka API keys** (bootstrap server + credentials)
- ✅ **Databricks workspace** (optional - chỉ cho Analytics tab)

---

## ⏱️ Step 1: Khởi Động Docker - MQTT Broker (1 phút)

**Bước 1.1 - Từ root folder dự án, khởi động Mosquitto (MQTT Broker):**
```bash
cd d:\DuLieuCuaHuu\HK2_20252026\KTHDVVDTDM\CK\CK3_DTDM
docker-compose up -d
```

**Bước 1.2 - Chờ Mosquitto khởi động:**
```bash
# Windows PowerShell:
Start-Sleep -Seconds 5

# Linux/Mac:
sleep 5
```

**Bước 1.3 - Kiểm tra container đang chạy:**
```bash
docker-compose ps
```

**Output mong đợi:**
```
NAME              STATUS             PORTS
mosquitto-broker  Up 1 minute        0.0.0.0:1883->1883/tcp, 0.0.0.0:9001->9001/tcp
```

✅ **Mosquitto ready! (Kafka chạy trên Confluent Cloud)**

---

## 🐍 Step 2: Cài Python Dependencies (1.5 phút)

**Bước 2.1 - Cài packages cho 3 Python services:**

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

**Bước 2.2 - Chuẩn bị Confluent Cloud Kafka credentials**

Bạn sẽ cần setup credentials cho Bridge & Backend. Lấy từ Confluent Cloud:
- **Bootstrap Server** (e.g., `pkc-xxx.region.provider.confluent.cloud:9092`)
- **API Key** (username)
- **API Secret** (password)

Những file `.env` sẽ được cập nhật ở bước 4 trước khi chạy services.

**Nếu gặp lỗi permission:**
```bash
pip install -r requirements.txt --user
```

✅ **Python packages ready!**

---

## 📦 Step 3: Cài Frontend (1 phút)

**Cài Node.js packages:**
```bash
cd frontend
npm install
cd ..
```

**Nếu chậm, thử:**
```bash
npm install --prefer-offline
```

✅ **Frontend ready!**

---

## 🚀 Step 4: Chạy 4 Services (Mở 4 Terminals)

### **⚠️ QUAN TRỌNG:**
**Trước khi chạy, cập nhật `.env` files với Confluent Cloud credentials:**

**`bridge/.env`:**
```env
KAFKA_BOOTSTRAP_SERVERS=pkc-xxx.region.provider.confluent.cloud:9092
KAFKA_SASL_USERNAME=your-api-key
KAFKA_SASL_PASSWORD=your-api-secret
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
MQTT_BROKER=127.0.0.1
MQTT_PORT=1883
MQTT_TOPIC=sensors/iot/data
KAFKA_TOPIC=iot-sensor-data
```

**`backend/.env`:**
```env
KAFKA_BOOTSTRAP_SERVERS=pkc-xxx.region.provider.confluent.cloud:9092
KAFKA_SASL_USERNAME=your-api-key
KAFKA_SASL_PASSWORD=your-api-secret
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_TOPIC=iot-sensor-data
```

(Optional - chỉ nếu dùng Databricks Analytics)
```env
DATABRICKS_SERVER_HOSTNAME=adb-xxx.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/xxx
DATABRICKS_TOKEN=dapi...
```

---

### **Mở 4 terminals RIÊNG BIỆT và chạy lần lượt theo thứ tự dưới đây!**

---

### **Terminal 1️⃣ : Backend (FastAPI WebSocket Server)**

```bash
cd backend
python main.py
```

**Chờ đến khi thấy:**
```
INFO:     Uvicorn running on http://0.0.0.0:8000
INFO:     Application startup complete [uvicorn]
```

**✓ Backend Ready!** (Để terminal này chạy)

---

### **Terminal 2️⃣ : Bridge (MQTT → Kafka)**

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

**✓ Bridge Ready!** (Để terminal này chạy)

---

### **Terminal 3️⃣ : Sensor Simulator**

```bash
cd sensor
python sensor_simulator.py
```

**Chờ đến khi thấy:**
```
🚀 Sensor Simulator bắt đầu...
[1] ✓ sensor_1 | temperature: 24.5 °C
[2] ✓ sensor_2 | humidity: 62.0 %
[3] ✓ sensor_3 | soil_moisture: 55.0 %
[4] ✓ sensor_4 | light_intensity: 10.0 lux
[5] ✓ sensor_5 | pressure: 1012.0 hPa
```

**✓ Dữ liệu chảy vào hệ thống!** (Để terminal này chạy)

---

### **Terminal 4️⃣ : Frontend (React Vite)**

```bash
cd frontend
npm run dev
```

**Chờ đến khi thấy:**
```
  ➜  Local:   http://localhost:5173/
  ➜  press h to show help
```

**✓ Frontend Ready!**

---

## 📊 Step 5: Xem Dashboard

**Mở browser và vào:**
```
http://localhost:5173
```

### **Bạn sẽ thấy:**
```
┌─────────────────────────────────────────────────────┐
│  🌐 IoT Real-time Dashboard                         │
├─────────────────────────────────────────────────────┤
│  🟢 WebSocket Status: Connected                     │
├─────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌────────────┐ │
│  │ 🌡️  Temperature │  │ 💧 Humidity    │  │ 🌱 Soil   │ │
│  │ 24.5 °C      │  │ 62.0 %       │  │ 55.0 %    │ │
│  │ Living_Room  │  │ Living_Room  │  │ Garden    │ │
│  └──────────────┘  └──────────────┘  └────────────┘ │
│                                                      │
│  ┌──────────────┐  ┌──────────────┐               │ │
│  │ 💡 Light      │  │ 🎯 Pressure    │               │ │
│  │ 10 lux       │  │ 1012.0 hPa   │               │ │
│  │ Outdoor      │  │ Outdoor      │               │ │
│  └──────────────┘  └──────────────┘               │ │
│                                                      │
│  Cập nhật cứ 5 giây 🔄                             │
└─────────────────────────────────────────────────────┘
```

✅ **Hoàn tất! Hệ thống đang chạy!**

---

## ✅ Verify Mọi Thứ Đang Chạy

### **1. Health Check Backend:**
```bash
curl http://localhost:8000/api/health
```
**Response mong đợi:**
```json
{"status": "ok", "connected_clients": 1, "kafka_connected": true}
```

### **2. Xem MQTT Messages Thực Tế:**
```bash
docker-compose exec mosquitto mosquitto_sub -t "sensors/iot/data" -n 5
```
**Sẽ thấy 5 messages từ 5 sensors**

### **3. Check Terminal Logs:**
- **Terminal 1:** Backend logs (WebSocket connects/disconnects)
- **Terminal 2:** Bridge logs (MQTT → Kafka messages)
- **Terminal 3:** Sensor logs (publishing data every 5 seconds)
- **Terminal 4:** Frontend logs (Vite dev server)

---

## 🔄 Real-time Data Flow Visualization

```
Sensor Simulator (Terminal 3)
    ↓
[1] Temperature: 24.5°C  [2] Humidity: 62.0%  [3] Soil: 55%
    ↓
MQTT Topic: sensors/iot/data
    ↓
Bridge (Terminal 2)
    ↓
Kafka Topic: iot-sensor-data
    ↓
Backend Consumer (Terminal 1)
    ↓
WebSocket Broadcast
    ↓
Frontend (Terminal 4)
    ↓
React Dashboard (Browser)
    ↓
Device Cards Update Instantly ✨

Total Time: ~100-350ms ⚡
```

---

## 🎯 Test Features

### **Test 1: WebSocket Connection**
- Mở DevTools (F12) → Console
- Bạn sẽ thấy `WebSocket connected` message

### **Test 2: Real-time Updates**
- Nhìn vào sensor cards
- Mỗi 5 giây số liệu sẽ thay đổi
- Giả lập: temperature, humidity có xu hướng biến đổi theo chu kỳ ngày/đêm

### **Test 3: Analytics Tab**
- Click tab "📊 Analytics" (nếu đã setup Databricks)
- Chọn sensor + date range
- Nhấn "🔍 Tìm Dữ Liệu"
- Sẽ thấy line chart từ historical data

### **Test 4: Click on Device Card**
- Nhấn vào bất kỳ device card nào
- Modal pop-up hiển thị
- Thấy line chart của device đó (last 2 hours)

---

## ⏯️ Pause/Resume Services

### **Tạm Dừng Service (Nếu cần debug):**
```bash
# Dừng 1 terminal: Ctrl+C
# Lệnh sẽ gracefully shutdown service
```

### **Restart Service:**
```bash
# Chạy lại command từ terminal tương ứng
cd backend && python main.py
```

### **Dừng Toàn Bộ Docker:**
```bash
docker-compose down
```

### **Xoá Tất Cả Data (Optional):**
```bash
docker-compose down -v
```

---

## 🐛 Nhanh Fix Common Issues

| Vấn Đề | Cách Fix |
|--------|---------|
| **"Connection refused" khi vào http://localhost:5173** | Backend chưa start. Kiểm tra Terminal 1 |
| **WebSocket shows "Disconnected" (đỏ)** | Backend down hoặc Kafka down. Kiểm tra Terminal 1 & docker-compose ps |
| **Sensor Simulator không chạy** | Bridge chưa sẵn sàng. Kiểm tra Terminal 2 trước |
| **Kafka startup quá lâu** | Chờ thêm, có khi cần `docker-compose restart kafka` |
| **npm install bị lỗi** | Thử `npm cache clean --force` rồi `npm install` lại |
| **Port 8000 đã dùng** | Process cũ còn chạy. Kill: `Get-NetTCPConnection -LocalPort 8000 \| Stop-Process` (Windows) |

---

## 📞 Need Help?

1. **Setup Issues?** → Xem phần "Troubleshooting" ở README.md
2. **Kafka Connection Error?** → Kiểm tra Confluent Cloud credentials
3. **Mosquitto Error?** → `docker-compose logs -f mosquitto`
4. **Bridge/Backend Error?** → Check Terminal 2 & 1 logs

---

## 🎉 Success Indicators

Bạn sẽ biết setup thành công khi:

- ✅ Dashboard loads at `http://localhost:5173`
- ✅ WebSocket shows **🟢 Connected**
- ✅ Sensor cards display values
- ✅ Values update every 5 seconds
- ✅ No red errors in browser console
- ✅ No "connection refused" errors
- ✅ All 4 terminals show no errors
- ✅ `curl http://localhost:8000/api/health` returns JSON

---

## 🚀 Next Steps

1. ✅ **Exploration:** Click around dashboard, xem cards update
2. ⭕ **Optional - Databricks:** Setup Databricks credentials (xem `DATABRICKS_SETUP.md`)
3. ⭕ **Optional - Analytics:** Configure Databricks và test Analytics tab
4. ⭕ **Production:** Deploy to cloud (tuỳ chọn)

---

## 📊 Expected Data Patterns

**Temperature (sensor_1):**
- Morning (6h): ~18°C (low)
- Afternoon (14h): ~28°C (peak)
- Evening (18h): ~24°C
- Night (22h): ~20°C (low)

**Humidity (sensor_2):**
- Opposite of temperature
- Morning: High (~70%)
- Afternoon: Low (~55%)

**Soil Moisture (sensor_3):**
- Gradually decreases (evaporation)
- Occasionally spikes (watering simulation)

**Light (sensor_4):**
- Night (0-6h): ~3 lux
- Day (6h-18h): Up to 38000 lux (peak at 12h)
- Evening (18h-24h): ~3 lux

**Pressure (sensor_5):**
- Smooth 24-hour cycle
- Range: 990-1035 hPa

---

## 🎯 Performance Metrics

| Metric | Value |
|--------|-------|
| **WebSocket Latency** | ~100-350ms |
| **Kafka Processing** | Sub-second |
| **Data Point Update** | Every 5 seconds |
| **Concurrent Clients** | Limited by RAM |
| **Storage per 24h** | ~5-10 MB (raw), ~500KB (filtered) |

---

**🎉 Hoàn tất! Bạn giờ có hệ thống IoT real-time đầy đủ!**

Thưởng thức dashboard của bạn! 📊✨
