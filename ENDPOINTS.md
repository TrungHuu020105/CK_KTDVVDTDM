# 📡 API & WebSocket Endpoints

## REST API Endpoints

### GET /api/health
Health check endpoint
```bash
curl http://localhost:8000/api/health
```

**Response:**
```json
{
  "status": "ok",
  "connected_clients": 5,
  "kafka_connected": true
}
```

### GET /api/status
Status information
```bash
curl http://localhost:8000/api/status
```

**Response:**
```json
{
  "app": "IoT Real-time Dashboard",
  "version": "1.0",
  "kafka_topic": "iot-sensor-data",
  "connected_clients": 5,
  "kafka_broker": "localhost:9092"
}
```

### GET /
API information
```bash
curl http://localhost:8000/
```

## WebSocket Endpoint

### ws://localhost:8000/ws

Connect to receive real-time sensor data

**JavaScript Example:**
```javascript
const ws = new WebSocket('ws://localhost:8000/ws');

ws.onopen = () => {
  console.log('Connected');
};

ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log('Sensor data:', data);
};

ws.onerror = (error) => {
  console.error('WebSocket error:', error);
};

ws.onclose = () => {
  console.log('Disconnected');
  // Auto-reconnect in 3 seconds
};
```

**Message Format:**
```json
{
  "timestamp": "2024-01-15T10:30:45.123456",
  "sensor_id": "SENSOR_001",
  "location": "Room_1",
  "temperature": 25.34,
  "humidity": 65.21,
  "pressure": 1005.32
}
```

## MQTT Topics

### sensors/iot/data
MQTT topic where sensor data is published

**Format:**
```json
{
  "timestamp": "ISO8601 format",
  "sensor_id": "Sensor identifier",
  "location": "Physical location",
  "temperature": "°C",
  "humidity": "%",
  "pressure": "hPa"
}
```

**Test Subscribe:**
```bash
docker-compose exec mosquitto mosquitto_sub -t "sensors/iot/data"
```

## Kafka Topics

### iot-sensor-data
Kafka topic receiving messages from bridge

**Commands:**
```bash
# List topics
docker-compose exec kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

# Consume messages
docker-compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic iot-sensor-data \
  --from-beginning
```

## Swagger UI

Interactive API documentation available at:
```
http://localhost:8000/docs
```

---

**For more details, see README.md**
