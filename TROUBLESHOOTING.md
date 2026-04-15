# 🔧 Troubleshooting

## Common Issues

### Docker Issues

**❌ "docker-compose: command not found"**
```bash
# Install Docker Desktop from https://www.docker.com/products/docker-desktop
docker-compose --version
```

**❌ "Kafka fails to start"**
```bash
# Wait 15-20 seconds, Kafka needs time to initialize
docker-compose logs kafka
docker-compose restart kafka
sleep 15
```

**❌ "Port already in use"**
```bash
# Linux/macOS
lsof -i :8000    # FastAPI
lsof -i :3000    # React
lsof -i :1883    # MQTT
lsof -i :9092    # Kafka

# Kill process
kill -9 <PID>
```

### Python Issues

**❌ "ModuleNotFoundError: No module named 'fastapi'"**
```bash
cd backend
pip install -r requirements.txt
```

**❌ "aiokafka.errors.KafkaConnectionError"**
```bash
# Kafka not running or not reachable
docker-compose ps kafka
docker-compose logs kafka
docker-compose restart kafka
sleep 15
```

### Frontend Issues

**❌ "npm: command not found"**
```bash
# Install Node.js from https://nodejs.org/
node --version
npm --version
```

**❌ "Port 3000 already in use"**
```bash
# Linux/macOS
lsof -i :3000
kill -9 <PID>

# Or use different port
PORT=3001 npm run dev
```

**❌ "WebSocket connection fails"**
```bash
# Check backend is running
curl http://localhost:8000/api/health

# Check browser console for errors
# Should connect to: ws://localhost:8000/ws
```

### Sensor/Bridge Issues

**❌ "Connection refused to MQTT broker"**
```bash
# Check Mosquitto is running
docker-compose ps mosquitto

# Check MQTT logs
docker-compose logs mosquitto

# Test MQTT
docker-compose exec mosquitto mosquitto_sub -t "sensors/iot/data"
```

## Quick Diagnostic

```bash
# Check everything
echo "=== Docker ===" && \
docker-compose ps && \
echo "" && \
echo "=== Backend Health ===" && \
curl -s http://localhost:8000/api/health | jq . && \
echo "" && \
echo "=== Kafka Topics ===" && \
docker-compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
```

## Reset Everything

```bash
# Nuclear option
docker-compose down -v
find . -type d -name __pycache__ -exec rm -rf {} +
find . -type f -name "*.pyc" -delete
cd frontend && rm -rf node_modules

# Start fresh
docker-compose up -d
sleep 15
```

---

**For more help, check README.md and QUICK_START.md**
