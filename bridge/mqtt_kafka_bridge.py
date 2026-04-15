"""
MQTT to Kafka Bridge
Subscribe vào MQTT broker và produce dữ liệu tới Kafka topic
"""

import paho.mqtt.client as mqtt
from kafka import KafkaProducer
import json
import logging
from datetime import datetime
import time

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Cấu hình
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "sensors/iot/data"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "iot-sensor-data"

METRIC_UNITS = {
    "temperature": "°C",
    "humidity": "%",
    "soil_moisture": "%",
    "light_intensity": "lux",
    "pressure": "hPa",
}

# Khởi tạo Kafka Producer
kafka_producer = None

def init_kafka_producer():
    """Khởi tạo Kafka Producer với retry logic"""
    global kafka_producer
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            kafka_producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3,
                request_timeout_ms=10000
            )
            logger.info("✓ Kết nối tới Kafka thành công")
            return True
        except Exception as e:
            retry_count += 1
            logger.warning(f"⚠️  Lỗi Kafka (lần {retry_count}/{max_retries}): {e}")
            if retry_count < max_retries:
                time.sleep(2)
    
    return False

# Callback MQTT
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("✓ Kết nối tới MQTT broker thành công")
        client.subscribe(MQTT_TOPIC, qos=1)
        logger.info(f"📨 Subscribe vào topic: {MQTT_TOPIC}")
    else:
        logger.error(f"✗ Kết nối MQTT thất bại với code {rc}")

def on_message(client, userdata, msg):
    """Xử lý message từ MQTT và gửi tới Kafka"""
    global kafka_producer
    
    try:
        payload = msg.payload.decode('utf-8')
        data = json.loads(payload)
        
        # Gửi tới Kafka
        if kafka_producer:
            kafka_producer.send(KAFKA_TOPIC, value=data)
            metric_key = data.get('metric_type')

            if not metric_key:
                for key in METRIC_UNITS:
                    if key in data:
                        metric_key = key
                        break

            if metric_key and metric_key in data:
                logger.info(
                    "✓ Bridge: MQTT -> Kafka | Sensor: %s | %s: %s %s",
                    data.get('sensor_id', 'N/A'),
                    metric_key,
                    data.get(metric_key, 'N/A'),
                    data.get('unit', METRIC_UNITS.get(metric_key, '')),
                )
            else:
                logger.info(
                    "✓ Bridge: MQTT -> Kafka | Sensor: %s | Payload: %s",
                    data.get('sensor_id', 'N/A'),
                    data,
                )
        else:
            logger.warning("⚠️  Kafka producer chưa sẵn sàng")
            
    except json.JSONDecodeError as e:
        logger.error(f"✗ Lỗi parse JSON: {e}")
    except Exception as e:
        logger.error(f"✗ Lỗi bridge: {e}")

def on_disconnect(client, userdata, rc):
    if rc != 0:
        logger.warning(f"⚠️  Mất kết nối MQTT: {rc}")

# Khởi tạo MQTT Client
mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.on_disconnect = on_disconnect

# Main
if __name__ == "__main__":
    logger.info("🚀 MQTT to Kafka Bridge bắt đầu...")
    
    # Khởi tạo Kafka Producer
    if not init_kafka_producer():
        logger.error("✗ Không thể kết nối tới Kafka")
        exit(1)
    
    # Kết nối MQTT
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        mqtt_client.loop_forever()
    except KeyboardInterrupt:
        logger.info("\n⏹️  Dừng Bridge")
        mqtt_client.loop_stop()
        if kafka_producer:
            kafka_producer.close()
    except Exception as e:
        logger.error(f"✗ Lỗi: {e}")
        if kafka_producer:
            kafka_producer.close()
