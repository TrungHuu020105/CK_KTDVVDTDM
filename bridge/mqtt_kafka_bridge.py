"""
MQTT to Kafka Bridge
Subscribe vào MQTT broker và produce dữ liệu tới Kafka topic
"""

import paho.mqtt.client as mqtt
from kafka import KafkaProducer
from dotenv import load_dotenv
import os
import json
import logging
from datetime import datetime
import time

# Load environment variables from bridge/.env (explicit path)
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Cấu hình
MQTT_BROKER = os.getenv("MQTT_BROKER", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "sensors/iot/data")
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "iot_user")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "iot_password")


# Confluent Cloud Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "pkc-921jm.us-east-2.aws.confluent.cloud:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iot-sensor-data")
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", "")
KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", "")

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
                request_timeout_ms=10000,
                security_protocol=KAFKA_SECURITY_PROTOCOL,
                sasl_mechanism=KAFKA_SASL_MECHANISM,
                sasl_plain_username=KAFKA_SASL_USERNAME,
                sasl_plain_password=KAFKA_SASL_PASSWORD
            )
            logger.info("✓ Kết nối tới Kafka (Confluent Cloud) thành công")
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
            send_future = kafka_producer.send(KAFKA_TOPIC, value=data)
            send_future.get(timeout=10)
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
if MQTT_USERNAME:
    mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.on_disconnect = on_disconnect

# Main
if __name__ == "__main__":
    logger.info("🚀 MQTT to Kafka Bridge bắt đầu...")
    logger.info("MQTT broker: %s:%s | topic: %s", MQTT_BROKER, MQTT_PORT, MQTT_TOPIC)
    logger.info("MQTT username: %s", MQTT_USERNAME)
    logger.info("Kafka broker: %s | topic: %s", KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
    
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
            kafka_producer.flush(timeout=5)
            kafka_producer.close()
    except Exception as e:
        logger.error(f"✗ Lỗi: {e}")
        if kafka_producer:
            kafka_producer.flush(timeout=5)
            kafka_producer.close()
