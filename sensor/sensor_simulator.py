"""
IoT Sensor Simulator

Mô phỏng dữ liệu thực tế bằng:
1) Random Walk có giới hạn để dữ liệu thay đổi mượt, không giật.
2) Time Trend theo chu kỳ ngày/đêm (nhiệt độ cao nhất ~14:00, thấp nhất ~02:00).

Xuất bản 5 loại chỉ số:
- temperature (°C)
- humidity (%)
- soil_moisture (%)
- light_intensity (lux)
- pressure (hPa)
"""

import json
import logging
import math
import os
import random
import time
from datetime import datetime
import pytz

import paho.mqtt.client as mqtt
from dotenv import load_dotenv
import os

# Load environment variables from sensor/.env (explicit path)
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# Cấu hình MQTT
MQTT_BROKER = os.getenv("MQTT_BROKER", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "sensors/iot/data")
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "iot_user")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "iot_password")
PUBLISH_INTERVAL_SECONDS = float(os.getenv("PUBLISH_INTERVAL_SECONDS", "5"))
# Nếu cần gửi trực tiếp Kafka, cấu hình endpoint mặc định:
# Confluent Cloud Kafka configuration (nếu cần gửi trực tiếp Kafka)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "pkc-xxxxxx.us-east-2.aws.confluent.cloud:9092")
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", "<Your_Confluent_Cloud_API_Key>")
KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", "<Your_Confluent_Cloud_API_Secret>")

# Cấu hình 5 sensor: mỗi sensor đo đúng 1 loại độ đo
SENSOR_PROFILES = [
    {
        "sensor_id": "sensor_1",
        "location": "Living_Room",
        "metric_key": "temperature",
        "unit": "°C",
        "minimum": 15.0,
        "maximum": 40.0,
        "start": 24.5,
        "max_step": 0.45,
        "pull_strength": 0.24,
        "decimals": 2,
        "bias": 0.0,
    },
    {
        "sensor_id": "sensor_2",
        "location": "Living_Room",
        "metric_key": "humidity",
        "unit": "%",
        "minimum": 20.0,
        "maximum": 95.0,
        "start": 62.0,
        "max_step": 1.1,
        "pull_strength": 0.18,
        "decimals": 2,
        "bias": 0.0,
    },
    {
        "sensor_id": "sensor_3",
        "location": "Garden",
        "metric_key": "soil_moisture",
        "unit": "%",
        "minimum": 8.0,
        "maximum": 95.0,
        "start": 55.0,
        "max_step": 0.55,
        "pull_strength": 0.50,
        "decimals": 2,
        "irrigation_chance": 0.03,
    },
    {
        "sensor_id": "sensor_4",
        "location": "Outdoor",
        "metric_key": "light_intensity",
        "unit": "lux",
        "minimum": 0.0,
        "maximum": 60000.0,
        "start": 10.0,
        "max_step": 80.0,
        "pull_strength": 0.30,
        "decimals": 0,
        "light_peak": 38000.0,
        "light_night": 3.0,
    },
    {
        "sensor_id": "sensor_5",
        "location": "Outdoor",
        "metric_key": "pressure",
        "unit": "hPa",
        "minimum": 990.0,
        "maximum": 1035.0,
        "start": 1012.0,
        "max_step": 0.25,
        "pull_strength": 0.16,
        "decimals": 2,
        "bias": 0.0,
    },
]


def clamp(value, minimum, maximum):
    return max(minimum, min(value, maximum))


def hour_fraction(now):
    return now.hour + now.minute / 60 + now.second / 3600


def bounded_random_walk(current, minimum, maximum, target, max_step, pull_strength):
    """
    Random Walk có giới hạn:
    - noise: dao động ngẫu nhiên nhỏ
    - drift: kéo nhẹ về target để đi theo xu hướng tự nhiên
    """
    noise = random.uniform(-max_step, max_step)
    drift = (target - current) * pull_strength
    return clamp(current + noise + drift, minimum, maximum)


def target_for_metric(profile, current, now):
    """Tính target theo chu kỳ thời gian và bối cảnh từng loại cảm biến."""
    metric_key = profile["metric_key"]
    hour = hour_fraction(now)

    # peak ~14:00, low ~02:00
    temp_cycle = math.sin((2 * math.pi * (hour - 8)) / 24)

    # daylight factor: 0 lúc đêm, gần 1 vào buổi trưa
    daylight_factor = max(0.0, math.sin(math.pi * (hour - 6) / 12))

    # áp suất có chu kỳ ngày nhẹ
    pressure_cycle = math.sin((2 * math.pi * (hour - 3)) / 24)

    if metric_key == "temperature":
        return 24.5 + profile.get("bias", 0.0) + 5.5 * temp_cycle

    if metric_key == "humidity":
        return 62.0 + profile.get("bias", 0.0) - 12.0 * temp_cycle

    if metric_key == "soil_moisture":
        # Độ ẩm đất giảm dần theo bốc hơi và tăng nhẹ khi có tưới/cơn mưa giả lập.
        evap = 0.05 + 0.10 * daylight_factor + max(temp_cycle, 0.0) * 0.08
        target = current - evap
        if random.random() < profile.get("irrigation_chance", 0.03):
            target += random.uniform(4.0, 9.0)
        return target

    if metric_key == "light_intensity":
        light_night = profile.get("light_night", 3.0)
        light_peak = profile.get("light_peak", 38000.0)
        return light_night + daylight_factor * (light_peak - light_night)

    if metric_key == "pressure":
        return 1012.0 + profile.get("bias", 0.0) + 1.2 * pressure_cycle

    return current


def init_sensor_state(profile):
    return profile["start"]


def update_sensor_value(current, profile, now):
    target = target_for_metric(profile, current, now)
    return bounded_random_walk(
        current=current,
        minimum=profile["minimum"],
        maximum=profile["maximum"],
        target=target,
        max_step=profile["max_step"],
        pull_strength=profile["pull_strength"],
    )


def generate_sensor_data(profile, value, now):
    decimals = profile["decimals"]
    measured_value = int(round(value)) if decimals == 0 else round(value, decimals)

    return {
        "timestamp": now.isoformat(),
        "sensor_id": profile["sensor_id"],
        "location": profile["location"],
        "metric_type": profile["metric_key"],
        "unit": profile["unit"],
        profile["metric_key"]: measured_value,
    }


# Callback khi kết nối thành công
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("✓ Kết nối tới MQTT broker thành công")
    else:
        logger.error(f"✗ Kết nối thất bại với code {rc}")


# Callback khi có lỗi
def on_disconnect(client, userdata, rc):
    if rc != 0:
        logger.warning(f"✗ Mất kết nối không mong muốn với code {rc}")


# Tạo client MQTT
client = mqtt.Client()
if MQTT_USERNAME:
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
client.on_connect = on_connect
client.on_disconnect = on_disconnect

# Kết nối tới MQTT broker
try:
    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    client.loop_start()
except Exception as e:
    logger.error(f"✗ Lỗi kết nối: {e}")
    raise SystemExit(1)


def main():
    sensor_states = {
        profile["sensor_id"]: init_sensor_state(profile) for profile in SENSOR_PROFILES
    }

    # Set timezone Việt Nam
    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')

    logger.info("🚀 Sensor Simulator bắt đầu...")
    logger.info("MQTT broker: %s:%s", MQTT_BROKER, MQTT_PORT)
    logger.info("MQTT username: %s", MQTT_USERNAME)
    logger.info("📤 Gửi dữ liệu tới topic: %s", MQTT_TOPIC)
    logger.info("🧪 Số sensor đang mô phỏng: %s", len(SENSOR_PROFILES))
    logger.info("📡 Mapping: sensor_1->temperature, sensor_2->humidity, sensor_3->soil_moisture, sensor_4->light_intensity, sensor_5->pressure")
    logger.info("🌍 Timezone: Asia/Ho_Chi_Minh (UTC+7)")

    counter = 0

    try:
        while True:
            # Lấy thời gian hiện tại ở múi giờ Việt Nam
            now = datetime.now(tz_vietnam).replace(tzinfo=None)

            for profile in SENSOR_PROFILES:
                sensor_id = profile["sensor_id"]
                next_value = update_sensor_value(sensor_states[sensor_id], profile, now)
                sensor_states[sensor_id] = next_value

                sensor_data = generate_sensor_data(profile, next_value, now)
                payload = json.dumps(sensor_data)

                result = client.publish(MQTT_TOPIC, payload, qos=1)
                counter += 1

                if result.rc == mqtt.MQTT_ERR_SUCCESS:
                    metric_key = profile["metric_key"]
                    logger.info(
                        "[%s] ✓ %s | %s: %s %s",
                        counter,
                        sensor_data["sensor_id"],
                        metric_key,
                        sensor_data[metric_key],
                        profile["unit"],
                    )
                else:
                    logger.error(
                        "✗ Gửi thất bại (%s) cho %s",
                        result.rc,
                        sensor_data["sensor_id"],
                    )

            time.sleep(PUBLISH_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("\n⏹️  Dừng Sensor Simulator")
    except Exception as e:
        logger.error(f"✗ Lỗi: {e}")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
