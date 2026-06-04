# CHỐT HƯỚNG ĐỒ ÁN  
# Kiến trúc dịch vụ và điện toán đám mây  
## Bản chỉnh đúng theo File 1 + File 2, có bổ sung Kafka nhưng không đi lệch trọng tâm

---

## 0. Kết luận chốt cuối cùng

Hướng tốt nhất cho đồ án là:

> **Giữ hệ thống IoT hiện tại làm nền, tái cấu trúc thành nền tảng giám sát nhiệt độ/độ ẩm theo kiến trúc dịch vụ, hỗ trợ cả cảm biến IoT vật lý ESP32 và cảm biến IoT ảo từ Meteostat. PostgreSQL dùng làm operational database cho realtime/alert/config, Databricks Lakehouse dùng làm nơi lưu trữ phân tích dài hạn, ETL, feature engineering, model training/evaluation/forecast. Kafka được bổ sung như tầng event streaming nâng cấp để tăng điểm kiến trúc, nhưng không làm lệch vai trò chính của Databricks và Meteostat.**

Nói ngắn gọn:

```text
File 1 cung cấp chi tiết triển khai:
- gộp temperature/humidity vào một sensor
- sensor_readings schema
- physical sensor + virtual Meteostat sensor
- API sensor-level
- frontend sensor card
- model leaderboard
- Databricks Bronze/Silver/Gold
- checklist minh chứng

File 2 chốt hướng kiến trúc sạch:
- bỏ server_backend
- bỏ server rental / VPS rental / Metrics Central
- bỏ hoặc làm rất mỏng model_backend
- Databricks là nơi train/evaluate/forecast
- backend chỉ query kết quả Databricks
- Meteostat nên chạy trong Databricks Notebook/Job
- PostgreSQL là operational DB
- Databricks Delta Lake là analytical/lakehouse DB

Kafka được thêm hợp lý:
- Kafka là event streaming layer nâng cấp
- Kafka không thay thế PostgreSQL
- Kafka không thay thế Databricks
- Kafka không làm mất vai trò Virtual IoT Sensor từ Meteostat
- MVP vẫn có thể dùng PostgreSQL -> Databricks Job theo đúng file 2
- bản nâng cao có thể cho Databricks đọc Kafka trực tiếp
```

---

## 1. Tên đề tài chốt

### Tên tiếng Việt

**Nền tảng giám sát IoT nhiệt độ/độ ẩm theo kiến trúc dịch vụ, kết hợp Kafka Event Streaming, Databricks Lakehouse và mô hình dự báo đa thuật toán**

### Tên tiếng Anh

**Temperature and Humidity IoT Monitoring Platform Using Service-Oriented Architecture, Kafka Event Streaming, Databricks Lakehouse, and Multi-Algorithm Forecasting Models**

### Tên ngắn khi trình bày

**IoT Monitoring Platform with Databricks Lakehouse and Kafka**

---

## 2. Bài toán được định nghĩa lại

Không trình bày đồ án là:

```text
Một app dashboard IoT đơn giản
```

Mà trình bày là:

> Hệ thống giám sát nhiệt độ và độ ẩm cho môi trường nông nghiệp/phòng thí nghiệm, hỗ trợ cả cảm biến IoT vật lý ESP32 và cảm biến IoT ảo từ Meteostat. Dữ liệu realtime từ cảm biến vật lý được xử lý bởi IoT backend, lưu vào PostgreSQL để phục vụ vận hành, đồng thời có thể phát sự kiện qua Kafka để mở rộng kiến trúc event-driven. Databricks Lakehouse chịu trách nhiệm ingest dữ liệu vật lý và dữ liệu Meteostat, xử lý Bronze/Silver/Gold, huấn luyện nhiều mô hình dự báo, đánh giá mô hình và lưu kết quả forecast/model comparison để frontend hiển thị.

Phạm vi chỉ tập trung vào:

```text
temperature
humidity
realtime monitoring
alert threshold
physical IoT sensor
virtual IoT sensor from Meteostat
Databricks Lakehouse pipeline
forecast
model comparison
frontend visualization
```

---

## 3. Hướng chính không được đi lệch

### Không nên làm đồ án bị loãng bởi:

```text
server_backend
server rental
server store
server subscription
VPS rental
private key flow
Metrics Central
routes_servers
frontend ServerStore
METRICS_CENTRAL_*
```

Các phần này nên bỏ khỏi báo cáo và demo.

Lý do:

> Đồ án tập trung vào kiến trúc dịch vụ cho IoT monitoring, cloud data lakehouse và ML pipeline trên Databricks. Các chức năng thuê VPS/server store nằm ngoài phạm vi bài toán và làm loãng đề tài.

### Không nên train model trong backend

Không nên để:

```text
model_backend train model chính
frontend train model
script local train model chính
```

Chốt đúng:

```text
Databricks Notebook/Job:
- training
- evaluation
- model comparison
- batch forecasting
- lưu forecast_results
- lưu model_evaluation_results

App backend:
- query kết quả từ Databricks
- expose API cho frontend
```

Nếu vẫn giữ `model_backend` vì code cũ, chỉ nên đổi vai trò thành:

```text
databricks_proxy
```

Nhưng kiến trúc sạch nhất là:

```text
Không dùng model_backend như service train chính.
```

---

## 4. Kiến trúc tổng thể chốt

## 4.1. Kiến trúc MVP đúng theo file 1 + file 2

Đây là bản chắc chắn nên làm được trước:

```text
Physical IoT Sensor ESP32
        |
        v
MQTT Broker
        |
        v
iot_backend
        |
        +--> PostgreSQL Operational Database
        |
        +--> WebSocket Realtime Dashboard
        |
        v
Frontend Realtime Monitoring


Databricks Job / Notebook
        |
        +--> Read physical IoT data from PostgreSQL
        |
        +--> Ingest Meteostat Virtual IoT Sensor
        |
        v
Bronze Delta Table
        |
        v
Silver Delta Table
        |
        v
Gold Feature Table
        |
        v
Model Training / Evaluation / Forecast
        |
        v
model_evaluation_results + forecast_results


app backend / API Gateway
        |
        +--> Auth / User / Device Config
        |
        +--> Threshold / Alert Config
        |
        +--> Query PostgreSQL operational data
        |
        +--> Query Databricks forecast/model result
        |
        v
Frontend Forecast Chart + Model Leaderboard
```

Đây là hướng phải hoàn thành trước vì:

```text
dễ demo
ít lỗi
đúng file 2
đủ Databricks
đủ physical IoT
đủ virtual Meteostat
đủ forecast/model comparison
```

---

## 4.2. Kiến trúc có Kafka nhưng không đi quá xa

Kafka nên được đưa vào như tầng event streaming nâng cấp:

```text
Physical IoT Sensor ESP32
        |
        v
MQTT Broker
        |
        v
iot_backend
        |
        +--> PostgreSQL Operational Database
        |
        +--> WebSocket Realtime Dashboard
        |
        +--> Kafka Topic: iot.sensor.readings.v1
                    |
                    +--> optional alert consumer
                    |
                    +--> optional analytics/export consumer
                    |
                    +--> optional Databricks streaming ingestion


Databricks Job / Notebook
        |
        +--> MVP: read physical IoT data from PostgreSQL
        |
        +--> Advanced: read physical IoT data from Kafka
        |
        +--> Ingest Meteostat Virtual IoT Sensor
        |
        v
Bronze / Silver / Gold / Model / Forecast
```

Cách hiểu đúng:

```text
Kafka có thể có trong kiến trúc để tăng điểm.
Nhưng Kafka không bắt buộc thay thế luồng PostgreSQL -> Databricks trong MVP.
Databricks đọc PostgreSQL là hướng an toàn theo file 2.
Databricks đọc Kafka trực tiếp là hướng nâng cao nếu đủ thời gian.
```

---

## 4.3. Sơ đồ kiến trúc nên đưa vào báo cáo

```text
[ESP32 Physical IoT Sensor]
        |
        | MQTT
        v
[MQTT Broker]
        |
        v
[iot_backend]
        |
        +---------------------> [PostgreSQL Operational DB]
        |                           |
        |                           +--> users / devices / thresholds / alerts
        |                           +--> latest readings
        |                           +--> short-term sensor_readings
        |
        +---------------------> [WebSocket Realtime]
        |                           |
        |                           v
        |                      [Frontend Realtime Dashboard]
        |
        +---------------------> [Kafka Topic - optional enhancement]
                                    |
                                    +--> event streaming / future consumers


[Databricks Lakehouse]
        |
        +--> Ingest Physical IoT data from PostgreSQL
        |
        +--> Ingest Virtual Meteostat Sensor data
        |
        v
[Bronze Delta Table]
        |
        v
[Silver Delta Table]
        |
        v
[Gold Feature Table]
        |
        v
[Model Training / Model Evaluation / Forecast]
        |
        v
[model_evaluation_results + forecast_results]
        |
        v
[app backend / API Gateway]
        |
        v
[Frontend Forecast Chart + Model Leaderboard]
```

---

## 5. Vai trò từng thành phần

## 5.1. Frontend

Frontend dùng để:

```text
hiển thị realtime sensor dashboard
hiển thị mỗi sensor một card
hiển thị temperature và humidity trong cùng card
hiển thị physical/virtual badge
hiển thị indoor/outdoor badge
hiển thị alert status
hiển thị history ngắn hạn
hiển thị forecast chart
hiển thị model comparison leaderboard
hiển thị best model
```

Frontend không gọi trực tiếp Databricks.

Sai:

```text
Frontend -> Databricks bằng token
```

Đúng:

```text
Frontend -> app backend -> Databricks
```

---

## 5.2. App backend / API Gateway

App backend là service chính cho frontend.

Vai trò:

```text
Auth / JWT
User management
Role admin/user
Device management
Sensor config
Threshold config
Alert config
Query PostgreSQL operational data
Query Databricks forecast/model result
Expose API cho frontend
Swagger/OpenAPI
```

App backend giữ Databricks token trong `.env`.

Không để token Databricks ở frontend.

Biến môi trường:

```env
DATABRICKS_SERVER_HOSTNAME=...
DATABRICKS_HTTP_PATH=...
DATABRICKS_TOKEN=...
DATABRICKS_CATALOG=iot_cloud
DATABRICKS_SCHEMA=sensor_analytics
DATABRICKS_FORECAST_TABLE=forecast_results
DATABRICKS_EVALUATION_TABLE=model_evaluation_results
```

---

## 5.3. iot_backend

iot_backend xử lý phần realtime vật lý.

Vai trò:

```text
MQTT ingestion
parse payload từ ESP32 hoặc fake sensor
normalize temperature/humidity
ghi PostgreSQL
check alert threshold
broadcast WebSocket
optional publish Kafka event
health check
logging
```

iot_backend không nên train model.

iot_backend không nên xử lý Meteostat chính nếu đi theo hướng file 2.

Meteostat nên chạy trong Databricks Notebook/Job.

---

## 5.4. PostgreSQL Operational Database

PostgreSQL dùng cho operational data.

Lưu:

```text
users
roles
devices
virtual sensor config
thresholds
alerts
latest readings
sensor_readings ngắn hạn
sync watermark nếu Databricks đọc PostgreSQL
```

PostgreSQL không phải data lake.

PostgreSQL không cần lưu toàn bộ lịch sử IoT vĩnh viễn.

Chốt retention:

```text
sensor_readings trong PostgreSQL giữ 15 ngày gần nhất
Databricks giữ lịch sử dài hạn
```

Không gọi là ghi đè.

Gọi đúng là:

```text
retention policy
```

Ví dụ:

```sql
DELETE FROM sensor_readings
WHERE event_ts < NOW() - INTERVAL '15 days'
AND databricks_synced = true;
```

Nếu chưa làm được `databricks_synced`, có thể tạm thời xóa theo ngày sau khi job Databricks chạy ổn.

---

## 5.5. Databricks Lakehouse

Databricks là trung tâm cloud/data/ML.

Vai trò:

```text
lưu dữ liệu lịch sử dài
ingest physical IoT data
ingest virtual Meteostat data
Bronze Delta Table
Silver Delta Table
Gold Feature Table
feature engineering
train nhiều model
evaluate model
batch forecast
lưu forecast_results
lưu model_evaluation_results
```

Databricks không chỉ là nơi phụ.

Đây là phần phải có thật và có ảnh minh chứng.

---

## 5.6. Kafka

Kafka được thêm để tăng điểm kiến trúc, nhưng phải đặt đúng vai trò.

Kafka dùng cho:

```text
event streaming
tách luồng xử lý
buffer dữ liệu IoT
mở rộng consumer trong tương lai
minh chứng event-driven architecture
```

Kafka không thay thế:

```text
PostgreSQL
Databricks
Meteostat
```

Kafka nên là:

```text
optional enhancement trong MVP
hoặc production-ready extension trong báo cáo
```

Hướng triển khai an toàn:

```text
iot_backend vẫn ghi PostgreSQL và bắn WebSocket
iot_backend đồng thời publish một bản event vào Kafka
Databricks MVP vẫn đọc PostgreSQL
Nếu đủ thời gian, Databricks đọc Kafka trực tiếp
```

---

## 6. Physical IoT Sensor và Virtual IoT Sensor

Đây là phần rất quan trọng, không được bỏ sót.

Hệ thống có 2 loại sensor:

```text
Physical IoT Sensor = ESP32 thật hoặc fake stream mô phỏng ESP32
Virtual IoT Sensor = Meteostat theo vị trí địa lý
```

Cả hai phải được chuẩn hóa về cùng schema `sensor_readings`.

---

## 6.1. Physical IoT Sensor

Nguồn:

```text
ESP32
DHT11/DHT22 hoặc sensor nhiệt độ/độ ẩm khác
MQTT payload
```

Schema:

```json
{
  "sensor_id": "esp32_devkit_v1",
  "timestamp": "2026-05-30T10:00:00+07:00",
  "temperature": 30.5,
  "humidity": 72.1,
  "source_type": "physical_iot",
  "provider": "esp32",
  "environment_type": "indoor"
}
```

Chạy realtime qua:

```text
ESP32 -> MQTT -> iot_backend -> PostgreSQL + WebSocket
```

Nếu có Kafka:

```text
ESP32 -> MQTT -> iot_backend -> PostgreSQL + WebSocket + Kafka
```

---

## 6.2. Virtual IoT Sensor từ Meteostat

Định nghĩa:

> Meteostat được xem như một cảm biến IoT ảo ngoài trời, cung cấp dữ liệu nhiệt độ và độ ẩm theo vị trí địa lý. Dữ liệu Meteostat giúp hệ thống có đủ dữ liệu lịch sử để xây dựng Databricks pipeline và huấn luyện mô hình dự báo.

Schema giống sensor thật:

```json
{
  "sensor_id": "virtual_meteostat_hcm",
  "timestamp": "2026-05-30T10:00:00+07:00",
  "temperature": 32.1,
  "humidity": 68.0,
  "source_type": "virtual_meteostat",
  "provider": "meteostat",
  "environment_type": "outdoor",
  "latitude": 10.762622,
  "longitude": 106.660172
}
```

Map dữ liệu:

```text
Meteostat temp -> temperature
Meteostat rhum -> humidity
```

Theo hướng file 2:

```text
Meteostat nên chạy trong Databricks Notebook/Job
Không nhất thiết để backend lấy Meteostat rồi đẩy sang Databricks
```

Luồng đúng:

```text
app backend tạo virtual sensor config
        |
        v
PostgreSQL lưu sensor_id, latitude, longitude, location
        |
        v
Databricks Notebook đọc config virtual sensor
        |
        v
Databricks gọi Meteostat
        |
        v
Ghi raw vào bronze_sensor_readings
        |
        v
Silver / Gold / Model Training
```

---

## 6.3. Chính sách Indoor/Outdoor

Chốt policy:

```text
Indoor physical sensor:
    dùng dữ liệu ESP32 thật
    không cần Meteostat

Outdoor physical sensor:
    dùng dữ liệu ESP32 thật
    có thể so sánh với Meteostat

Outdoor virtual sensor:
    dùng Meteostat như cảm biến IoT ảo

Indoor virtual sensor:
    không khuyến khích hoặc không cho tạo
```

Frontend phải hiển thị badge:

```text
Physical IoT
Virtual Meteostat
Indoor
Outdoor
```

---

## 7. Sensor schema chốt

Không được tách:

```text
sensor_id + temperature
sensor_id + humidity
```

Phải chuyển thành:

```text
sensor_id = một thiết bị duy nhất
một dòng reading = có cả temperature và humidity
```

Ví dụ đúng:

```json
{
  "sensor_id": "esp32_devkit_v1",
  "event_ts": "2026-05-30T10:00:00+07:00",
  "temperature": 30.5,
  "humidity": 72.1,
  "source_type": "physical_iot",
  "provider": "esp32",
  "environment_type": "indoor"
}
```

Lợi ích:

```text
đúng bản chất thiết bị IoT
frontend chỉ cần một card cho một sensor
forecast có thể dùng temperature và humidity cùng lúc
Databricks feature engineering dễ hơn
so sánh physical/virtual dễ hơn
```

---

## 8. PostgreSQL schema đề xuất

## 8.1. `iot_devices`

Dùng cho cả physical sensor và virtual sensor.

```text
id
user_id
sensor_id
name
source_type
provider
environment_type
location
latitude
longitude
timezone_name
is_active
alert_enabled
created_at
updated_at
```

Giá trị:

```text
source_type = physical_iot | virtual_meteostat
provider = esp32 | meteostat
environment_type = indoor | outdoor
```

Ví dụ physical:

```text
sensor_id = esp32_devkit_v1
name = ESP32 Lab Sensor
source_type = physical_iot
provider = esp32
environment_type = indoor
```

Ví dụ virtual:

```text
sensor_id = virtual_meteostat_hcm
name = Meteostat Ho Chi Minh
source_type = virtual_meteostat
provider = meteostat
environment_type = outdoor
latitude = 10.762622
longitude = 106.660172
```

---

## 8.2. `sensor_readings`

PostgreSQL giữ ngắn hạn.

```text
id
sensor_id
device_id
event_ts
temperature
humidity
temperature_unit
humidity_unit
source_type
provider
environment_type
location
latitude
longitude
databricks_synced
created_at
```

Nếu có Kafka, thêm:

```text
kafka_topic
kafka_partition
kafka_offset
```

Index:

```sql
CREATE INDEX idx_sensor_readings_sensor_ts
ON sensor_readings(sensor_id, event_ts DESC);
```

---

## 8.3. `sensor_latest_readings`

Dùng để lấy giá trị mới nhất nhanh.

```text
sensor_id
device_id
event_ts
temperature
humidity
alert_status
updated_at
```

iot_backend upsert bảng này khi có reading mới.

---

## 8.4. `sensor_thresholds`

Threshold riêng cho từng chỉ số.

```text
id
sensor_id
temperature_min_threshold
temperature_max_threshold
humidity_min_threshold
humidity_max_threshold
is_active
created_at
updated_at
```

---

## 8.5. `sensor_alerts`

```text
id
sensor_id
event_ts
metric_name
metric_value
threshold_min
threshold_max
severity
message
is_resolved
created_at
resolved_at
```

---

## 8.6. `databricks_sync_watermark`

Nếu Databricks đọc PostgreSQL định kỳ, nên có bảng này.

```text
id
source_name
last_synced_event_ts
last_run_at
status
message
created_at
updated_at
```

Ví dụ:

```text
source_name = physical_iot_postgres
last_synced_event_ts = 2026-05-30T10:00:00+07:00
status = success
```

---

## 9. Migration dữ liệu cũ

Nếu hệ thống cũ đang tách metric:

```text
sensor_id + metric_type = temperature
sensor_id + metric_type = humidity
```

Cần viết migration.

Việc migration cần làm:

```text
Đọc bảng metrics cũ
Gom các dòng cùng sensor_id theo bucket thời gian, ví dụ 1 phút
metric_type = temperature -> cột temperature
metric_type = humidity -> cột humidity
Insert vào sensor_readings mới
Gộp iot_devices trùng source
Giữ lại một device đại diện cho một sensor
Chuyển threshold cũ sang threshold mới nếu có
```

Giai đoạn đầu có thể giữ bảng `metrics` để không vỡ API cũ.

Nhưng báo cáo nên trình bày schema mới là `sensor_readings`.

---

## 10. MQTT payload mới

ESP32 hoặc fake stream nên gửi:

```json
{
  "sensor_id": "esp32_devkit_v1",
  "temperature": 30.5,
  "humidity": 72.1,
  "timestamp": "2026-05-30T10:00:00+07:00"
}
```

iot_backend bổ sung metadata:

```json
{
  "sensor_id": "esp32_devkit_v1",
  "event_ts": "2026-05-30T10:00:00+07:00",
  "temperature": 30.5,
  "humidity": 72.1,
  "source_type": "physical_iot",
  "provider": "esp32",
  "environment_type": "indoor",
  "ingested_at": "2026-05-30T10:00:02+07:00"
}
```

---

## 11. WebSocket message mới

Frontend nên nhận message sensor-level:

```json
{
  "type": "sensor_reading",
  "sensor_id": "esp32_devkit_v1",
  "temperature": 30.5,
  "humidity": 72.1,
  "timestamp": "2026-05-30T10:00:00+07:00",
  "source_type": "physical_iot",
  "provider": "esp32",
  "environment_type": "indoor",
  "alert_status": "normal"
}
```

---

## 12. Kafka thiết kế sao cho không lệch hướng

Kafka nên thêm theo 2 mức.

---

## 12.1. Mức 1: Kafka là event log nâng cấp

Đây là mức nên làm nếu muốn có Kafka nhưng không phức tạp quá.

Luồng:

```text
MQTT -> iot_backend
          |
          +--> PostgreSQL
          |
          +--> WebSocket
          |
          +--> Kafka topic iot.sensor.readings.v1
```

Vai trò:

```text
Kafka lưu bản sao event đã chuẩn hóa
Kafka chứng minh event-driven capability
Kafka có thể dùng cho consumer mở rộng
Databricks trong MVP vẫn đọc PostgreSQL theo hướng file 2
```

Ưu điểm:

```text
dễ làm
không phá luồng cũ
không cần Databricks kết nối Kafka ngay
vẫn có Kafka để ăn điểm kiến trúc
```

---

## 12.2. Mức 2: Kafka có consumer riêng

Nếu đủ thời gian, tách thành:

```text
MQTT -> iot_backend -> Kafka
                         |
                         +--> PostgreSQL Writer Consumer
                         |
                         +--> Realtime WebSocket Consumer
                         |
                         +--> Alert Consumer
```

Ưu điểm:

```text
kiến trúc đẹp hơn
tách service rõ hơn
đúng event-driven hơn
```

Nhược điểm:

```text
nhiều service hơn
dễ lỗi hơn
phải quản lý consumer group/offset
```

---

## 12.3. Mức 3: Databricks đọc Kafka trực tiếp

Đây là bản nâng cao, không bắt buộc.

```text
Kafka topic iot.sensor.readings.v1
        |
        v
Databricks Structured Streaming
        |
        v
bronze_sensor_readings
```

Chỉ làm nếu:

```text
Kafka public/accessible an toàn cho Databricks
cấu hình network ổn
có thời gian xử lý security/listener/checkpoint
```

Nếu không làm được, vẫn ổn vì file 2 khuyên Databricks đọc PostgreSQL.

---

## 12.4. Topic Kafka đề xuất

```text
iot.sensor.readings.v1
iot.sensor.alerts.v1
iot.sensor.dead_letter.v1
```

Topic chính:

```text
iot.sensor.readings.v1
```

Key:

```text
sensor_id
```

Event value:

```json
{
  "event_id": "uuid",
  "sensor_id": "esp32_devkit_v1",
  "event_ts": "2026-05-30T10:00:00+07:00",
  "temperature": 30.5,
  "humidity": 72.1,
  "source_type": "physical_iot",
  "provider": "esp32",
  "environment_type": "indoor",
  "schema_version": "v1"
}
```

---

## 13. Databricks Lakehouse thiết kế

## 13.1. Catalog/schema

Nếu dùng Unity Catalog:

```text
catalog = iot_cloud
schema = sensor_analytics
```

Tables:

```text
iot_cloud.sensor_analytics.bronze_sensor_readings
iot_cloud.sensor_analytics.silver_sensor_readings
iot_cloud.sensor_analytics.gold_sensor_features
iot_cloud.sensor_analytics.model_training_runs
iot_cloud.sensor_analytics.model_evaluation_results
iot_cloud.sensor_analytics.forecast_results
```

---

## 13.2. Bronze table

```text
bronze_sensor_readings
```

Vai trò:

```text
raw dữ liệu từ physical IoT
raw dữ liệu từ virtual Meteostat
giữ trace nguồn dữ liệu
chưa clean quá nhiều
```

Cột:

```text
event_id
sensor_id
event_ts
temperature
humidity
temperature_unit
humidity_unit
source_type
provider
environment_type
location
latitude
longitude
raw_payload
ingested_at
schema_version
```

Nếu ingest từ Kafka, thêm:

```text
kafka_topic
kafka_partition
kafka_offset
```

Nếu ingest từ PostgreSQL, thêm:

```text
postgres_reading_id
```

---

## 13.3. Silver table

```text
silver_sensor_readings
```

Vai trò:

```text
dữ liệu đã clean
chuẩn timestamp
chuẩn unit
bỏ duplicate
lọc dữ liệu lỗi
chuẩn hóa source_type/provider
```

Data quality rule:

```text
sensor_id không null
event_ts không null
temperature không null
humidity không null
humidity từ 0 đến 100
temperature trong khoảng hợp lý
không duplicate sensor_id + event_ts
```

---

## 13.4. Gold table

```text
gold_sensor_features
```

Feature nên có:

```text
sensor_id
event_ts
temperature
humidity
hour_of_day
day_of_week
month
lag_temperature
lag_humidity
rolling_mean_temperature
rolling_mean_humidity
rolling_min_temperature
rolling_max_temperature
rolling_min_humidity
rolling_max_humidity
source_type
provider
environment_type
```

Không cần thêm wind/pressure/rain nếu muốn sát thiết bị nông nghiệp thực tế.

---

## 13.5. Model evaluation table

```text
model_evaluation_results
```

Cột:

```text
run_id
sensor_id
target
model_name
mae
rmse
mape
training_time_seconds
is_best
created_at
```

---

## 13.6. Forecast table

```text
forecast_results
```

Cột:

```text
forecast_id
run_id
sensor_id
target
model_name
forecast_ts
forecast_value
created_at
```

---

## 14. Databricks Notebooks cần có

Theo hướng file 2, nên có 5 notebook chính:

```text
01_ingest_physical_iot_to_bronze
02_ingest_meteostat_virtual_sensor_to_bronze
03_clean_bronze_to_silver
04_build_gold_features
05_train_compare_forecast_models
```

Nếu muốn thêm điểm:

```text
06_batch_forecast_job
07_dashboard_analytics
```

---

## 14.1. `01_ingest_physical_iot_to_bronze`

Bản MVP:

```text
Đọc dữ liệu physical IoT từ PostgreSQL
Dựa vào databricks_sync_watermark
Ghi vào bronze_sensor_readings
Update watermark
```

Bản nâng cao:

```text
Đọc Kafka topic iot.sensor.readings.v1
Ghi vào bronze_sensor_readings
Dùng checkpoint
```

Nhưng bản chính theo file 2 vẫn là:

```text
Databricks Job read PostgreSQL + Meteostat -> Delta Lake
```

---

## 14.2. `02_ingest_meteostat_virtual_sensor_to_bronze`

Notebook rất quan trọng.

Nhiệm vụ:

```text
Đọc danh sách virtual sensors từ PostgreSQL hoặc config file
Lấy latitude/longitude
Gọi Meteostat
Map temp -> temperature
Map rhum -> humidity
Gắn source_type = virtual_meteostat
Gắn provider = meteostat
Gắn environment_type = outdoor
Ghi vào bronze_sensor_readings
```

Điểm cần nhấn mạnh trong báo cáo:

```text
Meteostat được xem như Virtual IoT Sensor.
Virtual sensor có schema giống physical sensor.
Databricks là nơi thống nhất physical IoT và virtual Meteostat data.
```

---

## 14.3. `03_clean_bronze_to_silver`

Nhiệm vụ:

```text
Đọc Bronze
Chuẩn timestamp
Bỏ duplicate
Chuẩn unit
Lọc giá trị lỗi
Ghi Silver
```

---

## 14.4. `04_build_gold_features`

Nhiệm vụ:

```text
Đọc Silver
Tạo feature thời gian
Tạo lag features
Tạo rolling features
Ghi Gold
```

---

## 14.5. `05_train_compare_forecast_models`

Nhiệm vụ:

```text
Đọc Gold
Train nhiều model
Evaluate MAE/RMSE/MAPE
Chọn best model
Ghi model_evaluation_results
Ghi forecast_results
```

---

## 15. Model training chốt

Target:

```text
temperature
humidity
```

Model:

```text
Naive Baseline
Linear Regression hoặc Ridge
Random Forest
XGBoost
LSTM/GRU nếu còn thời gian
```

Metric:

```text
MAE
RMSE
MAPE nếu cần
training_time_seconds
```

Chốt quan trọng:

```text
Best model của temperature có thể khác best model của humidity.
```

Ví dụ:

```text
XGBoost tốt nhất cho temperature
Random Forest tốt nhất cho humidity
```

---

## 16. API backend cần có

## 16.1. Sensor API

```text
GET  /api/sensors
GET  /api/sensors/{sensor_id}
GET  /api/sensors/{sensor_id}/latest
GET  /api/sensors/{sensor_id}/history
POST /api/sensors/readings
POST /api/sensors/readings/bulk
```

## 16.2. Threshold API

```text
GET /api/sensors/{sensor_id}/thresholds
PUT /api/sensors/{sensor_id}/thresholds
```

## 16.3. Virtual Meteostat API

Dù Meteostat chạy trong Databricks, backend vẫn cần API để tạo cấu hình virtual sensor.

```text
POST /api/sensors/virtual-meteostat
GET  /api/sensors/virtual-meteostat
POST /api/sensors/{sensor_id}/sync-meteostat
```

Giải thích:

```text
POST /api/sensors/virtual-meteostat:
    tạo virtual device config trong PostgreSQL

POST /api/sensors/{sensor_id}/sync-meteostat:
    có thể trigger Databricks Job hoặc đánh dấu yêu cầu sync
```

Nếu chưa trigger Databricks được qua API, có thể ghi trong báo cáo:

```text
Databricks Job chạy theo lịch để ingest Meteostat.
```

---

## 16.4. Forecast API

```text
GET /api/sensors/{sensor_id}/forecast
GET /api/sensors/{sensor_id}/forecast?target=temperature
GET /api/sensors/{sensor_id}/forecast?target=humidity
```

Backend query Databricks `forecast_results`.

---

## 16.5. Model leaderboard API

```text
GET /api/sensors/{sensor_id}/model-leaderboard
GET /api/sensors/{sensor_id}/model-leaderboard?target=temperature
GET /api/sensors/{sensor_id}/model-leaderboard?target=humidity
```

Backend query Databricks `model_evaluation_results`.

---

## 16.6. Analytics API

```text
GET /api/sensors/{sensor_id}/analytics
GET /api/sensors/{sensor_id}/analytics/daily
```

---

## 17. Response mẫu

## 17.1. Latest physical sensor

```json
{
  "sensor_id": "esp32_devkit_v1",
  "temperature": 30.5,
  "humidity": 72.1,
  "timestamp": "2026-05-30T10:00:00+07:00",
  "source_type": "physical_iot",
  "provider": "esp32",
  "environment_type": "indoor",
  "alert_status": "normal"
}
```

---

## 17.2. Latest virtual Meteostat sensor

```json
{
  "sensor_id": "virtual_meteostat_hcm",
  "temperature": 31.2,
  "humidity": 74.0,
  "timestamp": "2026-05-30T10:00:00+07:00",
  "source_type": "virtual_meteostat",
  "provider": "meteostat",
  "environment_type": "outdoor"
}
```

---

## 17.3. Model leaderboard

```json
{
  "sensor_id": "virtual_meteostat_hcm",
  "target": "temperature",
  "best_model": "xgboost",
  "models": [
    {
      "name": "linear_regression",
      "mae": 1.42,
      "rmse": 1.91,
      "training_time_seconds": 0.2,
      "is_best": false
    },
    {
      "name": "xgboost",
      "mae": 0.82,
      "rmse": 1.13,
      "training_time_seconds": 2.8,
      "is_best": true
    }
  ]
}
```

---

## 17.4. Forecast

```json
{
  "sensor_id": "esp32_devkit_v1",
  "target": "humidity",
  "model_name": "random_forest",
  "forecast": [
    {
      "forecast_ts": "2026-05-30T11:00:00+07:00",
      "forecast_value": 73.4
    },
    {
      "forecast_ts": "2026-05-30T12:00:00+07:00",
      "forecast_value": 72.8
    }
  ]
}
```

---

## 18. Frontend cần chỉnh

## 18.1. Dashboard

Mỗi sensor một card.

Card gồm:

```text
Tên sensor
Sensor ID
Physical/Virtual badge
Indoor/Outdoor badge
Temperature hiện tại
Humidity hiện tại
Thời gian cập nhật cuối
Alert status
Nút xem Forecast
Nút xem Model Comparison
```

Không tách:

```text
temperature card riêng
humidity card riêng
```

---

## 18.2. Sensor detail page

Nên có:

```text
Temperature history chart
Humidity history chart
Forecast temperature
Forecast humidity
Model leaderboard
Best model badge
Alert history
Threshold config
```

Nếu sensor là outdoor physical:

```text
có thể thêm so sánh IoT vs Meteostat
```

---

## 18.3. Model leaderboard

Bảng:

```text
Model | Target | MAE | RMSE | Training Time | Best
```

Ví dụ:

```text
XGBoost       | temperature | 0.82 | 1.13 | 2.8s | Best
Random Forest | humidity    | 3.10 | 4.20 | 1.9s | Best
```

---

## 19. Deployment đề xuất

## 19.1. Trên VPS/local

Có thể chạy:

```text
frontend
app backend
iot_backend
PostgreSQL
Mosquitto MQTT
Kafka nếu thêm
Nginx
```

## 19.2. Trên Databricks Cloud

Có:

```text
Databricks Workspace
Compute/Cluster
SQL Warehouse nếu backend query bằng Databricks SQL
Notebooks
Jobs
Delta Tables
```

## 19.3. Cấu hình VPS nếu có Kafka

Tối thiểu:

```text
2 vCPU
4GB RAM
30GB SSD
```

Khuyên dùng:

```text
2-4 vCPU
8GB RAM
50GB SSD
```

Nếu VPS yếu, Kafka có thể chạy local khi demo hoặc chỉ đưa vào phần mở rộng.

---

## 20. Bảo mật

Cần dọn:

```text
.env
Databricks token
Gmail app password
Telegram token
Gemini API key
PostgreSQL password
Kafka password nếu có
JWT secret
```

Không commit `.env`.

Chỉ commit `.env.example`.

Ví dụ:

```env
DATABRICKS_TOKEN=your-databricks-token
DATABRICKS_SERVER_HOSTNAME=your-hostname
DATABRICKS_HTTP_PATH=your-http-path
POSTGRES_PASSWORD=your-password
JWT_SECRET=your-jwt-secret
```

---

## 21. Checklist minh chứng

## 21.1. Frontend

```text
Dashboard sensor card
Temperature + humidity trong cùng card
Physical IoT badge
Virtual Meteostat badge
Indoor/Outdoor badge
Realtime chart
Forecast chart
Model leaderboard
Best model badge
Alert status
```

## 21.2. Backend

```text
Swagger/OpenAPI
/api/health
/api/sensors
/api/sensors/{sensor_id}/latest
/api/sensors/{sensor_id}/forecast
/api/sensors/{sensor_id}/model-leaderboard
```

## 21.3. PostgreSQL

```text
iot_devices
sensor_readings
sensor_latest_readings
sensor_thresholds
sensor_alerts
virtual Meteostat device config
```

## 21.4. Databricks

```text
Workspace
Compute/Cluster
Notebook 01 ingest physical IoT
Notebook 02 ingest Meteostat
Notebook 03 clean Bronze to Silver
Notebook 04 build Gold features
Notebook 05 train/compare/forecast
Job run success
bronze_sensor_readings
silver_sensor_readings
gold_sensor_features
model_evaluation_results
forecast_results
```

## 21.5. Kafka, nếu có

```text
Kafka container/service running
Topic iot.sensor.readings.v1
Log iot_backend publish Kafka
Consumer đọc được message
```

---

## 22. Lộ trình làm khuyến nghị

## Giai đoạn 1: Dọn phạm vi và schema

```text
1. Bỏ server_backend khỏi kiến trúc chính.
2. Bỏ server rental/VPS rental khỏi báo cáo.
3. Chốt sensor_readings.
4. Gộp temperature/humidity vào một sensor.
5. Chốt iot_devices dùng cho cả physical và virtual sensor.
6. Chốt source_type/provider/environment_type.
```

## Giai đoạn 2: Physical IoT realtime

```text
1. Sửa MQTT payload.
2. Sửa iot_backend parse payload có cả temperature/humidity.
3. Ghi PostgreSQL sensor_readings.
4. Upsert sensor_latest_readings.
5. Check threshold.
6. Broadcast WebSocket.
7. Sửa frontend gộp card sensor.
```

## Giai đoạn 3: Virtual Meteostat Sensor

```text
1. Tạo API tạo virtual Meteostat device.
2. Lưu latitude/longitude/location vào iot_devices.
3. Viết Databricks notebook ingest Meteostat.
4. Map temp/rhum sang temperature/humidity.
5. Ghi bronze_sensor_readings.
6. Hiển thị virtual sensor trên frontend như sensor thật.
```

## Giai đoạn 4: Databricks Lakehouse

```text
1. Tạo Workspace.
2. Tạo Cluster/Compute.
3. Tạo Bronze table.
4. Tạo Silver table.
5. Tạo Gold feature table.
6. Tạo model_evaluation_results.
7. Tạo forecast_results.
8. Tạo Databricks Job.
```

## Giai đoạn 5: Model training

```text
1. Train Naive Baseline.
2. Train Linear/Ridge.
3. Train Random Forest.
4. Train XGBoost.
5. So sánh MAE/RMSE.
6. Chọn best model.
7. Ghi forecast.
8. Frontend hiển thị leaderboard.
```

## Giai đoạn 6: Kafka nâng điểm

```text
1. Cài Kafka.
2. Tạo topic iot.sensor.readings.v1.
3. iot_backend publish event vào Kafka.
4. Log/ảnh minh chứng Kafka.
5. Nếu còn thời gian, tách PostgreSQL Writer Consumer.
6. Nếu còn thời gian hơn nữa, Databricks đọc Kafka trực tiếp.
```

## Giai đoạn 7: Báo cáo và minh chứng

```text
1. Vẽ sơ đồ kiến trúc.
2. Chụp frontend.
3. Chụp Swagger.
4. Chụp PostgreSQL.
5. Chụp Databricks notebooks/jobs/tables.
6. Chụp Kafka nếu có.
7. Viết đánh giá kết quả.
8. Viết hạn chế và hướng phát triển.
```

---

## 23. MVP đủ đẹp để nộp

Bắt buộc nên có:

```text
Một sensor card hiển thị cả temperature và humidity.
Có physical sensor demo realtime.
Có WebSocket realtime.
Có PostgreSQL lưu operational data.
Có virtual Meteostat sensor.
Có Databricks Bronze/Silver/Gold.
Có train ít nhất 3 model.
Có model comparison leaderboard.
Có forecast chart.
Có backend query Databricks result.
Có ảnh minh chứng Databricks.
```

Nếu thêm Kafka:

```text
Có Kafka topic.
Có log publish Kafka.
Có consumer đọc Kafka hoặc ít nhất chứng minh message vào topic.
```

---

## 24. Hướng nâng cao nếu đủ thời gian

```text
Kafka tách consumer riêng
Databricks Structured Streaming đọc Kafka
Dead-letter topic
Data quality dashboard
Alert topic
Batch forecast job riêng
Dashboard analytics notebook
Retention policy tự động cho PostgreSQL
Health check từng service
Docker Compose đầy đủ
```

---

## 25. Câu trình bày với giảng viên

### Câu dài

> Nhóm tái cấu trúc hệ thống từ một ứng dụng IoT dashboard thành kiến trúc dịch vụ kết hợp cloud lakehouse. Dữ liệu từ cảm biến vật lý ESP32 được gửi qua MQTT đến IoT backend, sau đó được chuẩn hóa thành sensor-level reading gồm cả nhiệt độ và độ ẩm. PostgreSQL được dùng làm operational database để phục vụ realtime dashboard, latest readings, threshold và alert. Databricks Lakehouse ingest dữ liệu vật lý từ PostgreSQL, đồng thời ingest dữ liệu từ Meteostat như một Virtual IoT Sensor ngoài trời. Tất cả dữ liệu được chuẩn hóa vào Bronze Delta Table, làm sạch sang Silver, tạo feature ở Gold, sau đó Databricks huấn luyện nhiều mô hình dự báo, đánh giá bằng MAE/RMSE và lưu kết quả forecast/model comparison để backend truy vấn và frontend hiển thị. Kafka được bổ sung như tầng event streaming để mở rộng kiến trúc event-driven, giúp hệ thống có thể tách luồng xử lý realtime, operational storage và analytics trong các phiên bản nâng cao.

### Câu ngắn

> PostgreSQL phục vụ vận hành realtime, Databricks phục vụ phân tích dữ liệu và ML pipeline, Meteostat đóng vai trò Virtual IoT Sensor, còn Kafka là tầng event streaming nâng cấp để tăng khả năng mở rộng của kiến trúc.

---

## 26. Chốt cuối cùng

Hướng đúng nhất, không đi quá xa:

```text
MVP chính:
ESP32 -> MQTT -> iot_backend -> PostgreSQL + WebSocket
Databricks Job -> đọc PostgreSQL + Meteostat -> Bronze/Silver/Gold -> ML/Forecast
app backend -> query Databricks result -> frontend

Kafka nâng cấp:
iot_backend -> publish Kafka event
Kafka dùng để chứng minh event-driven architecture
Databricks đọc Kafka trực tiếp chỉ là hướng nâng cao nếu đủ thời gian
```

Không được quên:

```text
Physical IoT Sensor = ESP32
Virtual IoT Sensor = Meteostat
Cả hai phải chung schema sensor_readings
Databricks là nơi thống nhất physical + virtual data
Model training nằm trên Databricks
Frontend hiển thị cả realtime + forecast + model comparison
```

Đây là bản cân bằng nhất giữa:

```text
tổng quát
chi tiết
đúng file 1
đúng file 2
có Kafka để nâng điểm
không làm lệch trọng tâm Databricks + Meteostat
```
