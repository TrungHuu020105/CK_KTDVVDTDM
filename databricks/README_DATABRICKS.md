# Databricks Lakehouse Pipeline For IoT MetricsPulse

Thu muc nay chua pipeline Databricks cho he thong ESP32 + Meteostat. Backend va frontend khong goi Databricks truc tiep de train; frontend chi goi backend API, backend doc Delta table/view qua Databricks SQL Warehouse.

Namespace mac dinh:

```text
dtdm.metrics_app_streaming
```

Pipeline chi tap trung vao 2 metric hien co cua ESP32:

```text
temperature
humidity
```

## Luong Chuan

Databricks nhan 2 nguon du lieu:

- ESP32/PostgreSQL: du lieu do that, phuc vu realtime, doi chieu va hieu chinh forecast.
- Meteostat: du lieu lich su hourly, la nguon chinh de build dataset train khi ESP32 con it du lieu.

Luong tong quat:

```text
PostgreSQL sensor_readings
  -> 03_sync_postgres_to_bronze.py
  -> bronze_esp32_raw

Meteostat hourly weather
  -> 04_sync_meteostat_to_bronze.py
  -> bronze_meteostat_hourly

bronze_esp32_raw + bronze_meteostat_hourly
  -> 05_silver_cleaning.py
  -> silver_esp32_cleaned
  -> silver_meteostat_cleaned
  -> silver_weather_joined
  -> gold_esp32_hourly

silver/gold cleaned data
  -> 06_gold_feature_engineering.py
  -> gold_training_dataset

gold_training_dataset
  -> 07_train_machine_learning.py
  -> 08_train_deep_learning_light.py
  -> gold_model_metrics

gold_model_metrics
  -> 09_select_best_models.py
  -> is_best=true cho model tot nhat

best model + gold_training_dataset
  -> 10_generate_7day_forecast.py
  -> gold_forecast_result
  -> device_calibration_profile

Backend API
  -> Databricks SQL Warehouse
  -> compatibility views
  -> Frontend dashboard
```

### Thu Tu Chay Lan Dau

Chay lan dau theo dung thu tu sau:

```text
01_create_catalog_schema_tables.sql
02_seed_dim_locations.py
03_sync_postgres_to_bronze.py
04_sync_meteostat_to_bronze.py
05_silver_cleaning.py
06_gold_feature_engineering.py
07_train_machine_learning.py
08_train_deep_learning_light.py
09_select_best_models.py
10_generate_7day_forecast.py
```

File phu:

- `01b_create_missing_silver_gold_tables.sql`: chi chay khi workspace da co Bronze/Dim nhung thieu Silver/Gold.
- `03b_kafka_to_bronze.py`: optional, chi dung khi muon ingest Kafka truc tiep vao Bronze. Hien tai PostgreSQL van la duong sync chinh.
- `10b_generate_7day_humidity_forecast.py`: optional, chi dung khi can rerun rieng forecast humidity.

### Lich Chay Dinh Ky

Sau khi pipeline on dinh, co the tach thanh workflow job:

| Nhom job | File | Lich goi y |
|---|---|---|
| Sync ESP32 | `03_sync_postgres_to_bronze.py` | Moi 5-15 phut hoac hourly |
| Sync Meteostat | `04_sync_meteostat_to_bronze.py` | Daily |
| Clean Silver | `05_silver_cleaning.py` | Hourly hoac daily |
| Build Gold Features | `06_gold_feature_engineering.py` | Daily hoac truoc training |
| Train ML | `07_train_machine_learning.py` | Weekly/monthly hoac khi can danh gia lai |
| Train DL | `08_train_deep_learning_light.py` | Weekly/monthly, sau ML |
| Select Best | `09_select_best_models.py` | Sau moi dot train |
| Forecast 7 ngay | `10_generate_7day_forecast.py` | Moi 6 gio hoac daily |

Dashboard/backend doc du lieu tu compatibility views:

```text
bronze_sensor_readings
forecast_results
model_evaluation_results
```

Canonical Delta tables phia sau:

```text
bronze_esp32_raw
gold_forecast_result
gold_model_metrics
```

## 1. Tao Bang

Chay:

```text
01_create_catalog_schema_tables.sql
```

Script tao cac bang/view chinh:

```text
dim_province
dim_location
dim_device
bronze_esp32_raw
bronze_meteostat_hourly
silver_esp32_cleaned
silver_meteostat_cleaned
silver_weather_joined
gold_esp32_hourly
gold_training_dataset
gold_model_metrics
gold_forecast_result
device_calibration_profile
model_leaderboard
bronze_sensor_readings
forecast_results
model_evaluation_results
```

## 2. Seed Location/Province

Chay:

```text
02_seed_dim_locations.py
```

Mac dinh dung bo 34 tinh/thanh hien hanh:

```text
LOCATION_SET=current_34
UPDATE_ACTIVE_PROVINCES=true
INCLUDE_INACTIVE_LOCATIONS=false
```

Neu can dung lai bo 63 tinh/thanh cu:

```text
LOCATION_SET=legacy_63
```

Kiem tra:

```sql
SELECT COUNT(*) AS location_count
FROM dtdm.metrics_app_streaming.dim_location;
```

## 3. Dong Bo ESP32

Nguon hien tai la PostgreSQL:

```text
03_sync_postgres_to_bronze.py
```

Bien moi truong goi y:

```text
DB_HOST=<postgres-host>
DB_PORT=5432
DB_DATABASE=rtmps_db
DB_USERNAME=rtmps_user
DB_PASSWORD=<use Databricks secret>
POSTGRES_TABLE=sensor_readings
POSTGRES_LOOKBACK_DAYS=30
```

Neu sau nay tich hop Kafka, dung:

```text
03b_kafka_to_bronze.py
```

Hien tai file Kafka chi la placeholder de giu dung numbering pipeline.

## 4. Dong Bo Meteostat

Chay:

```text
04_sync_meteostat_to_bronze.py
```

Script doc `dim_location`, lay hourly Meteostat, va ghi vao `bronze_meteostat_hourly`. Chi lay:

```text
temperature_c
relative_humidity
```

Config goi y:

```text
LOCATION_SET=current_34
METEOSTAT_YEARS_BACK=4
MAX_STATION_DISTANCE_KM=200
METEOSTAT_STATION_RADIUS_METERS=250000
METEOSTAT_MIN_COVERAGE_RATIO=0.6
OVERWRITE_EXISTING=true
```

Kiem tra:

```sql
SELECT COUNT(*) AS total_rows,
       COUNT(DISTINCT location_id) AS locations,
       MIN(event_ts) AS min_ts,
       MAX(event_ts) AS max_ts
FROM dtdm.metrics_app_streaming.bronze_meteostat_hourly;
```

## 5. Silver Cleaning

Chay:

```text
05_silver_cleaning.py
```

Buoc nay:

- Clean ESP32 temperature/humidity.
- Aggregate ESP32 ve hourly.
- Clean Meteostat temperature/humidity.
- Join ESP32 hourly voi Meteostat theo `location_id + hour_ts`.
- Ghi Silver va `gold_esp32_hourly`.

## 6. Gold Feature Engineering

Chay:

```text
06_gold_feature_engineering.py
```

Tao `gold_training_dataset` voi:

- Input: `temperature`, `humidity`, `meteostat_temperature`, `meteostat_humidity`.
- Time features: `hour`, `day_of_week`, `month`.
- Lag: 1, 6, 12, 24 hours.
- Rolling: 24 va 168 hours.
- Target forecast 7 ngay:
  - `temp_target_168`
  - `humidity_target_168`

Kiem tra:

```sql
SELECT COUNT(*) AS rows,
       COUNT(DISTINCT location_id) AS locations,
       MIN(hour_ts) AS min_hour,
       MAX(hour_ts) AS max_hour
FROM dtdm.metrics_app_streaming.gold_training_dataset;
```

## 7. Train May Hoc

Chay:

```text
07_train_machine_learning.py
```

File nay chi train nhom may hoc Spark MLlib va ghi:

```text
model_type=spark_mllib
```

Mac dinh dang toi uu toc do:

```text
TRAIN_PROFILE=quick
FINAL_REFIT=false
RANDOM_FOREST_TREES=30
RANDOM_FOREST_MAX_DEPTH=8
DECISION_TREE_MAX_DEPTH=8
ENABLE_GENERALIZED_LINEAR=false
ENABLE_GBT=false
ENABLE_FM_REGRESSOR=false
```

`TRAIN_PROFILE=quick` chi train:

- Spark Linear Regression
- Spark Decision Tree
- Spark Random Forest nho

`TRAIN_PROFILE=full` se train theo cac bien `ENABLE_*`.

`FINAL_REFIT=false` se log model da fit tren `train_df` sau khi danh gia holdout, khong fit lai tren `full_df`. Neu can model cuoi fit tren toan bo data, dat:

```text
FINAL_REFIT=true
```

Mo hinh:

- Spark Linear Regression
- Spark Generalized Linear Regression
- Spark Decision Tree
- Spark Random Forest
- Spark GBT
- Spark FM Regressor hoac ElasticNet fallback

MLflow Tracking luu:

- Run train tung model va tung target.
- Metrics: RMSE, MAE, R2, MSE, RSE.
- Params: feature columns, train/test rows, horizon, input window.
- Artifacts:
  - model artifact o path `model`
  - `summary/run_summary.json`

File nay chi xoa/cap nhat rows `spark_mllib`, khong xoa rows `deep_learning`.

### Location refit tu best global Spark model

Mac dinh `07_train_machine_learning.py` ho tro luong:

```text
TRAIN_SCOPE=global_then_location_best
```

Luong nay van train global truoc. Sau do voi moi target, script lay best global Spark model type vua danh gia, refit lai cung estimator/hyperparameters tren tung `location_id`, va ghi them rows:

```text
training_mode=per_location
model_scope=location_refit_best_global_type
location_id=<location>
```

Co the gioi han tai cho Databricks Job:

```text
LOCATION_ID=loc34_hanoi      # de trong neu train tat ca location du du lieu
LOCATION_MIN_ROWS=5000
LOCATION_MAX_ROWS=50000
LOCATION_TOP_N=0             # 0 = khong gioi han so location
```

Neu muon chi train global nhu cu:

```text
TRAIN_SCOPE=global
```

Neu temperature da train xong nhung humidity loi, co the rerun rieng humidity:

```text
TRAIN_TARGETS=humidity
```

Khi do script chi xoa/cap nhat rows `spark_mllib + humidity`, khong xoa temperature.

## 8. Train Hoc Sau

Chay:

```text
08_train_deep_learning_light.py
```

File nay chi train nhom hoc sau va ghi:

```text
model_type=deep_learning
```

Mo hinh:

- Keras LSTM
- Keras GRU
- Keras CNN-LSTM

Cau hinh day du mac dinh:

```text
DL_INPUT_WINDOW_HOURS=168
DL_MAX_ROWS_PER_SERIES=0
DL_MAX_SEQUENCES=120000
DL_MAX_DRIVER_ARRAY_MB=2048
DL_MIN_SEQUENCES=80
DL_EPOCHS=10
DL_BATCH_SIZE=128
DL_FINAL_REFIT_EPOCHS=0
```

File nay chi xoa/cap nhat rows `deep_learning`, khong xoa rows `spark_mllib`.

Mac dinh 08 train deep learning global, sau do refit deep learning theo location bang best global Keras model type:

```text
TRAIN_SCOPE=global_then_location_best
LOCATION_ENABLE_DEEP_LEARNING=true
LOCATION_ID=loc34_hanoi      # de trong neu train tat ca location du du lieu
LOCATION_MIN_ROWS=5000
LOCATION_MAX_ROWS=50000
LOCATION_TOP_N=0
LOCATION_DL_MAX_SEQUENCES=8000
LOCATION_DL_MIN_SEQUENCES=80
LOCATION_DL_EPOCHS=3
```

Luu y: per-location deep learning rat nang va co the chay lau. Neu muon tat refit location:

```text
LOCATION_ENABLE_DEEP_LEARNING=false
```

Neu muon test mot location truoc:

```text
LOCATION_ID=loc34_hanoi
```

Neu muon gioi han so location:

```text
LOCATION_TOP_N=3
```

Neu can rerun rieng humidity cho hoc sau:

```text
TRAIN_TARGETS=humidity
```

Khi do script chi xoa/cap nhat rows `deep_learning + humidity`, khong xoa temperature.

## 9. Chon Best Model

Chay sau khi da train may hoc va hoc sau:

```text
09_select_best_models.py
```

Buoc nay doc `gold_model_metrics` va chon:

```text
- best global model cho moi target
- best per-location model cho moi target + location_id neu co rows per_location
```

Tieu chi:

```text
RMSE tang dan
MAE tang dan
R2 giam dan
created_at moi nhat neu hoa
```

Script reset `is_best=false` cho cac model thuoc:

```text
spark_mllib
deep_learning
```

Sau do set `is_best=true` cho winner cua tung nhom:

```text
target_variable + training_mode + location_id
```

Kiem tra:

```sql
SELECT target_variable, training_mode, location_id, model_name, model_type, rmse, mae, r2, is_best
FROM dtdm.metrics_app_streaming.gold_model_metrics
WHERE is_best = true
ORDER BY target_variable, training_mode, location_id;
```

## 10. Generate Forecast 7 Ngay

Chay:

```text
10_generate_7day_forecast.py
```

Buoc nay:

- Doc best model tu `gold_model_metrics`.
- Uu tien model `training_mode=per_location` dung `location_id`.
- Neu location chua co model rieng, fallback ve best global model.
- Load model tu MLflow.
- Forecast 168 gio cho temperature va humidity.
- Tao forecast cho device/location.
- Ghi vao `gold_forecast_result`.
- Cap nhat `device_calibration_profile`.

Neu model artifact load loi, script fallback sang trend/naive de dashboard van co du lieu.

Kiem tra:

```sql
SELECT device_id,
       location_id,
       metric_type,
       COUNT(*) AS forecast_rows,
       MIN(forecast_timestamp) AS min_forecast_ts,
       MAX(forecast_timestamp) AS max_forecast_ts,
       MAX(model_name) AS model_name,
       MAX(model_type) AS model_type
FROM dtdm.metrics_app_streaming.gold_forecast_result
GROUP BY device_id, location_id, metric_type
ORDER BY location_id, device_id, metric_type;
```

Moi `device_id + location_id + metric_type` nen co:

```text
168 forecast rows
```

## Dashboard Contract

Frontend khong goi Databricks truc tiep.

Dung luong:

```text
Frontend -> Backend API -> Databricks SQL Warehouse -> Delta views/tables
```

Backend doc:

```text
DATABRICKS_BRONZE_TABLE=bronze_sensor_readings
DATABRICKS_FORECAST_TABLE=forecast_results
DATABRICKS_EVALUATION_TABLE=model_evaluation_results
```

Dashboard nen hien:

- Best Temperature Model.
- Best Humidity Model.
- Top Models: temperature.
- Top Models: humidity.
- Leaderboard chung, co `model_type` de phan biet `May hoc` va `Hoc sau`.
- Forecast chart/table tu `forecast_results`.
- System/Kafka status o cuoi dashboard.

## Compute Va Packages

Dung Databricks Runtime ML non-serverless cho training.

May hoc:

```python
%pip install "numpy<2" pandas "protobuf>=3.20.3,<5" meteostat==1.6.8
dbutils.library.restartPython()
```

Hoc sau nhe:

```python
%pip install "numpy<2" pandas "protobuf>=3.20.3,<5" meteostat==1.6.8 "tensorflow>=2.16,<2.18"
dbutils.library.restartPython()
```

Hoac cai theo file:

```python
%pip install -r databricks/requirements_training.txt
dbutils.library.restartPython()
```

## Backend Env

```text
DATABRICKS_ENABLED=true
DATABRICKS_SERVER_HOSTNAME=<sql-warehouse-host>
DATABRICKS_HTTP_PATH=<sql-warehouse-http-path>
DATABRICKS_TOKEN=<databricks-token>
DATABRICKS_CATALOG=dtdm
DATABRICKS_SCHEMA=metrics_app_streaming
DATABRICKS_BRONZE_TABLE=bronze_sensor_readings
DATABRICKS_FORECAST_TABLE=forecast_results
DATABRICKS_EVALUATION_TABLE=model_evaluation_results
```

Khong commit token vao source code.

## Workflow Goi Y

Xem lai muc `Luong Chuan` o dau file de chay theo 2 che do:

- Lan dau: chay tu `01` den `10` de tao bang, nap du lieu, train model va sinh forecast.
- Dinh ky: tach job ingest, cleaning, feature, training, select best va forecast theo lich rieng.

Neu can tao Databricks Workflow Jobs chi tiet, xem them `11_create_workflow_jobs.md`.

## Kiem Tra Nhanh

Leaderboard:

```sql
SELECT *
FROM dtdm.metrics_app_streaming.model_leaderboard
ORDER BY target_variable, is_best DESC, rmse ASC;
```

Model evaluation view cho backend:

```sql
SELECT model_name, model_type, target_variable, rmse, mae, r2, is_best
FROM dtdm.metrics_app_streaming.model_evaluation_results
ORDER BY target_variable, is_best DESC, rmse ASC;
```

Forecast view cho backend:

```sql
SELECT metric_type, COUNT(*) AS rows, MAX(generated_at) AS latest_generated_at
FROM dtdm.metrics_app_streaming.forecast_results
GROUP BY metric_type;
```

Bronze view cho backend:

```sql
SELECT COUNT(*) AS bronze_rows
FROM dtdm.metrics_app_streaming.bronze_sensor_readings;
```
