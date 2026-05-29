# Schema Standard - Databricks Edition

## Operational PostgreSQL

### iot_devices

Mot dong dai dien mot sensor vat ly hoac sensor ao.

- `source`: sensor_id duy nhat o tang nghiep vu.
- `source_type`: `physical_iot` hoac `virtual_meteostat`.
- `capabilities`: mac dinh `temperature,humidity`.
- `environment_type`: `indoor` hoac `outdoor`.
- `location_province`: tinh/thanh phuc vu Meteostat va bao cao.

### sensor_readings

Bang canonical moi cho du lieu cam bien.

- `sensor_id`
- `event_ts`
- `temperature`
- `humidity`
- `source_type`
- `provider`
- `environment_type`
- `location_province`
- `databricks_status`

Bang `metrics` chi con la compatibility layer cho cac API cu.

## Databricks Delta Tables

- Bronze: raw sensor-level readings.
- Silver: cleaned readings.
- Gold: feature table cho training.
- Evaluation: ket qua so sanh model.
- Forecast: ket qua du bao batch cho frontend.
