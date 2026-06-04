# Databricks notebook source
# MAGIC %sql
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN dtdm;
# MAGIC
# MAGIC SELECT model_status, COUNT(*) AS rows
# MAGIC FROM dtdm.metrics_app_streaming.gold_forecast_result
# MAGIC GROUP BY model_status;
# MAGIC
# MAGIC SELECT
# MAGIC   device_id,
# MAGIC   location_id,
# MAGIC   metric_type,
# MAGIC   COUNT(*) AS rows
# MAGIC FROM dtdm.metrics_app_streaming.gold_forecast_result
# MAGIC GROUP BY device_id, location_id, metric_type
# MAGIC ORDER BY location_id, device_id, metric_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT target_variable, model_name, training_mode, location_id, COUNT(*) AS rows
# MAGIC FROM dtdm.metrics_app_streaming.gold_model_metrics
# MAGIC WHERE model_type = 'deep_learning'
# MAGIC GROUP BY target_variable, model_name, training_mode, location_id
# MAGIC ORDER BY training_mode, location_id, target_variable, model_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   metric_type,
# MAGIC   device_id,
# MAGIC   location_id,
# MAGIC   COUNT(*) AS forecast_rows,
# MAGIC   MIN(forecast_timestamp) AS start_time,
# MAGIC   MAX(forecast_timestamp) AS end_time,
# MAGIC   MAX(horizon_days) AS horizon_days
# MAGIC FROM dtdm.metrics_app_streaming.gold_forecast_result
# MAGIC GROUP BY metric_type, device_id, location_id
# MAGIC ORDER BY metric_type, location_id, device_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'bronze_esp32_raw' AS table_name, COUNT(*) AS rows FROM bronze_esp32_raw
# MAGIC UNION ALL
# MAGIC SELECT 'bronze_meteostat_hourly', COUNT(*) FROM bronze_meteostat_hourly
# MAGIC UNION ALL
# MAGIC SELECT 'silver_esp32_cleaned', COUNT(*) FROM silver_esp32_cleaned
# MAGIC UNION ALL
# MAGIC SELECT 'silver_meteostat_cleaned', COUNT(*) FROM silver_meteostat_cleaned
# MAGIC UNION ALL
# MAGIC SELECT 'gold_training_dataset', COUNT(*) FROM gold_training_dataset
# MAGIC UNION ALL
# MAGIC SELECT 'model_evaluation_results', COUNT(*) FROM model_evaluation_results
# MAGIC UNION ALL
# MAGIC SELECT 'forecast_results', COUNT(*) FROM forecast_results;

# COMMAND ----------

