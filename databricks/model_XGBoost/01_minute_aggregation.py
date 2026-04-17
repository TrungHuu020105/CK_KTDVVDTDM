# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 1 - Tạo bảng dữ liệu 1 phút
# MAGIC
# MAGIC Notebook này lấy dữ liệu từ:
# MAGIC - `iot_analytics.smart_filtered_measurements`
# MAGIC
# MAGIC và tạo bảng:
# MAGIC - `iot_analytics.sensor_minute_agg`
# MAGIC
# MAGIC Mỗi dòng đại diện cho 1 phút / 1 sensor / 1 metric.

# COMMAND ----------

from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")

DB_NAME = "iot_analytics"
SOURCE_TABLE = f"{DB_NAME}.smart_filtered_measurements"
TARGET_TABLE = f"{DB_NAME}.sensor_minute_agg"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")

print("Timezone:", spark.conf.get("spark.sql.session.timeZone"))
print("Source table:", SOURCE_TABLE)
print("Target table:", TARGET_TABLE)

# COMMAND ----------

spark.sql(f'''
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
  minute_ts TIMESTAMP,
  sensor_id STRING,
  location STRING,
  metric_type STRING,
  unit STRING,
  value_avg DOUBLE,
  value_min DOUBLE,
  value_max DOUBLE,
  value_std DOUBLE,
  obs_count BIGINT
) USING DELTA
PARTITIONED BY (metric_type)
''')

# COMMAND ----------

source_df = spark.table(SOURCE_TABLE).select(
    "event_ts", "sensor_id", "location", "metric_type", "metric_value", "unit"
)

display(source_df.orderBy(F.col("event_ts").desc()).limit(20))

# COMMAND ----------

minute_df = (
    source_df
    .withColumn("minute_ts", F.date_trunc("minute", F.col("event_ts")))
    .groupBy("minute_ts", "sensor_id", "location", "metric_type", "unit")
    .agg(
        F.avg("metric_value").alias("value_avg"),
        F.min("metric_value").alias("value_min"),
        F.max("metric_value").alias("value_max"),
        F.stddev_pop("metric_value").alias("value_std"),
        F.count("*").alias("obs_count")
    )
)

display(minute_df.orderBy(F.col("minute_ts").desc()).limit(20))

# COMMAND ----------

(minute_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print(f"Done writing to {TARGET_TABLE}")

# COMMAND ----------

display(
    spark.sql(f'''
        SELECT
            sensor_id,
            metric_type,
            COUNT(*) AS rows,
            MIN(minute_ts) AS min_ts,
            MAX(minute_ts) AS max_ts,
            ROUND(AVG(obs_count), 2) AS avg_obs_per_minute
        FROM {TARGET_TABLE}
        GROUP BY sensor_id, metric_type
        ORDER BY sensor_id, metric_type
    ''')
)