# Databricks notebook source
# 02_clean_bronze_to_silver.py

from pyspark.sql import functions as F
from pyspark.sql.window import Window

catalog = "iot_cloud"
schema = "sensor_analytics"
bronze = f"{catalog}.{schema}.bronze_sensor_readings"
silver = f"{catalog}.{schema}.silver_sensor_readings"

raw = spark.table(bronze)
cleaned = (
    raw
    .where(F.col("sensor_id").isNotNull())
    .withColumn("event_ts", F.to_timestamp("event_ts"))
    .withColumn("temperature", F.when((F.col("temperature") >= -20) & (F.col("temperature") <= 60), F.col("temperature")))
    .withColumn("humidity", F.when((F.col("humidity") >= 0) & (F.col("humidity") <= 100), F.col("humidity")))
    .withColumn("source_type", F.coalesce(F.col("source_type"), F.lit("physical_iot")))
    .withColumn("provider", F.coalesce(F.col("provider"), F.lit("unknown")))
    .withColumn("environment_type", F.coalesce(F.col("environment_type"), F.lit("indoor")))
    .withColumn("processed_at", F.current_timestamp())
)

window = Window.partitionBy("sensor_id", "event_ts").orderBy(F.col("ingested_at").desc_nulls_last())
latest = cleaned.withColumn("rn", F.row_number().over(window)).where("rn = 1").drop("rn")

latest.select(
    "sensor_id", "event_ts", "temperature", "humidity", "source_type", "provider",
    "environment_type", "location_province", "latitude", "longitude", "processed_at"
).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver)
