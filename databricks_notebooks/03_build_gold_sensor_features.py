# Databricks notebook source
# 03_build_gold_sensor_features.py

from pyspark.sql import functions as F
from pyspark.sql.window import Window

catalog = "iot_cloud"
schema = "sensor_analytics"
silver = f"{catalog}.{schema}.silver_sensor_readings"
gold = f"{catalog}.{schema}.gold_sensor_features"

base = spark.table(silver).where("temperature IS NOT NULL OR humidity IS NOT NULL")

long_df = base.selectExpr(
    "sensor_id", "event_ts", "source_type", "location_province", "temperature", "humidity",
    "stack(2, 'temperature', temperature, 'humidity', humidity) as (target, value)"
).where("value IS NOT NULL")

w = Window.partitionBy("sensor_id", "target").orderBy("event_ts")
features = (
    long_df
    .withColumn("hour_of_day", F.hour("event_ts"))
    .withColumn("day_of_week", F.dayofweek("event_ts"))
    .withColumn("month", F.month("event_ts"))
    .withColumn("lag_1", F.lag("value", 1).over(w))
    .withColumn("lag_3", F.lag("value", 3).over(w))
    .withColumn("lag_6", F.lag("value", 6).over(w))
    .withColumn("rolling_mean_6", F.avg("value").over(w.rowsBetween(-6, -1)))
    .withColumn("rolling_mean_24", F.avg("value").over(w.rowsBetween(-24, -1)))
)

features.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold)
