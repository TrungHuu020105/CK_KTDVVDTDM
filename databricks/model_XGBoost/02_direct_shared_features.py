# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 2 - Tạo feature train cho Direct + Shared Model
# MAGIC
# MAGIC Notebook này:
# MAGIC - đọc từ bảng `sensor_minute_agg`
# MAGIC - tạo lag / rolling / calendar features
# MAGIC - bung nhãn cho 60 horizon
# MAGIC - ghi ra bảng train
# MAGIC
# MAGIC Mô hình hướng tới:
# MAGIC - Direct forecasting
# MAGIC - Shared model theo từng metric_type

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
import math

spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")

DB_NAME = "iot_analytics"
SOURCE_TABLE = f"{DB_NAME}.sensor_minute_agg"
TARGET_TABLE = f"{DB_NAME}.sensor_forecast_train"

HORIZON_MAX = 60

print("Timezone:", spark.conf.get("spark.sql.session.timeZone"))
print("Source:", SOURCE_TABLE)
print("Target:", TARGET_TABLE)
print("Horizon max:", HORIZON_MAX)

# COMMAND ----------

base_df = (
    spark.table(SOURCE_TABLE)
    .select(
        "minute_ts",
        "sensor_id",
        "location",
        "metric_type",
        "unit",
        "value_avg",
        "value_min",
        "value_max",
        "value_std",
        "obs_count"
    )
)

display(base_df.orderBy(F.col("minute_ts").desc()).limit(20))

# COMMAND ----------

w = Window.partitionBy("sensor_id", "metric_type").orderBy("minute_ts")
w_3 = w.rowsBetween(-2, 0)
w_5 = w.rowsBetween(-4, 0)
w_10 = w.rowsBetween(-9, 0)
w_15 = w.rowsBetween(-14, 0)
w_30 = w.rowsBetween(-29, 0)
w_60 = w.rowsBetween(-59, 0)

feature_df = (
    base_df
    .withColumn("lag_1", F.lag("value_avg", 1).over(w))
    .withColumn("lag_2", F.lag("value_avg", 2).over(w))
    .withColumn("lag_3", F.lag("value_avg", 3).over(w))
    .withColumn("lag_5", F.lag("value_avg", 5).over(w))
    .withColumn("lag_10", F.lag("value_avg", 10).over(w))
    .withColumn("lag_15", F.lag("value_avg", 15).over(w))
    .withColumn("lag_30", F.lag("value_avg", 30).over(w))
    .withColumn("lag_60", F.lag("value_avg", 60).over(w))
    .withColumn("roll_mean_3", F.avg("value_avg").over(w_3))
    .withColumn("roll_mean_5", F.avg("value_avg").over(w_5))
    .withColumn("roll_mean_10", F.avg("value_avg").over(w_10))
    .withColumn("roll_mean_15", F.avg("value_avg").over(w_15))
    .withColumn("roll_mean_30", F.avg("value_avg").over(w_30))
    .withColumn("roll_mean_60", F.avg("value_avg").over(w_60))
    .withColumn("roll_std_5", F.stddev_pop("value_avg").over(w_5))
    .withColumn("roll_std_15", F.stddev_pop("value_avg").over(w_15))
    .withColumn("roll_std_30", F.stddev_pop("value_avg").over(w_30))
    .withColumn("roll_std_60", F.stddev_pop("value_avg").over(w_60))
    .withColumn("minute_of_hour", F.minute("minute_ts"))
    .withColumn("hour_of_day", F.hour("minute_ts"))
    .withColumn("day_of_week", F.dayofweek("minute_ts"))
    .withColumn("is_weekend", F.when(F.dayofweek("minute_ts").isin([1, 7]), 1).otherwise(0))
    .withColumn("sin_hour", F.sin(F.lit(2 * math.pi) * F.col("hour_of_day") / F.lit(24.0)))
    .withColumn("cos_hour", F.cos(F.lit(2 * math.pi) * F.col("hour_of_day") / F.lit(24.0)))
    .withColumn("sin_minute", F.sin(F.lit(2 * math.pi) * F.col("minute_of_hour") / F.lit(60.0)))
    .withColumn("cos_minute", F.cos(F.lit(2 * math.pi) * F.col("minute_of_hour") / F.lit(60.0)))
)

display(feature_df.orderBy(F.col("minute_ts").desc()).limit(20))

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

w_target = Window.partitionBy("sensor_id", "metric_type").orderBy("minute_ts")

target_wide_df = feature_df

# tạo 60 cột target_h1 ... target_h60
for h in range(1, HORIZON_MAX + 1):
    target_wide_df = target_wide_df.withColumn(f"target_h{h}", F.lead("value_avg", h).over(w_target))

# unpivot từ wide -> long để có horizon và target
stack_expr = ", ".join([f"{h}, target_h{h}" for h in range(1, HORIZON_MAX + 1)])

train_df = (
    target_wide_df
    .selectExpr(
        "minute_ts",
        "sensor_id",
        "location",
        "metric_type",
        "unit",
        "value_avg",
        "value_min",
        "value_max",
        "value_std",
        "obs_count",
        "lag_1",
        "lag_2",
        "lag_3",
        "lag_5",
        "lag_10",
        "lag_15",
        "lag_30",
        "lag_60",
        "roll_mean_3",
        "roll_mean_5",
        "roll_mean_10",
        "roll_mean_15",
        "roll_mean_30",
        "roll_mean_60",
        "roll_std_5",
        "roll_std_15",
        "roll_std_30",
        "roll_std_60",
        "minute_of_hour",
        "hour_of_day",
        "day_of_week",
        "is_weekend",
        "sin_hour",
        "cos_hour",
        "sin_minute",
        "cos_minute",
        f"stack({HORIZON_MAX}, {stack_expr}) as (horizon, target)"
    )
    .filter(F.col("target").isNotNull())
    .filter(F.col("lag_60").isNotNull())
)

display(train_df.orderBy(F.col("minute_ts").desc(), F.col("horizon").asc()).limit(50))

# COMMAND ----------

(train_df.write
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
            metric_type,
            COUNT(*) AS rows,
            MIN(minute_ts) AS min_ts,
            MAX(minute_ts) AS max_ts,
            MIN(horizon) AS min_horizon,
            MAX(horizon) AS max_horizon
        FROM {TARGET_TABLE}
        GROUP BY metric_type
        ORDER BY metric_type
    ''')
)