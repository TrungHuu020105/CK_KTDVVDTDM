# Databricks notebook source

"""Select best global and per-location models after ML and DL training.

Run after:
  1. 07_train_machine_learning.py
  2. 08_train_deep_learning_light.py

This notebook updates `gold_model_metrics.is_best` across both model families
and both supported training scopes:
  - spark_mllib
  - deep_learning
  - training_mode=global: one best model per target
  - training_mode=per_location: one best model per target/location

The frontend/backend read the same table through `model_evaluation_results`,
so no frontend change is needed after this step.
"""

import os
from pathlib import Path

from pyspark.sql import functions as F


DEFAULTS = {
    "DATABRICKS_CATALOG": "dtdm",
    "DATABRICKS_SCHEMA": "metrics_app_streaming",
    "BEST_MODEL_TYPES": "spark_mllib,deep_learning",
}


# COMMAND ----------

# Environment helpers

def load_local_env():
    candidates = []
    try:
        candidates.append(Path(__file__).resolve().parents[1] / ".env")
    except Exception:
        pass
    candidates.append(Path.cwd() / ".env")
    for env_path in candidates:
        if not env_path.exists():
            continue
        for line in env_path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))
        return


load_local_env()


def widget_value(name):
    try:
        value = dbutils.widgets.get(name).strip()  # type: ignore[name-defined]
        return value if value else None
    except Exception:
        return None


def setting(name):
    return os.getenv(name) or widget_value(name) or DEFAULTS.get(name, "")


def create_widgets():
    try:
        for name, default in DEFAULTS.items():
            try:
                dbutils.widgets.get(name)  # type: ignore[name-defined]
                continue
            except Exception:
                pass
            dbutils.widgets.text(name, os.getenv(name, default))  # type: ignore[name-defined]
    except Exception:
        pass


def fq_table(name):
    return setting("DATABRICKS_CATALOG") + "." + setting("DATABRICKS_SCHEMA") + "." + name


def table_exists(table_name):
    name = table_name.split(".")[-1]
    rows = spark.sql(  # type: ignore[name-defined]
        "SHOW TABLES IN "
        + setting("DATABRICKS_CATALOG")
        + "."
        + setting("DATABRICKS_SCHEMA")
        + " LIKE '"
        + name
        + "'"
    ).collect()
    return len(rows) > 0


def sql_list(values):
    cleaned = [value.strip() for value in values if value and value.strip()]
    if not cleaned:
        raise ValueError("BEST_MODEL_TYPES cannot be empty.")
    return ", ".join("'" + value.replace("'", "''") + "'" for value in cleaned)


# COMMAND ----------

# Best model selection

def main():
    create_widgets()
    metrics_table = fq_table("gold_model_metrics")
    if not table_exists(metrics_table):
        raise RuntimeError(metrics_table + " does not exist. Run 01_create_catalog_schema_tables.sql first.")

    model_types = [item.strip() for item in setting("BEST_MODEL_TYPES").split(",")]
    model_types_sql = sql_list(model_types)

    candidates = spark.table(metrics_table).where(  # type: ignore[name-defined]
        (F.col("model_type").isin([item for item in model_types if item]))
        & (F.coalesce(F.col("training_mode"), F.lit("global")).isin(["global", "per_location"]))
        & F.col("target_variable").isNotNull()
        & F.col("rmse").isNotNull()
        & F.col("mae").isNotNull()
        & F.col("r2").isNotNull()
    )
    if candidates.count() == 0:
        raise RuntimeError("No model candidates found in " + metrics_table)

    candidates.createOrReplaceTempView("__model_candidates")
    spark.sql(  # type: ignore[name-defined]
        """
        CREATE OR REPLACE TEMP VIEW __selected_best_models AS
        SELECT *
        FROM (
          SELECT
            *,
            ROW_NUMBER() OVER (
              PARTITION BY
                target_variable,
                COALESCE(training_mode, 'global'),
                COALESCE(location_id, 'global')
              ORDER BY rmse ASC NULLS LAST,
                       mae ASC NULLS LAST,
                       r2 DESC NULLS LAST,
                       created_at DESC NULLS LAST
            ) AS best_rank
          FROM __model_candidates
        )
        WHERE best_rank = 1
        """
    )

    spark.sql(  # type: ignore[name-defined]
        "UPDATE "
        + metrics_table
        + " SET is_best = false"
        + " WHERE model_type IN ("
        + model_types_sql
        + ") AND COALESCE(training_mode, 'global') IN ('global', 'per_location')"
    )

    spark.sql(  # type: ignore[name-defined]
        """
        MERGE INTO {metrics_table} AS target
        USING __selected_best_models AS best
        ON target.target_variable = best.target_variable
           AND target.model_name = best.model_name
           AND target.model_type = best.model_type
           AND COALESCE(target.training_mode, 'global') = COALESCE(best.training_mode, 'global')
           AND COALESCE(target.location_id, 'global') = COALESCE(best.location_id, 'global')
           AND COALESCE(target.model_uri, '') = COALESCE(best.model_uri, '')
        WHEN MATCHED THEN UPDATE SET target.is_best = true
        """.format(metrics_table=metrics_table)
    )

    print("Selected best global and per-location models across:", ", ".join(model_types))
    spark.table(metrics_table).where(  # type: ignore[name-defined]
        (F.col("model_type").isin([item for item in model_types if item]))
        & (F.coalesce(F.col("training_mode"), F.lit("global")).isin(["global", "per_location"]))
    ).orderBy(
        "target_variable",
        "training_mode",
        "location_id",
        F.col("is_best").desc(),
        F.col("rmse").asc(),
    ).select(
        "target_variable",
        "training_mode",
        "location_id",
        "model_name",
        "model_type",
        "rmse",
        "mae",
        "r2",
        "is_best",
        "model_uri",
    ).show(50, truncate=False)


# COMMAND ----------

main()
