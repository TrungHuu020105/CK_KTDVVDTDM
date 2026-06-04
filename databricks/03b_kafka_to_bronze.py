# Databricks notebook source

"""Optional Kafka-to-Bronze ingestion placeholder.

The current pipeline syncs ESP32 data from PostgreSQL via:
  databricks/03_sync_postgres_to_bronze.py

When Kafka ingestion is enabled, implement this notebook to read Kafka events
and write the same canonical Bronze table schema used by the rest of the
pipeline:
  dtdm.metrics_app_streaming.bronze_esp32_raw

Keeping this file in the pipeline makes the future Kafka step explicit without
changing Silver, Gold, training, forecast, backend, or frontend contracts.
"""

raise NotImplementedError(
    "Kafka ingestion is not enabled yet. Use 03_sync_postgres_to_bronze.py for the current pipeline."
)
