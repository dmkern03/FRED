# Databricks notebook source
# MAGIC %md
# MAGIC # FRED Gold Layer Setup - Delta Live Tables
# MAGIC
# MAGIC This notebook defines the Gold layer Materialized View for the FRED data pipeline using Delta Live Tables (DLT).
# MAGIC
# MAGIC **Important:** This notebook should be run as part of a Delta Live Tables pipeline, not as a standalone notebook.
# MAGIC
# MAGIC ## What This Creates:
# MAGIC - **gold_observations** - Materialized View that denormalizes silver_observations and silver_metadata
# MAGIC
# MAGIC ## Features:
# MAGIC - Automatic refresh when source Silver tables update
# MAGIC - Optimized for analytical queries with CLUSTER BY
# MAGIC - Business-ready denormalized schema
# MAGIC - Query performance optimization with Delta auto-optimize

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Gold Layer Materialized View
# MAGIC DROP MATERIALIZED VIEW IF EXISTS investments.fred.gold_observations;
# MAGIC CREATE MATERIALIZED VIEW investments.fred.gold_observations
# MAGIC CLUSTER BY (series_id, date)
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true',
# MAGIC     'delta.enableChangeDataFeed' = 'false',
# MAGIC     'delta.tuneFileSizesForRewrites' = 'true',
# MAGIC     'pipelines.autoOptimize.managed' = 'true',
# MAGIC     'quality' = 'gold'
# MAGIC )
# MAGIC COMMENT 'Gold layer: Denormalized FRED data with metadata (Materialized View)'
# MAGIC AS
# MAGIC SELECT
# MAGIC   r.series_id,
# MAGIC   r.date,
# MAGIC   r.value,
# MAGIC   m.title,
# MAGIC   m.friendly_name,
# MAGIC   m.units,
# MAGIC   'FRED' AS source
# MAGIC FROM
# MAGIC   investments.fred.silver_observations r
# MAGIC JOIN
# MAGIC   investments.fred.silver_metadata m
# MAGIC ON
# MAGIC   r.series_id = m.series_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Note: Materialized Views automatically maintain data freshness
# MAGIC -- Change Data Feed is not applicable to Materialized Views
# MAGIC -- The view will automatically refresh when source tables change