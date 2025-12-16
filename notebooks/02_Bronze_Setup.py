# Databricks notebook source
# MAGIC %md
# MAGIC # FRED Bronze Layer Setup - Delta Live Tables
# MAGIC
# MAGIC This notebook defines the Bronze layer Streaming Tables for the FRED data pipeline using Delta Live Tables (DLT).
# MAGIC
# MAGIC **Important:** This notebook should be run as part of a Delta Live Tables pipeline, not as a standalone notebook.
# MAGIC
# MAGIC ## How to Use:
# MAGIC 1. Create a DLT pipeline in Databricks UI or using the DAB configuration in `resources/pipelines.yml`
# MAGIC 2. Add this notebook (along with 03_Silver_Setup.py and 04_Gold_Setup.py) to the pipeline
# MAGIC 3. Start the pipeline to create and populate the streaming tables
# MAGIC
# MAGIC ## What This Creates:
# MAGIC - **bronze_observations** - Streaming table for FRED observation data (CSV from volumes)
# MAGIC - **bronze_metadata** - Streaming table for FRED series metadata (JSON from volumes)

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Observations Streaming Table
# MAGIC
# MAGIC Ingests CSV files from `/Volumes/investments/fred/observations/` using Auto Loader (cloudFiles).
# MAGIC
# MAGIC **Features:**
# MAGIC - Auto Loader for continuous file ingestion
# MAGIC - Schema inference and evolution
# MAGIC - Optimized write and auto-compaction

# COMMAND ----------

@dlt.table(
    name="bronze_observations",
    comment="Bronze layer: Raw FRED observations (Streaming Table)",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "false",
        "delta.tuneFileSizesForRewrites": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.reset.allowed": "false",
        "quality": "bronze"
    }
)
def bronze_observations():
    """
    Bronze layer: Raw FRED observations from CSV files in volumes.

    Reads from: /Volumes/investments/fred/observations/
    Schema Location: /Volumes/investments/fred/observations/_schema
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.schemaLocation", "/Volumes/investments/fred/observations/_schema")
        .load("/Volumes/investments/fred/observations/")
        .select(
            col("run_timestamp").cast("timestamp").alias("run_timestamp"),
            col("series_id").cast("string").alias("series_id"),
            col("series_name").cast("string").alias("series_name"),
            col("date").cast("date").alias("date"),
            col("value").cast("double").alias("value"),
            current_timestamp().alias("ingestion_timestamp")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Metadata Streaming Table
# MAGIC
# MAGIC Ingests JSON files from `/Volumes/investments/fred/metadata/` using Auto Loader (cloudFiles).
# MAGIC
# MAGIC **Features:**
# MAGIC - Auto Loader for continuous file ingestion
# MAGIC - Schema inference and evolution
# MAGIC - Optimized write and auto-compaction

# COMMAND ----------

@dlt.table(
    name="bronze_metadata",
    comment="Bronze layer: Raw FRED series metadata/dimensional info (Streaming Table)",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "false",
        "delta.tuneFileSizesForRewrites": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.reset.allowed": "false",
        "quality": "bronze"
    }
)
def bronze_metadata():
    """
    Bronze layer: Raw FRED series metadata from JSON files in volumes.

    Reads from: /Volumes/investments/fred/metadata/
    Schema Location: /Volumes/investments/fred/metadata/_schema
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/Volumes/investments/fred/metadata/_schema")
        .load("/Volumes/investments/fred/metadata/")
        .select(
            col("run_timestamp").cast("string").alias("run_timestamp"),
            col("series_id").cast("string").alias("series_id"),
            col("friendly_name").cast("string").alias("friendly_name"),
            col("title").cast("string").alias("title"),
            col("frequency").cast("string").alias("frequency"),
            col("frequency_short").cast("string").alias("frequency_short"),
            col("units").cast("string").alias("units"),
            col("units_short").cast("string").alias("units_short"),
            col("seasonal_adjustment").cast("string").alias("seasonal_adjustment"),
            col("seasonal_adjustment_short").cast("string").alias("seasonal_adjustment_short"),
            col("observation_start").cast("string").alias("observation_start"),
            col("observation_end").cast("string").alias("observation_end"),
            col("last_updated").cast("string").alias("last_updated"),
            col("popularity").cast("string").alias("popularity"),
            col("notes").cast("string").alias("notes")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Flow:**
# MAGIC ```
# MAGIC FRED API
# MAGIC     │
# MAGIC     ▼
# MAGIC ┌─────────────────────────────────────────┐
# MAGIC │  Landing Zone (CSV in Volumes)          │
# MAGIC │  /Volumes/investments/fred/observations/ │
# MAGIC │  /Volumes/investments/fred/metadata/    │
# MAGIC └─────────────────────────────────────────┘
# MAGIC     │
# MAGIC     ▼
# MAGIC ┌─────────────────────────────────────────┐
# MAGIC │  Bronze Layer (Delta Tables)            │
# MAGIC │  investments.fred.bronze_observations   │
# MAGIC │  investments.fred.bronze_metadata       │
# MAGIC └─────────────────────────────────────────┘
# MAGIC ```
