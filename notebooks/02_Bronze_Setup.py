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

# MAGIC %sql
# MAGIC -- Create schema
# MAGIC CREATE SCHEMA IF NOT EXISTS investments.fred;
# MAGIC
# MAGIC -- Landing Zone Volumes (CSV files)
# MAGIC CREATE VOLUME IF NOT EXISTS investments.fred.observations;
# MAGIC CREATE VOLUME IF NOT EXISTS investments.fred.metadata;
# MAGIC
# MAGIC -- Bronze Layer Streaming Tables
# MAGIC DROP TABLE IF EXISTS investments.fred.bronze_observations;
# MAGIC CREATE OR REPLACE STREAMING TABLE investments.fred.bronze_observations (
# MAGIC     run_timestamp TIMESTAMP,
# MAGIC     series_id STRING,
# MAGIC     series_name STRING,
# MAGIC     date DATE,
# MAGIC     value DOUBLE,
# MAGIC     ingestion_timestamp TIMESTAMP
# MAGIC )
# MAGIC CLUSTER BY (series_id, date)
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true',
# MAGIC     'delta.enableChangeDataFeed' = 'false',
# MAGIC     'delta.tuneFileSizesForRewrites' = 'true',
# MAGIC     'pipelines.autoOptimize.managed' = 'true',
# MAGIC     'pipelines.reset.allowed' = 'false',
# MAGIC     'quality' = 'bronze'
# MAGIC )
# MAGIC COMMENT 'Bronze layer: Raw FRED observations (Streaming Table)'
# MAGIC AS SELECT
# MAGIC     run_timestamp,
# MAGIC     series_id,
# MAGIC     series_name,
# MAGIC     date,
# MAGIC     value,
# MAGIC     ingestion_timestamp
# MAGIC FROM cloud_files(
# MAGIC     '/Volumes/investments/fred/observations/',
# MAGIC     'csv',
# MAGIC     map('header', 'true', 'inferSchema', 'true', 'cloudFiles.schemaLocation', '/Volumes/investments/fred/observations/_schema')
# MAGIC );
# MAGIC
# MAGIC DROP TABLE IF EXISTS investments.fred.bronze_metadata;
# MAGIC CREATE OR REPLACE STREAMING TABLE investments.fred.bronze_metadata (
# MAGIC     run_timestamp STRING,
# MAGIC     series_id STRING,
# MAGIC     friendly_name STRING,
# MAGIC     title STRING,
# MAGIC     frequency STRING,
# MAGIC     frequency_short STRING,
# MAGIC     units STRING,
# MAGIC     units_short STRING,
# MAGIC     seasonal_adjustment STRING,
# MAGIC     seasonal_adjustment_short STRING,
# MAGIC     observation_start STRING,
# MAGIC     observation_end STRING,
# MAGIC     last_updated STRING,
# MAGIC     popularity STRING,
# MAGIC     notes STRING
# MAGIC )
# MAGIC CLUSTER BY (series_id)
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true',
# MAGIC     'delta.enableChangeDataFeed' = 'false',
# MAGIC     'delta.tuneFileSizesForRewrites' = 'true',
# MAGIC     'pipelines.autoOptimize.managed' = 'true',
# MAGIC     'pipelines.reset.allowed' = 'false',
# MAGIC     'quality' = 'bronze'
# MAGIC )
# MAGIC COMMENT 'Bronze layer: Raw FRED series metadata/dimensional info (Streaming Table)'
# MAGIC AS SELECT
# MAGIC     run_timestamp,
# MAGIC     series_id,
# MAGIC     friendly_name,
# MAGIC     title,
# MAGIC     frequency,
# MAGIC     frequency_short,
# MAGIC     units,
# MAGIC     units_short,
# MAGIC     seasonal_adjustment,
# MAGIC     seasonal_adjustment_short,
# MAGIC     observation_start,
# MAGIC     observation_end,
# MAGIC     last_updated,
# MAGIC     popularity,
# MAGIC     notes
# MAGIC FROM cloud_files(
# MAGIC     '/Volumes/investments/fred/metadata/',
# MAGIC     'json',
# MAGIC     map('cloudFiles.schemaLocation', '/Volumes/investments/fred/metadata/_schema')
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Flow:**
# MAGIC ```
# MAGIC FRED API
# MAGIC     │
# MAGIC             ▼
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

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Latest observations
# MAGIC SELECT * FROM investments.fred.bronze_observations
# MAGIC WHERE run_timestamp = (SELECT MAX(run_timestamp) FROM investments.fred.bronze_observations);
# MAGIC
# MAGIC -- Observation history for 10-Year Treasury
# MAGIC SELECT * FROM investments.fred.bronze_observations
# MAGIC WHERE series_id = 'DGS10'
# MAGIC ORDER BY date DESC;
# MAGIC
# MAGIC -- Metadata for all series
# MAGIC SELECT * FROM investments.fred.bronze_metadata
# MAGIC WHERE run_timestamp = (SELECT MAX(run_timestamp) FROM investments.fred.bronze_metadata);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Note: Bronze tables are now defined as Streaming Tables above
# MAGIC -- This cell is kept for reference but can be removed
