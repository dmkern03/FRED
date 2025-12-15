# Databricks notebook source
# MAGIC %sql
# MAGIC -- Create schema
# MAGIC CREATE SCHEMA IF NOT EXISTS investments.fred;
# MAGIC
# MAGIC -- Landing Zone Volumes (CSV files)
# MAGIC CREATE VOLUME IF NOT EXISTS investments.fred.observations;
# MAGIC CREATE VOLUME IF NOT EXISTS investments.fred.metadata;
# MAGIC
# MAGIC -- Bronze Layer Delta Tables
# MAGIC DROP TABLE IF EXISTS investments.fred.bronze_observations;
# MAGIC CREATE TABLE IF NOT EXISTS investments.fred.bronze_observations (
# MAGIC     run_timestamp TIMESTAMP,
# MAGIC     series_id STRING,
# MAGIC     series_name STRING,
# MAGIC     date DATE,
# MAGIC     value DOUBLE,
# MAGIC     ingestion_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Bronze layer: Raw FRED observations';
# MAGIC
# MAGIC DROP TABLE IF EXISTS investments.fred.bronze_metadata;
# MAGIC CREATE TABLE IF NOT EXISTS investments.fred.bronze_metadata (
# MAGIC     run_timestamp TIMESTAMP,
# MAGIC     series_id STRING,
# MAGIC     friendly_name STRING,
# MAGIC     title STRING,
# MAGIC     frequency STRING,
# MAGIC     frequency_short STRING,
# MAGIC     units STRING,
# MAGIC     units_short STRING,
# MAGIC     seasonal_adjustment STRING,
# MAGIC     seasonal_adjustment_short STRING,
# MAGIC     observation_start DATE,
# MAGIC     observation_end DATE,
# MAGIC     last_updated TIMESTAMP,
# MAGIC     popularity INT,
# MAGIC     notes STRING,
# MAGIC     file_path STRING,
# MAGIC     ingestion_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Bronze layer: Raw FRED series metadata/dimensional info';
# MAGIC

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
# MAGIC -- Drop the existing table
# MAGIC DROP TABLE IF EXISTS investments.fred.bronze_metadata;
# MAGIC
# MAGIC -- Recreate with all STRING columns
# MAGIC CREATE TABLE investments.fred.bronze_metadata (
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
# MAGIC USING DELTA;