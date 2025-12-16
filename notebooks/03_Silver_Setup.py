# Databricks notebook source
# MAGIC %md
# MAGIC # FRED Silver Layer Setup - Delta Live Tables
# MAGIC
# MAGIC This notebook defines the Silver layer Streaming Tables for the FRED data pipeline using Delta Live Tables (DLT).
# MAGIC
# MAGIC **Important:** This notebook should be run as part of a Delta Live Tables pipeline, not as a standalone notebook.
# MAGIC
# MAGIC ## What This Creates:
# MAGIC - **silver_metadata** - Streaming table with cleaned, typed metadata including PRIMARY KEY constraint
# MAGIC - **silver_observations** - Streaming table with cleaned, typed observations including FOREIGN KEY constraint
# MAGIC
# MAGIC ## Data Quality Features:
# MAGIC - Type conversions (STRING → DATE, TIMESTAMP, INT, DOUBLE)
# MAGIC - NOT NULL constraints on critical fields
# MAGIC - Primary key constraint on series_id
# MAGIC - Foreign key constraint ensuring referential integrity
# MAGIC - Automatic filtering of invalid data (NULL values)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =============================================================================
# MAGIC -- FRED Silver Layer Setup (One-time)
# MAGIC -- =============================================================================
# MAGIC
# MAGIC -- Silver Streaming Table for metadata with proper data types
# MAGIC DROP TABLE IF EXISTS investments.fred.silver_metadata;
# MAGIC CREATE OR REPLACE STREAMING TABLE investments.fred.silver_metadata (
# MAGIC     series_id STRING NOT NULL,
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
# MAGIC     run_timestamp TIMESTAMP,
# MAGIC     updated_at TIMESTAMP,
# MAGIC     CONSTRAINT pk_series PRIMARY KEY(series_id)
# MAGIC )
# MAGIC CLUSTER BY (series_id)
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true',
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'delta.tuneFileSizesForRewrites' = 'true',
# MAGIC     'delta.deletedFileRetentionDuration' = 'interval 7 days',
# MAGIC     'pipelines.autoOptimize.managed' = 'true',
# MAGIC     'pipelines.reset.allowed' = 'false',
# MAGIC     'quality' = 'silver'
# MAGIC )
# MAGIC COMMENT 'Silver layer: Cleaned FRED series metadata with proper data types (Streaming Table)'
# MAGIC AS SELECT
# MAGIC     series_id,
# MAGIC     friendly_name,
# MAGIC     title,
# MAGIC     frequency,
# MAGIC     frequency_short,
# MAGIC     units,
# MAGIC     units_short,
# MAGIC     seasonal_adjustment,
# MAGIC     seasonal_adjustment_short,
# MAGIC     TO_DATE(observation_start) AS observation_start,
# MAGIC     TO_DATE(observation_end) AS observation_end,
# MAGIC     try_to_timestamp(last_updated, 'yyyy-MM-dd HH:mm:ssXXX') AS last_updated,
# MAGIC     CAST(popularity AS INT) AS popularity,
# MAGIC     notes,
# MAGIC     try_to_timestamp(run_timestamp, 'yyyy-MM-dd HH:mm:ss') AS run_timestamp,
# MAGIC     current_timestamp() AS updated_at
# MAGIC FROM STREAM(investments.fred.bronze_metadata);
# MAGIC
# MAGIC -- Silver Streaming Table for observations with proper data types
# MAGIC DROP TABLE IF EXISTS investments.fred.silver_observations;
# MAGIC CREATE OR REPLACE STREAMING TABLE investments.fred.silver_observations (
# MAGIC     series_id STRING NOT NULL,
# MAGIC     series_name STRING,
# MAGIC     date DATE NOT NULL,
# MAGIC     value DOUBLE NOT NULL,
# MAGIC     run_timestamp TIMESTAMP,
# MAGIC     updated_at TIMESTAMP,
# MAGIC     CONSTRAINT fk_series FOREIGN KEY(series_id) REFERENCES investments.fred.silver_metadata(series_id)
# MAGIC )
# MAGIC CLUSTER BY (series_id, date)
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true',
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'delta.tuneFileSizesForRewrites' = 'true',
# MAGIC     'delta.deletedFileRetentionDuration' = 'interval 7 days',
# MAGIC     'pipelines.autoOptimize.managed' = 'true',
# MAGIC     'pipelines.reset.allowed' = 'false',
# MAGIC     'quality' = 'silver'
# MAGIC )
# MAGIC COMMENT 'Silver layer: Cleaned FRED observations with proper data types (Streaming Table)'
# MAGIC AS SELECT
# MAGIC     series_id,
# MAGIC     series_name,
# MAGIC     TO_DATE(date) AS date,
# MAGIC     CAST(value AS DOUBLE) AS value,
# MAGIC     TO_TIMESTAMP(run_timestamp, 'yyyy-MM-dd HH:mm:ss') AS run_timestamp,
# MAGIC     current_timestamp() AS updated_at
# MAGIC FROM STREAM(investments.fred.bronze_observations)
# MAGIC WHERE value IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Primary Key and Foreign Key Constraints
# MAGIC
# MAGIC The following constraints are defined inline in the Streaming Table definitions above:
# MAGIC
# MAGIC ### Primary Keys:
# MAGIC - **silver_metadata.series_id** (PK) - Unique identifier for each economic series
# MAGIC
# MAGIC ### Foreign Keys:
# MAGIC - **silver_observations.series_id** → **silver_metadata.series_id** (FK)
# MAGIC   - Ensures referential integrity between observations and metadata
# MAGIC   - Every observation must have corresponding metadata
# MAGIC
# MAGIC ### Optional Foreign Key:
# MAGIC - **silver_observations.date** → **common.reference.dim_calendar.calendar_date** (FK, commented out)
# MAGIC   - Can be enabled if you have a calendar dimension table
# MAGIC   - Ensures all observation dates are valid calendar dates
# MAGIC
# MAGIC **Note:** In Delta Live Tables with Streaming Tables, constraints are informational and help document
# MAGIC the data model. They are enforced at the application level during streaming ingestion.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify constraints are in place
# MAGIC DESCRIBE EXTENDED investments.fred.silver_metadata;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify constraints are in place
# MAGIC DESCRIBE EXTENDED investments.fred.silver_observations;