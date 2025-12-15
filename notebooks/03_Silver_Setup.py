# Databricks notebook source
# MAGIC %sql
# MAGIC -- =============================================================================
# MAGIC -- FRED Silver Layer Setup (One-time)
# MAGIC -- =============================================================================
# MAGIC
# MAGIC -- Silver table for metadata with proper data types
# MAGIC DROP TABLE IF EXISTS investments.fred.silver_metadat;
# MAGIC CREATE TABLE IF NOT EXISTS investments.fred.silver_metadata (
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
# MAGIC     run_timestamp TIMESTAMP,
# MAGIC     updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC -- -- COMMENT 'Silver layer: Cleaned FRED series metadata with proper data types';
# MAGIC
# MAGIC -- Silver table for observations with proper data types
# MAGIC DROP TABLE IF EXISTS investments.fred.silver_observations;
# MAGIC CREATE TABLE IF NOT EXISTS investments.fred.silver_observations (
# MAGIC     series_id STRING,
# MAGIC     series_name STRING,
# MAGIC     date DATE,
# MAGIC     value DOUBLE,
# MAGIC     run_timestamp TIMESTAMP,
# MAGIC     updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC -- -- COMMENT 'Silver layer: Cleaned FRED observations with proper data types';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Make series_id NOT NULL
# MAGIC ALTER TABLE investments.fred.silver_metadata 
# MAGIC ALTER COLUMN series_id SET NOT NULL;
# MAGIC
# MAGIC -- Now add the primary key constraint
# MAGIC ALTER TABLE investments.fred.silver_metadata 
# MAGIC ADD CONSTRAINT pk_series PRIMARY KEY(series_id);
# MAGIC
# MAGIC -- Add the foreign key constraint (after primary key is set)
# MAGIC ALTER TABLE investments.fred.silver_observations
# MAGIC ADD CONSTRAINT fk_series
# MAGIC FOREIGN KEY(series_id) REFERENCES investments.fred.silver_metadata(series_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop the existing primary key constraint first
# MAGIC ALTER TABLE common.reference.dim_calendar
# MAGIC DROP CONSTRAINT pk_date;
# MAGIC
# MAGIC -- Make calendar_date NOT NULL
# MAGIC ALTER TABLE common.reference.dim_calendar
# MAGIC ALTER COLUMN calendar_date SET NOT NULL;
# MAGIC
# MAGIC -- Add the primary key constraint
# MAGIC ALTER TABLE common.reference.dim_calendar
# MAGIC ADD CONSTRAINT pk_date PRIMARY KEY(calendar_date);
# MAGIC
# MAGIC -- Add the foreign key constraint
# MAGIC ALTER TABLE investments.fred.silver_observations
# MAGIC ADD CONSTRAINT fk_date
# MAGIC FOREIGN KEY(date) REFERENCES common.reference.dim_calendar(calendar_date);