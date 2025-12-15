-- =============================================================================
-- FRED Data Pipeline - Complete Setup SQL
-- =============================================================================
-- This file contains all DDL statements for setting up the FRED pipeline.
-- Run notebooks in order instead, or use this for reference.
-- =============================================================================

-- =============================================================================
-- SCHEMA AND VOLUMES
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS investments.fred;

CREATE VOLUME IF NOT EXISTS investments.fred.observations;
CREATE VOLUME IF NOT EXISTS investments.fred.metadata;

-- =============================================================================
-- BRONZE LAYER
-- =============================================================================

CREATE TABLE IF NOT EXISTS investments.fred.bronze_observations (
    run_timestamp STRING,
    series_id STRING,
    series_name STRING,
    date STRING,
    value DOUBLE
)
USING DELTA
COMMENT 'Bronze layer: Raw FRED observations';

CREATE TABLE IF NOT EXISTS investments.fred.bronze_metadata (
    run_timestamp STRING,
    series_id STRING,
    friendly_name STRING,
    title STRING,
    frequency STRING,
    frequency_short STRING,
    units STRING,
    units_short STRING,
    seasonal_adjustment STRING,
    seasonal_adjustment_short STRING,
    observation_start STRING,
    observation_end STRING,
    last_updated STRING,
    popularity STRING,
    notes STRING
)
USING DELTA
COMMENT 'Bronze layer: Raw FRED series metadata';

-- =============================================================================
-- SILVER LAYER
-- =============================================================================

CREATE TABLE IF NOT EXISTS investments.fred.silver_metadata (
    series_id STRING NOT NULL,
    friendly_name STRING,
    title STRING,
    frequency STRING,
    frequency_short STRING,
    units STRING,
    units_short STRING,
    seasonal_adjustment STRING,
    seasonal_adjustment_short STRING,
    observation_start DATE,
    observation_end DATE,
    last_updated TIMESTAMP,
    popularity INT,
    notes STRING,
    run_timestamp TIMESTAMP,
    updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Silver layer: Cleaned FRED series metadata';

CREATE TABLE IF NOT EXISTS investments.fred.silver_observations (
    series_id STRING,
    series_name STRING,
    date DATE,
    value DOUBLE,
    run_timestamp TIMESTAMP,
    updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Silver layer: Cleaned FRED observations';

-- Primary Key on silver_metadata
ALTER TABLE investments.fred.silver_metadata 
ADD CONSTRAINT pk_series PRIMARY KEY(series_id);

-- Foreign Key from silver_observations to silver_metadata
ALTER TABLE investments.fred.silver_observations
ADD CONSTRAINT fk_series
FOREIGN KEY(series_id) REFERENCES investments.fred.silver_metadata(series_id);

-- Foreign Key from silver_observations to dim_calendar (optional)
-- ALTER TABLE investments.fred.silver_observations
-- ADD CONSTRAINT fk_date
-- FOREIGN KEY(date) REFERENCES common.reference.dim_calendar(calendar_date);

-- =============================================================================
-- GOLD LAYER
-- =============================================================================

CREATE TABLE IF NOT EXISTS investments.fred.gold_observations (
    series_id STRING,
    date DATE,
    value DOUBLE,
    title STRING,
    friendly_name STRING,
    units STRING,
    source STRING
)
USING DELTA
COMMENT 'Gold layer: Denormalized FRED data with metadata';

-- Enable Change Data Feed for downstream consumers
ALTER TABLE investments.fred.gold_observations
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
