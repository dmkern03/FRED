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
-- BRONZE LAYER - STREAMING TABLES
-- =============================================================================

-- Bronze Streaming Table for observations
DROP TABLE IF EXISTS investments.fred.bronze_observations;
CREATE OR REPLACE STREAMING TABLE investments.fred.bronze_observations (
    run_timestamp TIMESTAMP,
    series_id STRING,
    series_name STRING,
    date DATE,
    value DOUBLE,
    ingestion_timestamp TIMESTAMP
)
CLUSTER BY (series_id, date)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'false',
    'delta.tuneFileSizesForRewrites' = 'true',
    'pipelines.autoOptimize.managed' = 'true',
    'pipelines.reset.allowed' = 'false',
    'quality' = 'bronze'
)
COMMENT 'Bronze layer: Raw FRED observations (Streaming Table)'
AS SELECT
    run_timestamp,
    series_id,
    series_name,
    date,
    value,
    ingestion_timestamp
FROM cloud_files(
    '/Volumes/investments/fred/observations/',
    'csv',
    map('header', 'true', 'inferSchema', 'true', 'cloudFiles.schemaLocation', '/Volumes/investments/fred/observations/_schema')
);

-- Bronze Streaming Table for metadata
DROP TABLE IF EXISTS investments.fred.bronze_metadata;
CREATE OR REPLACE STREAMING TABLE investments.fred.bronze_metadata (
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
CLUSTER BY (series_id)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'false',
    'delta.tuneFileSizesForRewrites' = 'true',
    'pipelines.autoOptimize.managed' = 'true',
    'pipelines.reset.allowed' = 'false',
    'quality' = 'bronze'
)
COMMENT 'Bronze layer: Raw FRED series metadata (Streaming Table)'
AS SELECT
    run_timestamp,
    series_id,
    friendly_name,
    title,
    frequency,
    frequency_short,
    units,
    units_short,
    seasonal_adjustment,
    seasonal_adjustment_short,
    observation_start,
    observation_end,
    last_updated,
    popularity,
    notes
FROM cloud_files(
    '/Volumes/investments/fred/metadata/',
    'json',
    map('cloudFiles.schemaLocation', '/Volumes/investments/fred/metadata/_schema')
);

-- =============================================================================
-- SILVER LAYER - STREAMING TABLES
-- =============================================================================

-- Silver Streaming Table for metadata with proper data types
DROP TABLE IF EXISTS investments.fred.silver_metadata;
CREATE OR REPLACE STREAMING TABLE investments.fred.silver_metadata (
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
    updated_at TIMESTAMP,
    CONSTRAINT pk_series PRIMARY KEY(series_id)
)
CLUSTER BY (series_id)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true',
    'delta.tuneFileSizesForRewrites' = 'true',
    'delta.deletedFileRetentionDuration' = 'interval 7 days',
    'pipelines.autoOptimize.managed' = 'true',
    'pipelines.reset.allowed' = 'false',
    'quality' = 'silver'
)
COMMENT 'Silver layer: Cleaned FRED series metadata (Streaming Table)'
AS SELECT
    series_id,
    friendly_name,
    title,
    frequency,
    frequency_short,
    units,
    units_short,
    seasonal_adjustment,
    seasonal_adjustment_short,
    TO_DATE(observation_start) AS observation_start,
    TO_DATE(observation_end) AS observation_end,
    try_to_timestamp(last_updated, 'yyyy-MM-dd HH:mm:ssXXX') AS last_updated,
    CAST(popularity AS INT) AS popularity,
    notes,
    try_to_timestamp(run_timestamp, 'yyyy-MM-dd HH:mm:ss') AS run_timestamp,
    current_timestamp() AS updated_at
FROM STREAM(investments.fred.bronze_metadata);

-- Silver Streaming Table for observations with proper data types
DROP TABLE IF EXISTS investments.fred.silver_observations;
CREATE OR REPLACE STREAMING TABLE investments.fred.silver_observations (
    series_id STRING NOT NULL,
    series_name STRING,
    date DATE NOT NULL,
    value DOUBLE NOT NULL,
    run_timestamp TIMESTAMP,
    updated_at TIMESTAMP,
    CONSTRAINT fk_series FOREIGN KEY(series_id) REFERENCES investments.fred.silver_metadata(series_id)
)
CLUSTER BY (series_id, date)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true',
    'delta.tuneFileSizesForRewrites' = 'true',
    'delta.deletedFileRetentionDuration' = 'interval 7 days',
    'pipelines.autoOptimize.managed' = 'true',
    'pipelines.reset.allowed' = 'false',
    'quality' = 'silver'
)
COMMENT 'Silver layer: Cleaned FRED observations (Streaming Table)'
AS SELECT
    series_id,
    series_name,
    TO_DATE(date) AS date,
    CAST(value AS DOUBLE) AS value,
    TO_TIMESTAMP(run_timestamp, 'yyyy-MM-dd HH:mm:ss') AS run_timestamp,
    current_timestamp() AS updated_at
FROM STREAM(investments.fred.bronze_observations)
WHERE value IS NOT NULL;

-- Optional: Foreign Key from silver_observations to dim_calendar
-- ALTER TABLE investments.fred.silver_observations
-- ADD CONSTRAINT fk_date
-- FOREIGN KEY(date) REFERENCES common.reference.dim_calendar(calendar_date);

-- =============================================================================
-- GOLD LAYER - MATERIALIZED VIEW
-- =============================================================================

-- Gold Materialized View for denormalized observations
DROP MATERIALIZED VIEW IF EXISTS investments.fred.gold_observations;
CREATE MATERIALIZED VIEW investments.fred.gold_observations
CLUSTER BY (series_id, date)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'false',
    'delta.tuneFileSizesForRewrites' = 'true',
    'pipelines.autoOptimize.managed' = 'true',
    'quality' = 'gold'
)
COMMENT 'Gold layer: Denormalized FRED data with metadata (Materialized View)'
AS
SELECT
  r.series_id,
  r.date,
  r.value,
  m.title,
  m.friendly_name,
  m.units,
  'FRED' AS source
FROM
  investments.fred.silver_observations r
JOIN
  investments.fred.silver_metadata m
ON
  r.series_id = m.series_id;

-- Note: Materialized Views automatically maintain data freshness
-- To manually refresh if needed: REFRESH MATERIALIZED VIEW investments.fred.gold_observations;
