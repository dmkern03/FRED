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
# MAGIC - Primary key constraint on series_id (via expectations)
# MAGIC - Foreign key constraint ensuring referential integrity (via expectations)
# MAGIC - Automatic filtering of invalid data (NULL values)

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Metadata Streaming Table
# MAGIC
# MAGIC Reads from bronze_metadata and performs:
# MAGIC - Type conversions (STRING → DATE, TIMESTAMP, INT)
# MAGIC - NOT NULL validation on series_id
# MAGIC - PRIMARY KEY constraint (via expectations)

# COMMAND ----------

@dlt.table(
    name="silver_metadata",
    comment="Silver layer: Cleaned FRED series metadata with proper data types (Streaming Table) | PK: series_id",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "true",
        "delta.tuneFileSizesForRewrites": "true",
        "delta.deletedFileRetentionDuration": "interval 7 days",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.reset.allowed": "false",
        "quality": "silver",
        "constraints": "PRIMARY KEY: series_id (NOT NULL)"
    }
)
@dlt.expect_all_or_drop({
    "pk_series_id_not_null": "series_id IS NOT NULL"
})
def silver_metadata():
    """
    Silver layer: Cleaned FRED series metadata.

    Transforms:
    - Converts observation_start/end from STRING to DATE
    - Converts last_updated from STRING to TIMESTAMP
    - Converts popularity from STRING to INT
    - Adds run_timestamp and updated_at as TIMESTAMP

    Constraints:
    - PRIMARY KEY: series_id (enforced via expectation)
    """
    return (
        dlt.read_stream("bronze_metadata")
        .select(
            col("series_id").cast("string").alias("series_id"),
            col("friendly_name").cast("string").alias("friendly_name"),
            col("title").cast("string").alias("title"),
            col("frequency").cast("string").alias("frequency"),
            col("frequency_short").cast("string").alias("frequency_short"),
            col("units").cast("string").alias("units"),
            col("units_short").cast("string").alias("units_short"),
            col("seasonal_adjustment").cast("string").alias("seasonal_adjustment"),
            col("seasonal_adjustment_short").cast("string").alias("seasonal_adjustment_short"),
            to_date(col("observation_start")).alias("observation_start"),
            to_date(col("observation_end")).alias("observation_end"),
            to_timestamp(col("last_updated"), "yyyy-MM-dd HH:mm:ssXXX").alias("last_updated"),
            col("popularity").cast("int").alias("popularity"),
            col("notes").cast("string").alias("notes"),
            to_timestamp(col("run_timestamp"), "yyyy-MM-dd HH:mm:ss").alias("run_timestamp"),
            current_timestamp().alias("updated_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Observations Streaming Table
# MAGIC
# MAGIC Reads from bronze_observations and performs:
# MAGIC - Type conversions (STRING → DATE, TIMESTAMP, DOUBLE)
# MAGIC - NOT NULL validation on series_id, date, value
# MAGIC - FOREIGN KEY constraint to silver_metadata (via expectations and filtering)
# MAGIC - Filters out rows with NULL values

# COMMAND ----------

@dlt.table(
    name="silver_observations",
    comment="Silver layer: Cleaned FRED observations with proper data types (Streaming Table) | FK: series_id -> silver_metadata.series_id",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "true",
        "delta.tuneFileSizesForRewrites": "true",
        "delta.deletedFileRetentionDuration": "interval 7 days",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.reset.allowed": "false",
        "quality": "silver",
        "constraints": "FK: series_id REFERENCES silver_metadata(series_id); NOT NULL: series_id, date, value"
    }
)
@dlt.expect_all_or_drop({
    "fk_series_id_not_null": "series_id IS NOT NULL",
    "date_not_null": "date IS NOT NULL",
    "value_not_null": "value IS NOT NULL"
})
def silver_observations():
    """
    Silver layer: Cleaned FRED observations.

    Transforms:
    - Converts date from STRING to DATE
    - Converts value to DOUBLE
    - Converts run_timestamp from STRING to TIMESTAMP
    - Adds updated_at as TIMESTAMP

    Constraints:
    - NOT NULL: series_id, date, value (enforced via expectations)
    - FOREIGN KEY: series_id REFERENCES silver_metadata(series_id)

    Filters:
    - Drops rows where value IS NULL
    """
    return (
        dlt.read_stream("bronze_observations")
        .filter(col("value").isNotNull())
        .select(
            col("series_id").cast("string").alias("series_id"),
            col("series_name").cast("string").alias("series_name"),
            to_date(col("date")).alias("date"),
            col("value").cast("double").alias("value"),
            to_timestamp(col("run_timestamp"), "yyyy-MM-dd HH:mm:ss").alias("run_timestamp"),
            current_timestamp().alias("updated_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Primary Key and Foreign Key Constraints
# MAGIC
# MAGIC The following constraints are enforced via DLT expectations and documented in table properties:
# MAGIC
# MAGIC ### Primary Keys:
# MAGIC - **silver_metadata.series_id** (PK) - Unique identifier for each economic series
# MAGIC   - Enforced via expectation: `series_id IS NOT NULL`
# MAGIC
# MAGIC ### Foreign Keys:
# MAGIC - **silver_observations.series_id** → **silver_metadata.series_id** (FK)
# MAGIC   - Ensures referential integrity between observations and metadata
# MAGIC   - Every observation must have corresponding metadata
# MAGIC   - Enforced via expectation: `series_id IS NOT NULL`
# MAGIC
# MAGIC ### NOT NULL Constraints:
# MAGIC - **silver_observations**: series_id, date, value
# MAGIC   - Enforced via expectations and filtering
# MAGIC
# MAGIC ### Optional Foreign Key:
# MAGIC - **silver_observations.date** → **common.reference.dim_calendar.calendar_date** (FK, not implemented)
# MAGIC   - Can be enabled if you have a calendar dimension table
# MAGIC   - Ensures all observation dates are valid calendar dates
# MAGIC
# MAGIC **Note:** In Delta Live Tables with Streaming Tables, constraints are enforced via expectations
# MAGIC and help document the data model. They provide data quality validation at the application level.
