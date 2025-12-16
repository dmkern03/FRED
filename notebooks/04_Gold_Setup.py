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
# MAGIC - Optimized for analytical queries
# MAGIC - Business-ready denormalized schema
# MAGIC - Query performance optimization with Delta auto-optimize

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Observations Materialized View
# MAGIC
# MAGIC Denormalized view joining silver_observations and silver_metadata for business-ready analytics.
# MAGIC
# MAGIC **Features:**
# MAGIC - Automatic refresh when source tables change
# MAGIC - Optimized read performance with auto-optimize
# MAGIC - Denormalized schema (no joins needed for queries)

# COMMAND ----------

@dlt.table(
    name="gold_observations",
    comment="Gold layer: Denormalized FRED data with metadata (Materialized View)",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "false",
        "delta.tuneFileSizesForRewrites": "true",
        "pipelines.autoOptimize.managed": "true",
        "quality": "gold"
    }
)
def gold_observations():
    """
    Gold layer: Business-ready denormalized FRED data.

    Joins:
    - silver_observations (fact table)
    - silver_metadata (dimension table)

    Output Schema:
    - series_id: Economic series identifier
    - date: Observation date
    - value: Observation value
    - title: Full series title
    - friendly_name: Short friendly name
    - units: Measurement units
    - source: Data source (always 'FRED')
    """
    # Read from silver tables
    observations = dlt.read("silver_observations")
    metadata = dlt.read("silver_metadata")

    # Join and select business-ready columns
    return (
        observations.alias("r")
        .join(metadata.alias("m"), observations.series_id == metadata.series_id, "inner")
        .select(
            col("r.series_id").alias("series_id"),
            col("r.date").alias("date"),
            col("r.value").alias("value"),
            col("m.title").alias("title"),
            col("m.friendly_name").alias("friendly_name"),
            col("m.units").alias("units"),
            lit("FRED").alias("source")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Materialized View Behavior
# MAGIC
# MAGIC **Automatic Refresh:**
# MAGIC - The view automatically refreshes when source tables (silver_observations, silver_metadata) update
# MAGIC - No manual refresh needed when running in DLT pipeline
# MAGIC - Delta Live Tables manages the refresh schedule based on upstream changes
# MAGIC
# MAGIC **Change Data Feed:**
# MAGIC - Not applicable to materialized views (disabled in table properties)
# MAGIC - The view is read-only and always reflects the current state of source tables
# MAGIC
# MAGIC **Query Performance:**
# MAGIC - Optimized for analytical queries with auto-optimize enabled
# MAGIC - Denormalized schema eliminates need for joins in downstream queries
# MAGIC - Delta auto-compaction maintains optimal file sizes
