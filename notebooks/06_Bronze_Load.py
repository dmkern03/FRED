# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Load (Deprecated)
# MAGIC
# MAGIC **Note:** Bronze tables are now defined as **Streaming Tables** that automatically ingest data from cloud_files.
# MAGIC
# MAGIC The streaming tables continuously monitor:
# MAGIC - `/Volumes/investments/fred/observations/` for CSV files
# MAGIC - `/Volumes/investments/fred/metadata/` for JSON files
# MAGIC
# MAGIC **No manual COPY INTO commands are needed.** Data ingestion happens automatically as part of the Delta Live Tables pipeline.
# MAGIC
# MAGIC This notebook is kept for reference but is no longer necessary in the pipeline execution flow.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Legacy COPY INTO logic (no longer used with Streaming Tables)
# MAGIC -- COPY INTO investments.fred.bronze_observations
# MAGIC -- FROM '/Volumes/investments/fred/observations/'
# MAGIC -- FILEFORMAT = CSV
# MAGIC -- PATTERN = 'fred_observations_historical_*.csv'
# MAGIC -- FORMAT_OPTIONS (
# MAGIC --     'header' = 'true',
# MAGIC --     'inferSchema' = 'true'
# MAGIC -- )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Legacy COPY INTO logic (no longer used with Streaming Tables)
# MAGIC -- COPY INTO investments.fred.bronze_metadata
# MAGIC -- FROM '/Volumes/investments/fred/metadata/'
# MAGIC -- FILEFORMAT = JSON
# MAGIC -- PATTERN = 'fred_series_metadata_*.json';