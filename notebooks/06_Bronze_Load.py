# Databricks notebook source
# MAGIC %sql
# MAGIC -- Load observations data
# MAGIC COPY INTO investments.fred.bronze_observations
# MAGIC FROM '/Volumes/investments/fred/observations/'
# MAGIC FILEFORMAT = CSV
# MAGIC PATTERN = 'fred_observations_historical_*.csv'
# MAGIC FORMAT_OPTIONS (
# MAGIC     'header' = 'true',
# MAGIC     'inferSchema' = 'true'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO investments.fred.bronze_metadata
# MAGIC FROM '/Volumes/investments/fred/metadata/'
# MAGIC FILEFORMAT = JSON
# MAGIC PATTERN = 'fred_series_metadata_*.json';