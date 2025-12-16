# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer Load (Deprecated)
# MAGIC
# MAGIC **Note:** Silver tables are now defined as **Streaming Tables** that automatically process data from bronze streaming tables.
# MAGIC
# MAGIC The silver streaming tables:
# MAGIC - `investments.fred.silver_metadata` - Reads from STREAM(bronze_metadata) with type conversions
# MAGIC - `investments.fred.silver_observations` - Reads from STREAM(bronze_observations) with type conversions
# MAGIC
# MAGIC **No manual MERGE commands are needed.** Data transformation and loading happens automatically as part of the Delta Live Tables pipeline.
# MAGIC
# MAGIC This notebook is kept for reference but is no longer necessary in the pipeline execution flow.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Legacy MERGE logic (no longer used with Streaming Tables)
# MAGIC -- Silver metadata streaming table automatically processes bronze_metadata
# MAGIC -- with type conversions and deduplication

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Legacy MERGE logic (no longer used with Streaming Tables)
# MAGIC -- Silver observations streaming table automatically processes bronze_observations
# MAGIC -- with type conversions, deduplication, and filtering