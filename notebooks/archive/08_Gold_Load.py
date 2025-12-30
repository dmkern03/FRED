# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer Load (Deprecated)
# MAGIC
# MAGIC **Note:** The gold layer is now defined as a **Materialized View** that automatically maintains data freshness.
# MAGIC
# MAGIC The materialized view `investments.fred.gold_observations`:
# MAGIC - Automatically joins `silver_observations` and `silver_metadata`
# MAGIC - Refreshes automatically when source tables change
# MAGIC - No manual INSERT OVERWRITE needed
# MAGIC
# MAGIC **No manual data loading commands are needed.** The materialized view stays up-to-date automatically.
# MAGIC
# MAGIC This notebook is kept for reference but is no longer necessary in the pipeline execution flow.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Legacy load logic (no longer used with Materialized View)
# MAGIC -- The materialized view automatically maintains the joined data
# MAGIC -- Query the materialized view to see current data:
# MAGIC SELECT *
# MAGIC FROM investments.fred.gold_observations
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- To manually refresh the materialized view (if needed):
# MAGIC -- REFRESH MATERIALIZED VIEW investments.fred.gold_observations;