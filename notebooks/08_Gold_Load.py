# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW temp_view_fred_rates AS
# MAGIC SELECT
# MAGIC   r.series_id,
# MAGIC   r.date,
# MAGIC   r.value,
# MAGIC   m.title,
# MAGIC   m.friendly_name,
# MAGIC   m.units,
# MAGIC   'FRED' AS source
# MAGIC FROM
# MAGIC   investments.fred.silver_rates r
# MAGIC JOIN
# MAGIC   investments.fred.silver_metadata m
# MAGIC ON
# MAGIC   r.series_id = m.series_id;
# MAGIC
# MAGIC SELECT *
# MAGIC FROM temp_view_fred_rates;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE TABLE investments.fred.gold_rates
# MAGIC SELECT *
# MAGIC FROM temp_view_fred_rates;