# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS investments.fred.gold_observations;
# MAGIC CREATE TABLE IF NOT EXISTS investments.fred.gold_observations (
# MAGIC   series_id STRING,
# MAGIC   date DATE,
# MAGIC   value DOUBLE,
# MAGIC   title STRING,
# MAGIC   friendly_name STRING,
# MAGIC   units STRING,
# MAGIC   source STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE investments.fred.gold_observations
# MAGIC   SET TBLPROPERTIES (delta.enableChangeDataFeed = true)