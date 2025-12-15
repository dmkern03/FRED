# Databricks notebook source
# MAGIC %sql
# MAGIC MERGE INTO investments.fred.silver_metadata AS target
# MAGIC USING (
# MAGIC     SELECT 
# MAGIC         series_id,
# MAGIC         friendly_name,
# MAGIC         title,
# MAGIC         frequency,
# MAGIC         frequency_short,
# MAGIC         units,
# MAGIC         units_short,
# MAGIC         seasonal_adjustment,
# MAGIC         seasonal_adjustment_short,
# MAGIC         TO_DATE(observation_start) AS observation_start,
# MAGIC         TO_DATE(observation_end) AS observation_end,
# MAGIC         try_to_timestamp(last_updated, 'yyyy-MM-dd HH:mm:ssXXX') AS last_updated,
# MAGIC         CAST(popularity AS INT) AS popularity,
# MAGIC         notes,
# MAGIC         try_to_timestamp(run_timestamp, 'yyyy-MM-dd HH:mm:ss') AS run_timestamp,
# MAGIC         current_timestamp() AS updated_at
# MAGIC     FROM (
# MAGIC         SELECT *,
# MAGIC             ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY run_timestamp DESC) AS rn
# MAGIC         FROM investments.fred.bronze_metadata
# MAGIC     )
# MAGIC     WHERE rn = 1
# MAGIC ) AS source
# MAGIC ON target.series_id = source.series_id
# MAGIC WHEN MATCHED AND (
# MAGIC         target.friendly_name != source.friendly_name OR
# MAGIC         target.title != source.title OR
# MAGIC         target.frequency != source.frequency OR
# MAGIC         target.frequency_short != source.frequency_short OR
# MAGIC         target.units != source.units OR
# MAGIC         target.units_short != source.units_short OR
# MAGIC         target.seasonal_adjustment != source.seasonal_adjustment OR
# MAGIC         target.seasonal_adjustment_short != source.seasonal_adjustment_short OR
# MAGIC         target.observation_start != source.observation_start OR
# MAGIC         target.observation_end != source.observation_end 
# MAGIC     )
# MAGIC THEN
# MAGIC     UPDATE SET
# MAGIC         target.friendly_name = source.friendly_name,
# MAGIC         target.title = source.title,
# MAGIC         target.frequency = source.frequency,
# MAGIC         target.frequency_short = source.frequency_short,
# MAGIC         target.units = source.units,
# MAGIC         target.units_short = source.units_short,
# MAGIC         target.seasonal_adjustment = source.seasonal_adjustment,
# MAGIC         target.seasonal_adjustment_short = source.seasonal_adjustment_short,
# MAGIC         target.observation_start = source.observation_start,
# MAGIC         target.observation_end = source.observation_end,
# MAGIC         target.last_updated = source.last_updated,
# MAGIC         target.popularity = source.popularity,
# MAGIC         target.notes = source.notes,
# MAGIC         target.run_timestamp = source.run_timestamp,
# MAGIC         target.updated_at = source.updated_at
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (series_id, friendly_name, title, frequency, frequency_short, units, units_short, 
# MAGIC             seasonal_adjustment, seasonal_adjustment_short, observation_start, observation_end, 
# MAGIC             last_updated, popularity, notes, run_timestamp, updated_at)
# MAGIC     VALUES (source.series_id, source.friendly_name, source.title, source.frequency, source.frequency_short, 
# MAGIC             source.units, source.units_short, source.seasonal_adjustment, source.seasonal_adjustment_short, 
# MAGIC             source.observation_start, source.observation_end, source.last_updated, source.popularity, 
# MAGIC             source.notes, source.run_timestamp, source.updated_at);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- -----------------------------------------------------------------------------
# MAGIC -- Load Silver Observations
# MAGIC -- Converts data types and deduplicates by series_id + date (max run_timestamp)
# MAGIC -- -----------------------------------------------------------------------------
# MAGIC MERGE INTO investments.fred.silver_observations AS target
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         series_id,
# MAGIC         series_name,
# MAGIC         TO_DATE(date) AS date,
# MAGIC         CAST(value AS DOUBLE) AS value,
# MAGIC         TO_TIMESTAMP(run_timestamp, 'yyyy-MM-dd HH:mm:ss') AS run_timestamp,
# MAGIC         current_timestamp() AS updated_at
# MAGIC     FROM (
# MAGIC         SELECT *,
# MAGIC             ROW_NUMBER() OVER (PARTITION BY series_id, date ORDER BY run_timestamp DESC) AS rn
# MAGIC         FROM investments.fred.bronze_observations
# MAGIC         WHERE value IS NOT NULL
# MAGIC     )
# MAGIC     WHERE rn = 1
# MAGIC ) AS source
# MAGIC ON target.series_id = source.series_id AND target.date = source.date
# MAGIC WHEN MATCHED AND target.value != source.value THEN
# MAGIC     UPDATE SET
# MAGIC         target.series_name = source.series_name,
# MAGIC         target.value = source.value,
# MAGIC         target.run_timestamp = source.run_timestamp,
# MAGIC         target.updated_at = source.updated_at
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (series_id, series_name, date, value, run_timestamp, updated_at)
# MAGIC     VALUES (source.series_id, source.series_name, source.date, source.value, source.run_timestamp, source.updated_at);