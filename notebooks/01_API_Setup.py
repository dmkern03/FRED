# Databricks notebook source
# MAGIC %md
# MAGIC # FRED API Setup
# MAGIC 
# MAGIC This notebook sets up the infrastructure for the FRED data pipeline:
# MAGIC - Creates schema and volumes
# MAGIC - Configures secret scope for API key
# MAGIC 
# MAGIC **Run this notebook once during initial setup.**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create schema (if needed)
# MAGIC CREATE SCHEMA IF NOT EXISTS investments.fred;
# MAGIC
# MAGIC -- Create volume for observation data
# MAGIC CREATE VOLUME IF NOT EXISTS investments.fred.observations;
# MAGIC
# MAGIC -- Create volume for series metadata/dimensional info
# MAGIC CREATE VOLUME IF NOT EXISTS investments.fred.metadata;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure API Key
# MAGIC 
# MAGIC **Option 1: Via Databricks CLI (recommended)**
# MAGIC ```bash
# MAGIC databricks secrets create-scope fred-api
# MAGIC databricks secrets put-secret fred-api api-key --string-value "YOUR_API_KEY"
# MAGIC ```
# MAGIC 
# MAGIC **Option 2: Via SDK (run cell below after updating YOUR_API_KEY)**

# COMMAND ----------

# Uncomment and update with your API key to configure via SDK
# from databricks.sdk import WorkspaceClient
# 
# w = WorkspaceClient()
# w.secrets.put_secret(
#     scope="fred-api",
#     key="api-key",
#     string_value="YOUR_API_KEY_HERE"  # Replace with your actual key
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Secret Configuration

# COMMAND ----------

# List scopes
print("Available scopes:")
print(dbutils.secrets.listScopes())

# List keys in scope (won't show values, just key names)
print("\nKeys in fred-api scope:")
print(dbutils.secrets.list("fred-api"))

# Test retrieving (value will be redacted in output but will work in code)
api_key = dbutils.secrets.get(scope="fred-api", key="api-key")
print(f"\n✓ API key retrieved: {len(api_key)} characters")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC 1. Run `02_Bronze_Setup.py` to create Bronze tables
# MAGIC 2. Run `03_Silver_Setup.py` to create Silver tables with constraints
# MAGIC 3. Run `04_Gold_Setup.py` to create Gold table
# MAGIC 4. Run `05_Daily_API_Call.py` to fetch data
