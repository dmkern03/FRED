# Delta Live Tables Pipeline Setup Guide

This guide explains how to set up and run the FRED data pipeline using Delta Live Tables (DLT).

## Overview

The FRED pipeline uses Delta Live Tables with:
- **Streaming Tables** for Bronze and Silver layers (Python API with `@dlt.table`)
- **Materialized View** for Gold layer (Python API with `@dlt.table`)
- **Auto Loader** for continuous file ingestion (`cloudFiles` format)
- **Automated data quality** with DLT expectations (`@dlt.expect_all_or_drop`)
- **PySpark transformations** for type conversions and data cleaning

## Prerequisites

1. Databricks workspace with Unity Catalog enabled
2. FRED API key configured in Databricks Secrets
3. Databricks CLI installed (for DAB deployment)

## Setup Methods

### Method 1: Using Databricks Asset Bundles (Recommended)

1. **Install Databricks CLI** (if not already installed):
   ```bash
   pip install databricks-cli
   ```

2. **Configure authentication**:
   ```bash
   databricks configure
   ```

3. **Deploy the pipeline**:
   ```bash
   cd s:\repos\FRED
   databricks bundle deploy --target dev
   ```

4. **Start the pipeline**:
   ```bash
   databricks bundle run fred_dlt_pipeline --target dev
   ```

### Method 2: Using Databricks UI

1. **Navigate to Delta Live Tables**:
   - Go to your Databricks workspace
   - Click on "Delta Live Tables" in the sidebar
   - Click "Create Pipeline"

2. **Configure the pipeline**:
   - **Pipeline name**: `FRED Data Pipeline - DLT`
   - **Product edition**: Advanced (for Streaming Tables and Materialized Views)
   - **Pipeline mode**: Triggered (or Continuous for real-time processing)

3. **Add notebook libraries**:
   - Click "Add notebook library"
   - Add these three notebooks in order:
     1. `/Workspace/Users/<your-email>/fred-data-pipeline/notebooks/02_Bronze_Setup.py`
     2. `/Workspace/Users/<your-email>/fred-data-pipeline/notebooks/03_Silver_Setup.py`
     3. `/Workspace/Users/<your-email>/fred-data-pipeline/notebooks/04_Gold_Setup.py`

4. **Configure destination**:
   - **Catalog**: `investments`
   - **Schema**: `fred`

5. **Configure cluster**:
   - **Cluster mode**: Enhanced Autoscaling
   - **Min workers**: 1
   - **Max workers**: 2
   - **Photon acceleration**: Enabled

6. **Advanced settings**:
   - **Channel**: Current (for latest features)
   - **Configuration**: Add key-value pairs if needed

7. **Create and start the pipeline**:
   - Click "Create"
   - Click "Start" to run the pipeline

## Pipeline Configuration File

The pipeline is defined in `resources/pipelines.yml`:

```yaml
resources:
  pipelines:
    fred_dlt_pipeline:
      name: "FRED Data Pipeline - DLT"
      target: "investments.fred"

      libraries:
        - notebook:
            path: ../notebooks/02_Bronze_Setup.py
        - notebook:
            path: ../notebooks/03_Silver_Setup.py
        - notebook:
            path: ../notebooks/04_Gold_Setup.py
```

## Running the Pipeline

### First-Time Setup

1. **Create volumes and configure secrets** (one-time):
   ```bash
   # Run notebook 01_API_Setup.py manually
   databricks workspace run /Workspace/.../01_API_Setup.py
   ```

2. **Fetch initial data** (before starting DLT pipeline):
   ```bash
   # Run notebook 05_Daily_API_Call.py to populate volumes
   databricks workspace run /Workspace/.../05_Daily_API_Call.py
   ```

3. **Start the DLT pipeline**:
   - Using UI: Click "Start" in the pipeline page
   - Using CLI: `databricks pipelines start --id <pipeline-id>`

### Daily Operations

1. **Schedule the API call** (uploads data to volumes):
   - Schedule `05_Daily_API_Call.py` as a daily job
   - Recommended time: 6 AM Eastern

2. **Pipeline auto-processes**:
   - Bronze Streaming Tables auto-ingest from volumes (Auto Loader)
   - Silver Streaming Tables auto-transform from Bronze
   - Gold Materialized View auto-refreshes from Silver

3. **Optional: Trigger manual refresh**:
   ```bash
   databricks pipelines start --id <pipeline-id>
   ```

## Pipeline Workflow

```
05_Daily_API_Call.py (Scheduled Job)
         │
         ▼
    Volumes (Landing Zone)
         │
         ▼ (Auto Loader - continuous)
Bronze Streaming Tables (DLT Pipeline)
         │
         ▼ (Stream processing)
Silver Streaming Tables (DLT Pipeline)
         │
         ▼ (Auto refresh)
Gold Materialized View (DLT Pipeline)
```

## Monitoring

### View Pipeline Status

1. **In Databricks UI**:
   - Go to "Delta Live Tables"
   - Click on your pipeline
   - View the graph showing all tables and their dependencies

2. **Check table status**:
   ```sql
   -- View bronze tables
   SELECT COUNT(*) FROM investments.fred.bronze_observations;
   SELECT COUNT(*) FROM investments.fred.bronze_metadata;

   -- View silver tables
   SELECT COUNT(*) FROM investments.fred.silver_observations;
   SELECT COUNT(*) FROM investments.fred.silver_metadata;

   -- View gold table
   SELECT COUNT(*) FROM investments.fred.gold_observations;
   ```

### View Pipeline Metrics

- Event log: Shows all pipeline runs and events
- Data quality metrics: Shows constraint violations
- Lineage: Visual representation of data flow

## Troubleshooting

### Pipeline Fails to Start

**Error**: "This Delta Live Tables query is syntactically valid, but you must create a pipeline..."

**Solution**: Make sure you're running the notebooks as part of a DLT pipeline, not as standalone notebooks.

### Constraints Not Applied

**Issue**: Primary/Foreign key constraints not showing up

**Solution**: Constraints in DLT Python API are enforced via expectations and documented in table properties. Check with:
```sql
DESCRIBE EXTENDED investments.fred.silver_metadata;
DESCRIBE EXTENDED investments.fred.silver_observations;
```

Look for:
- Table properties: `constraints` field documenting PK/FK relationships
- DLT expectations in the pipeline UI showing validation rules (e.g., `pk_series_id_not_null`)

### No Data in Tables

**Issue**: Tables exist but are empty

**Solution**:
1. Check that `05_Daily_API_Call.py` ran successfully and populated volumes
2. Verify files exist in volumes:
   ```sql
   LIST '/Volumes/investments/fred/observations/';
   LIST '/Volumes/investments/fred/metadata/';
   ```
3. Check DLT pipeline event log for errors

### Auto Loader Not Picking Up Files

**Issue**: New files in volumes not processed

**Solution**:
1. Check pipeline mode (should be Triggered or Continuous)
2. Verify schema location is accessible: `/Volumes/investments/fred/observations/_schema`
3. Restart the pipeline

## Best Practices

1. **Use Continuous mode** for real-time processing (if needed)
2. **Monitor costs** - Continuous mode runs 24/7
3. **Set up alerts** for pipeline failures
4. **Review data quality metrics** regularly
5. **Test in dev** before deploying to prod
6. **Use version control** for pipeline configuration changes

## Additional Resources

- [Databricks DLT Documentation](https://docs.databricks.com/delta-live-tables/index.html)
- [Streaming Tables Guide](https://docs.databricks.com/delta-live-tables/streaming-tables.html)
- [Materialized Views Guide](https://docs.databricks.com/sql/language-manual/sql-ref-syntax-ddl-create-materialized-view.html)
- [Auto Loader Documentation](https://docs.databricks.com/ingestion/auto-loader/index.html)
